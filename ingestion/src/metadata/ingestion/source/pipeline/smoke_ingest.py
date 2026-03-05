"""smoke_ingest.py — end-to-end ingestion smoke test against a live OM instance.

Uses the OM REST API directly (no Python SDK required).

Usage:
    python smoke_ingest.py [--host http://localhost:8585] [--dag aci_sf_encrypted_pipeline]
                           [--dumps-dir /path/to/ado-dumps]

Environment overrides:
    OM_HOST          default: http://localhost:8585
    OM_EMAIL         default: admin@open-metadata.org
    OM_PASSWORD_B64  default: YWRtaW4=  (base64 of "admin")
    AIRFLOW_DAG_NAME default: aci_sf_encrypted_pipeline
    ADO_DUMPS_DIR    default: examples/lineage-dumps/ado-dumps (relative to repo root)
"""

import argparse
import json
import logging
import os
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import urllib.request
import urllib.parse
import urllib.error

# Make generated schemas (installed to /tmp/omsite) importable before pulling them in.
_EXTRA_SITE = pathlib.Path("/tmp/omsite")
if _EXTRA_SITE.exists() and str(_EXTRA_SITE) not in sys.path:
    sys.path.insert(0, str(_EXTRA_SITE))

# Generated schema imports (installed via /tmp/omsite)
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.services.createDatabaseService import (
    CreateDatabaseServiceRequest,
)
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.entity.data.table import Column, ColumnName, DataType
from metadata.generated.schema.entity.services.connections.database.customDatabaseConnection import (
    CustomDatabaseConnection,
)
from metadata.generated.schema.entity.services.databaseService import (
    DatabaseConnection,
)
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
)
from metadata.generated.schema.type.entityLineage import ColumnLineage, LineageDetails

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("smoke_ingest")

# ---------------------------------------------------------------------------
# ADO dump loader (mirrors models.py without requiring the SDK)
# ---------------------------------------------------------------------------

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[6]  # ingestion/src/metadata/ingestion/source/pipeline/smoke_ingest.py → repo root
_DEFAULT_DUMPS_DIR = _REPO_ROOT / "ingestion" / "examples" / "lineage-dumps" / "ado-dumps"
_DEFAULT_DBT_MANIFEST = _REPO_ROOT / "airflowdbt" / "dbt_local" / "target" / "manifest.json"
_DEFAULT_DBT_RUN_RESULTS = _REPO_ROOT / "airflowdbt" / "dbt_local" / "target" / "run_results.json"
_DEFAULT_DAGS_DIR = _REPO_ROOT / "airflowdbt" / "airflow" / "dags" / "aci"

# Ensure repo root is importable for generated schema imports
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
_VENV_SITE = pathlib.Path("/tmp/omsite")
if _VENV_SITE.exists() and str(_VENV_SITE) not in sys.path:
    sys.path.insert(0, str(_VENV_SITE))

# Target service/database names
PIPELINE_SERVICE_NAME = "airflow_pipeline"
DB_SERVICE_NAME = "postgres_pipeline"
DB_NAME = "warehouse"
DB_SCHEMA_NAME = "public"


def _dump(model) -> Dict:
    """Pydantic v1/v2 compatibility helper."""
    if hasattr(model, "model_dump"):
        return model.model_dump(by_alias=True, exclude_none=True)
    return model.dict(by_alias=True, exclude_none=True)


def _json_dumps(obj: Dict) -> str:
    return json.dumps(obj, default=lambda o: getattr(o, "value", str(o)))
_INGESTION_SRC = _REPO_ROOT / "ingestion" / "src"
if str(_INGESTION_SRC) not in sys.path:
    sys.path.insert(0, str(_INGESTION_SRC))


def _load_runs(dumps_dir: pathlib.Path) -> List[Dict]:
    runs_path = dumps_dir / "runs.json"
    if not runs_path.exists():
        log.warning("runs.json not found at %s — using empty list", runs_path)
        return []
    return json.loads(runs_path.read_text(encoding="utf-8"))


def _load_approvals(dumps_dir: pathlib.Path) -> Dict[int, Dict]:
    approvals_path = dumps_dir / "approvals.json"
    if not approvals_path.exists():
        return {}
    data = json.loads(approvals_path.read_text(encoding="utf-8"))
    return {a["runId"]: a for a in data}


def _build_run_description(run: Dict, approvals: Dict[int, Dict]) -> str:
    run_id = run["id"]
    run_type = run.get("type", "unknown")
    git_sha = run.get("git_sha", "unknown")
    pipeline_type = run.get("pipeline_type", "unknown")
    result = run.get("result", "unknown")

    parts = [
        f"ADO {run_type} run {run_id}",
        f"result={result}",
        f"pipeline_type={pipeline_type}",
        f"git_sha={git_sha}",
    ]

    if run_type == "build":
        parts.append(f"image={run.get('image', 'unknown')}")
    else:
        parts.append(f"version={run.get('version', 'unknown')}")
        parts.append(f"env={run.get('env', 'unknown')}")
        approval = approvals.get(run_id)
        if approval:
            parts.append(f"approved_by={approval['approver']}")
            parts.append(f"approved_at={approval['approvedAt']}")

    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Minimal HTTP helper (no third-party deps)
# ---------------------------------------------------------------------------

class OmClient:
    def __init__(self, host: str, token: str):
        self.host = host.rstrip("/")
        self.token = token

    def _request(self, method: str, path: str, body: Optional[Dict] = None) -> Dict:
        url = f"{self.host}{path}"
        data = _json_dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req) as resp:
                raw = resp.read()
                return json.loads(raw) if raw.strip() else {}
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode()
            log.error("HTTP %s %s -> %d: %s", method, path, exc.code, body_text[:300])
            raise

    def get(self, path: str) -> Dict:
        return self._request("GET", path)

    def post(self, path: str, body: Dict) -> Dict:
        return self._request("POST", path, body)

    def put(self, path: str, body: Dict) -> Dict:
        return self._request("PUT", path, body)


def login(host: str, email: str, password_b64: str) -> str:
    url = f"{host.rstrip('/')}/api/v1/users/login"
    payload = json.dumps({"email": email, "password": password_b64}).encode()
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())["accessToken"]


# ---------------------------------------------------------------------------
# Entity helpers
# ---------------------------------------------------------------------------

def upsert_pipeline_service(client: OmClient, name: str) -> Dict:
    payload = {
        "name": name,
        "serviceType": "CustomPipeline",
        "connection": {"config": {"type": "CustomPipeline"}},
    }
    result = client.put("/api/v1/services/pipelineServices", payload)
    log.info("pipeline service  %-20s  fqn=%s", name, result.get("fullyQualifiedName"))
    return result


def upsert_storage_service(client: OmClient, name: str) -> Dict:
    payload = {
        "name": name,
        "serviceType": "CustomStorage",
        "connection": {"config": {"type": "CustomStorage"}},
    }
    result = client.put("/api/v1/services/storageServices", payload)
    log.info("storage service   %-20s  fqn=%s", name, result.get("fullyQualifiedName"))
    return result


def upsert_pipeline(client: OmClient, name: str, service: str,
                    description: str = "", tags: Optional[list] = None) -> Dict:
    payload: Dict[str, Any] = {"name": name, "service": service, "description": description}
    if tags:
        payload["tags"] = [{"tagFQN": t} for t in tags]
    result = client.put("/api/v1/pipelines", payload)
    log.info("pipeline          %-30s  fqn=%s", name, result.get("fullyQualifiedName"))
    return result


def upsert_container(client: OmClient, name: str, service: str, description: str = "") -> Dict:
    payload = {"name": name, "service": service, "description": description}
    result = client.put("/api/v1/containers", payload)
    log.info("container         %-30s  fqn=%s", name, result.get("fullyQualifiedName"))
    return result


# Database hierarchy helpers (CustomDatabase to avoid real creds)
def upsert_database_service(client: OmClient, name: str) -> Dict:
    request = CreateDatabaseServiceRequest(
        name=name,
        serviceType=DatabaseServiceType.CustomDatabase,
        description="Custom DB service for dbt models",
        connection=DatabaseConnection(
            config=CustomDatabaseConnection(type="CustomDatabase")
        ),
    )
    payload = _dump(request)
    result = client.put("/api/v1/services/databaseServices", payload)
    log.info("database service  %-20s  fqn=%s", name, result.get("fullyQualifiedName"))
    return result


def upsert_database(client: OmClient, name: str, service_name: str) -> Dict:
    request = CreateDatabaseRequest(
        name=name,
        service=service_name,
    )
    payload = _dump(request)
    result = client.put("/api/v1/databases", payload)
    log.info("database          %-20s  fqn=%s", name, result.get("fullyQualifiedName"))
    return result


def upsert_database_schema(client: OmClient, name: str, database_fqn: str) -> Dict:
    request = CreateDatabaseSchemaRequest(
        name=name,
        database=database_fqn,
    )
    payload = _dump(request)
    result = client.put("/api/v1/databaseSchemas", payload)
    log.info("db schema         %-20s  fqn=%s", name, result.get("fullyQualifiedName"))
    return result


def upsert_table(
    client: OmClient,
    table_name: str,
    database_schema_fqn: str,
    columns: List[Column],
    table_type: str = "Regular",
) -> Dict:
    request = CreateTableRequest(
        name=table_name,
        tableType=table_type,
        columns=columns,
        databaseSchema=database_schema_fqn,
    )
    payload = _dump(request)
    result = client.put("/api/v1/tables", payload)
    log.info(
        "table             %-30s  fqn=%s", table_name, result.get("fullyQualifiedName")
    )
    return result


def push_table_data(client: OmClient, table_id: str, columns: List[str], rows: List[List]) -> None:
    payload = {"columns": columns, "rows": rows}
    client.put(f"/api/v1/tables/{table_id}/sampleData", payload)
    log.info("sample data       %d rows → table id %s", len(rows), table_id)


def get_entity_id(client: OmClient, entity_type: str, fqn: str) -> Optional[str]:
    """Return entity id for fqn, or None if not found."""
    encoded = urllib.parse.quote(fqn, safe="")
    path_map = {
        "pipeline": f"/api/v1/pipelines/name/{encoded}?fields=id",
        "container": f"/api/v1/containers/name/{encoded}?fields=id",
        "table": f"/api/v1/tables/name/{encoded}?fields=id",
        "databaseSchema": f"/api/v1/databaseSchemas/name/{encoded}?fields=id",
    }
    path = path_map.get(entity_type)
    if path is None:
        log.warning("Unknown entity_type: %s", entity_type)
        return None
    try:
        result = client.get(path)
        return result["id"]
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            log.warning("Entity not found: %s %s", entity_type, fqn)
        return None


def post_lineage(client: OmClient,
                 from_type: str, from_fqn: str,
                 to_type: str, to_fqn: str,
                 description: str,
                 details: Optional[LineageDetails] = None) -> bool:
    from_id = get_entity_id(client, from_type, from_fqn)
    to_id = get_entity_id(client, to_type, to_fqn)
    if not from_id or not to_id:
        log.warning("Skipping edge %s -> %s (entity not found)", from_fqn, to_fqn)
        return False

    payload = {
        "edge": {
            "fromEntity": {"id": from_id, "type": from_type},
            "toEntity": {"id": to_id, "type": to_type},
            "lineageDetails": _dump(details or LineageDetails(description=description)),
        }
    }
    try:
        client.put("/api/v1/lineage", payload)
        log.info("lineage           %s  -->  %s  [%s]", from_fqn, to_fqn, description)
        return True
    except urllib.error.HTTPError as exc:
        log.error("Failed lineage %s -> %s: %s", from_fqn, to_fqn, exc)
        return False


def _load_dbt_manifest(manifest_path: pathlib.Path) -> Dict:
    if not manifest_path.exists():
        raise FileNotFoundError(f"dbt manifest not found: {manifest_path}")
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed manifest.json: {exc}") from exc


def _parse_dbt_models(manifest: Dict) -> List[Dict]:
    return [
        node
        for node in manifest.get("nodes", {}).values()
        if node.get("resource_type") == "model"
    ]


def _model_to_table_name(model: Dict) -> str:
    return model.get("name") or model["unique_id"].split(".")[-1]


def _build_columns(model: Dict) -> List[Column]:
    cols = []
    for col in model.get("columns", {}).values():
        col_name = col.get("name")
        if not col_name:
            continue
        cols.append(
            Column(
                name=col_name,
                dataType=DataType.STRING,
                description=col.get("description"),
            )
        )
    if not cols:
        cols.append(Column(name="value", dataType=DataType.STRING))
    return cols


def _build_model_lineage_edges(models: List[Dict]) -> List[Tuple[str, str]]:
    lookup = {m["unique_id"]: m for m in models}
    edges: List[Tuple[str, str]] = []
    for model in models:
        downstream = _model_to_table_name(model)
        for upstream_uid in model.get("depends_on", {}).get("nodes", []):
            if upstream_uid in lookup:
                upstream = _model_to_table_name(lookup[upstream_uid])
                edges.append((upstream, downstream))
    return edges


def _discover_airflow_dags(dags_dir: pathlib.Path) -> List[str]:
    dag_ids: List[str] = []
    for pyfile in sorted(dags_dir.glob("aci_*.py")):
        text = pyfile.read_text(encoding="utf-8", errors="ignore")
        if "dag_id" not in text:
            continue
        for marker in ["dag_id=", "dag_id ="]:
            if marker in text:
                part = text.split(marker, 1)[1]
                quote = "'" if "'" in part else '"'
                try:
                    dag_id = part.split(quote, 2)[1]
                    dag_ids.append(dag_id)
                    break
                except IndexError:
                    continue
    return dag_ids


def _group_models_by_tag(models: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    superfund = []
    base = []
    for m in models:
        tags = [t.lower() for t in m.get("tags", [])]
        (superfund if "superfund" in tags else base).append(m)
    return base, superfund


def seed_audit_log(
    dumps_dir: pathlib.Path,
    output_path: pathlib.Path,
    dag_fqns: List[str],
    run_end: Optional[datetime] = None,
) -> None:
    """Build a real audit_log.jsonl from ADO runs + approvals dumps.

    Reads runs.json and approvals.json, constructs real AuditEntity objects
    (including approved_by and env from the infra deploy + approval record),
    then runs DagEnricher against the provided DAG FQNs so every row has
    accurate approved_by/env instead of nulls.
    """
    import importlib.util
    from unittest.mock import MagicMock

    _dag_enricher_path = pathlib.Path(__file__).with_name("dag_enricher.py")
    _spec = importlib.util.spec_from_file_location("dag_enricher", _dag_enricher_path)
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    AuditEntity = _mod.AuditEntity
    DagEnricher = _mod.DagEnricher
    DagRecord = _mod.DagRecord

    runs = _load_runs(dumps_dir)
    approvals = _load_approvals(dumps_dir)
    if not runs:
        log.warning("No runs found in %s — cannot seed audit log", dumps_dir)
        return

    if run_end is None:
        run_end = datetime.now(tz=timezone.utc)

    builds: List[AuditEntity] = []
    infras: List[AuditEntity] = []
    for r in runs:
        start_raw = r.get("startTime") or r.get("finishTime") or ""
        if not start_raw:
            continue
        date = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
        fqn = f"ADO.{r.get('type','run')}-{r['id']}"
        if r.get("pipeline_type") == "image_build":
            builds.append(AuditEntity(fqn=fqn, date=date, git_sha=r.get("git_sha", "")))
        elif r.get("pipeline_type") == "infra_deploy":
            approval = approvals.get(r["id"], {})
            infras.append(AuditEntity(
                fqn=fqn,
                date=date,
                git_sha=r.get("git_sha", ""),
                approved_by=approval.get("approver"),
                env=r.get("env"),
            ))

    if not builds:
        log.warning("No image_build runs found — cannot seed audit log")
        return

    # Load per-DAG real end_dates from Airflow dump if available
    airflow_runs_path = dumps_dir / "airflow_dag_runs.json"
    airflow_end_dates: Dict[str, datetime] = {}
    if airflow_runs_path.exists():
        for item in json.loads(airflow_runs_path.read_text(encoding="utf-8")):
            dag_id = item.get("dag_id", "")
            end_raw = item.get("end_date", "")
            if dag_id and end_raw:
                try:
                    airflow_end_dates[dag_id] = datetime.fromisoformat(
                        end_raw.replace("Z", "+00:00")
                    )
                except ValueError:
                    pass
        log.info("Loaded %d Airflow DAG run timestamps", len(airflow_end_dates))

    default_run_end = run_end or datetime.now(tz=timezone.utc)
    dag_records = [
        DagRecord(
            fqn=fqn,
            run_end=airflow_end_dates.get(fqn.split(".")[-1], default_run_end),
        )
        for fqn in dag_fqns
    ]

    # Remove old file so we get a clean set (no carry-over from test runs)
    if output_path.exists():
        output_path.unlink()
        log.info("Removed existing %s", output_path)

    mock_om = MagicMock()
    DagEnricher(mock_om, audit_log_path=output_path).enrich(builds, infras, dag_records)

    rows_written = sum(1 for l in output_path.read_text().splitlines() if l.strip())
    log.info("Seeded %d audit rows → %s", rows_written, output_path)
    # Show a preview of what was written
    for line in output_path.read_text().splitlines():
        if line.strip():
            row = json.loads(line)
            log.info(
                "  %s | %s | approved_by=%s | env=%s",
                row["audit_date"], row["dag_fqn"][:50],
                row.get("approved_by") or "(none)",
                row.get("env") or "(none)",
            )


AUDIT_COLUMNS = [
    "audit_date",
    "dag_fqn",
    "run_end_timestamp",
    "image_build_ref",
    "git_sha",
    "infra_deploy_ref",
    "approved_by",
    "env",
]

AUDIT_SERVICE_NAME = "audit_db"
AUDIT_DB_NAME = "audit_db"
AUDIT_SCHEMA_NAME = "public"
AUDIT_TABLE_NAME = "daily_deployment_log"
AUDIT_TABLE_FQN = f"{AUDIT_SERVICE_NAME}.{AUDIT_DB_NAME}.{AUDIT_SCHEMA_NAME}.{AUDIT_TABLE_NAME}"


def ingest_audit_log(
    client: OmClient,
    audit_log_path: pathlib.Path,
    gold_table_fqns: Optional[List[str]] = None,
) -> None:
    if not audit_log_path.exists():
        log.warning("audit_log.jsonl not found at %s — skipping", audit_log_path)
        return

    rows_raw = [
        json.loads(line)
        for line in audit_log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not rows_raw:
        log.info("audit_log.jsonl is empty — nothing to push")
        return
    log.info("Loaded %d audit rows from %s", len(rows_raw), audit_log_path)

    log.info("=" * 60)
    log.info("Audit Log — DB hierarchy")
    log.info("=" * 60)
    upsert_database_service(client, AUDIT_SERVICE_NAME)
    upsert_database(client, AUDIT_DB_NAME, AUDIT_SERVICE_NAME)
    db_fqn = f"{AUDIT_SERVICE_NAME}.{AUDIT_DB_NAME}"
    schema_fqn = f"{db_fqn}.{AUDIT_SCHEMA_NAME}"
    upsert_database_schema(client, AUDIT_SCHEMA_NAME, db_fqn)

    cols = [Column(name=c, dataType=DataType.STRING) for c in AUDIT_COLUMNS]
    table_resp = upsert_table(client, AUDIT_TABLE_NAME, schema_fqn, cols)
    table_id = table_resp["id"]

    log.info("=" * 60)
    log.info("Audit Log — Sample data")
    log.info("=" * 60)
    rows_list = [
        [str(row.get(col) or "") for col in AUDIT_COLUMNS]
        for row in rows_raw
    ]
    push_table_data(client, table_id, AUDIT_COLUMNS, rows_list)

    log.info("=" * 60)
    log.info("Audit Log — Lineage to gold tables")
    log.info("=" * 60)
    for gold_fqn in (gold_table_fqns or []):
        post_lineage(client, "table", AUDIT_TABLE_FQN, "table", gold_fqn, "audit → gold")


# ---------------------------------------------------------------------------
# Main ingestion flow
# ---------------------------------------------------------------------------

def run(
    host: str,
    email: str,
    password_b64: str,
    dag_name: str,
    dumps_dir: pathlib.Path,
    dbt_manifest: pathlib.Path,
    dbt_run_results: Optional[pathlib.Path],
    dags_dir: pathlib.Path,
    dry_run: bool = False,
    audit_log_path: Optional[pathlib.Path] = None,
    gold_table_fqns: Optional[List[str]] = None,
    seed_audit: bool = False,
):
    log.info("Logging in to %s as %s", host, email)
    token = login(host, email, password_b64)
    client = OmClient(host=host, token=token)

    if dry_run:
        log.info("DRY RUN ENABLED — no writes will be sent to OM")

        def _noop(method: str, path: str, body: Optional[Dict] = None) -> Dict:
            log.info("[DRY RUN] %s %s %s", method, path, body if body else "")
            return {"id": "dry-run"}

        client.put = lambda path, body=None: _noop("PUT", path, body)
        client.post = lambda path, body=None: _noop("POST", path, body)

    runs = _load_runs(dumps_dir)
    approvals = _load_approvals(dumps_dir)
    log.info("Loaded %d ADO runs, %d approvals from %s", len(runs), len(approvals), dumps_dir)

    log.info("dbt manifest: %s", dbt_manifest)
    if dbt_run_results:
        log.info("dbt run_results: %s", dbt_run_results)
    log.info("Airflow DAGs dir: %s", dags_dir)

    log.info("=" * 60)
    log.info("Step 1 — Create pipeline services")
    log.info("=" * 60)
    upsert_pipeline_service(client, "ADO")
    upsert_pipeline_service(client, "ACI-prd")
    upsert_pipeline_service(client, "airflow_pipeline")

    log.info("=" * 60)
    log.info("Step 2 — Create storage service (ACR)")
    log.info("=" * 60)
    upsert_storage_service(client, "ACR")

    log.info("=" * 60)
    log.info("Step 3 — Create pipeline entities from ADO runs dump")
    log.info("=" * 60)
    build_run: Optional[Dict] = None
    deploy_run: Optional[Dict] = None
    for r in runs:
        desc = _build_run_description(r, approvals)
        name = f"{r.get('type', 'run')}-{r['id']}"
        upsert_pipeline(client, name, "ADO", description=desc, tags=["Tier.Tier2"])
        if r.get("type") == "build" and build_run is None:
            build_run = r
        elif r.get("type") == "deploy" and deploy_run is None:
            deploy_run = r

    if not runs:
        log.warning("No runs loaded — falling back to hardcoded entities")
        upsert_pipeline(client, "build-232", "ADO",
                        description="ADO build run 232 | result=succeeded | pipeline_type=image_build | git_sha=abc123def456 | image=airflow:231",
                        tags=["Tier.Tier2"])
        upsert_pipeline(client, "infra-deploy-233", "ADO",
                        description="ADO deploy run 233 | result=succeeded | pipeline_type=infra_deploy | git_sha=abc123def456 | version=231 | env=prd",
                        tags=["Tier.Tier2"])
        build_fqn = "ADO.build-232"
        deploy_fqn = "ADO.infra-deploy-233"
    else:
        build_fqn = f"ADO.build-{build_run['id']}" if build_run else None
        deploy_fqn = f"ADO.{deploy_run.get('type', 'deploy')}-{deploy_run['id']}" if deploy_run else None

    log.info("=" * 60)
    log.info("Step 4 — Create ACI pipeline entity")
    log.info("=" * 60)
    deploy_image = "unknown"
    if deploy_run:
        deploy_image = deploy_run.get("version", "unknown")
    elif build_run:
        deploy_image = build_run.get("image", "unknown")
    upsert_pipeline(client, "aci-pipeline-prd", "ACI-prd",
                    description=f"ACI container group aci-pipeline-prd (image {deploy_image})",
                    tags=["Tier.Tier1"])

    log.info("=" * 60)
    log.info("Step 5 — Create ACR image container (ACR.airflow)")
    log.info("=" * 60)
    upsert_container(client, "airflow", "ACR",
                     description="ACR image repository — acrpipelinedevhbwx.azurecr.io/pipeline/airflow")

    log.info("=" * 60)
    log.info("Step 6 — Create Airflow DAG pipelines")
    log.info("=" * 60)
    dag_ids = _discover_airflow_dags(dags_dir)
    if dag_name not in dag_ids:
        dag_ids.append(dag_name)
    for dag_id in sorted(set(dag_ids)):
        upsert_pipeline(
            client,
            dag_id,
            PIPELINE_SERVICE_NAME,
            description=f"Airflow DAG {dag_id} — orchestrated by ACI",
            tags=["Tier.Tier1"],
        )

    log.info("=" * 60)
    log.info("Step 7 — Stitch lineage edges")
    log.info("=" * 60)
    edges: List[Tuple[str, str, str, str, str, Optional[LineageDetails]]] = []
    if build_fqn:
        edges.append(("pipeline", build_fqn, "container", "ACR.airflow", "builds", None))
    if deploy_fqn:
        edges.append(("pipeline", deploy_fqn, "pipeline", "ACI-prd.aci-pipeline-prd", "deploys", None))
    for dag_id in sorted(set(dag_ids)):
        edges.append(
            (
                "pipeline",
                "ACI-prd.aci-pipeline-prd",
                "pipeline",
                f"{PIPELINE_SERVICE_NAME}.{dag_id}",
                "runs",
                None,
            )
        )

    log.info("=" * 60)
    log.info("Step 8 — dbt models (tables) and lineage")
    log.info("=" * 60)
    manifest_data = _load_dbt_manifest(dbt_manifest)
    models = _parse_dbt_models(manifest_data)
    base_models, superfund_models = _group_models_by_tag(models)

    # Ensure DB hierarchy
    upsert_database_service(client, DB_SERVICE_NAME)
    upsert_database(client, DB_NAME, DB_SERVICE_NAME)
    db_fqn = f"{DB_SERVICE_NAME}.{DB_NAME}"
    schema_fqn = f"{db_fqn}.{DB_SCHEMA_NAME}"
    upsert_database_schema(client, DB_SCHEMA_NAME, db_fqn)

    # Create tables
    model_table_fqn: Dict[str, str] = {}
    for model in models:
        table_name = _model_to_table_name(model)
        table_fqn = f"{schema_fqn}.{table_name}"
        model_table_fqn[model["unique_id"]] = table_fqn
        cols = _build_columns(model)
        upsert_table(client, table_name, schema_fqn, cols)

    # Pipeline -> dbt model lineage (table-level)
    for dag_id in sorted(set(dag_ids)):
        target_models = superfund_models if "sf" in dag_id else base_models
        for model in target_models:
            table_name = _model_to_table_name(model)
            edges.append(
                (
                    "pipeline",
                    f"{PIPELINE_SERVICE_NAME}.{dag_id}",
                    "table",
                    f"{schema_fqn}.{table_name}",
                    "runs",
                    None,
                )
            )

    # dbt model -> model lineage
    for upstream_name, downstream_name in _build_model_lineage_edges(models):
        edges.append(
            (
                "table",
                f"{schema_fqn}.{upstream_name}",
                "table",
                f"{schema_fqn}.{downstream_name}",
                "dbt dependency",
                None,
            )
        )

    if dry_run:
        log.info("[DRY RUN] Would post %d lineage edges", len(edges))
        return 0

    posted = sum(post_lineage(client, *e) for e in edges)
    log.info("Posted %d/%d lineage edges", posted, len(edges))

    log.info("=" * 60)
    log.info("Done. Open http://localhost:8585 and navigate to Lineage on any entity.")
    log.info("=" * 60)

    if audit_log_path:
        log.info("=" * 60)
        log.info("Step 9 — Audit log table + sample data + lineage")
        log.info("=" * 60)
        if seed_audit:
            log.info("Seeding audit_log.jsonl from ADO dumps…")
            seed_audit_log(
                dumps_dir=dumps_dir,
                output_path=audit_log_path,
                dag_fqns=sorted(set(dag_ids)),
            )
        ingest_audit_log(client, audit_log_path, gold_table_fqns)

    return 0 if posted == len(edges) else 1


# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="3D lineage smoke ingestion")
    parser.add_argument("--host", default=os.environ.get("OM_HOST", "http://localhost:8585"))
    parser.add_argument("--email", default=os.environ.get("OM_EMAIL", "admin@open-metadata.org"))
    parser.add_argument("--password-b64", default=os.environ.get("OM_PASSWORD_B64", "YWRtaW4="),
                        help="Base64-encoded password (default: base64 of 'admin')")
    parser.add_argument("--dag", default=os.environ.get("AIRFLOW_DAG_NAME", "aci_sf_encrypted_pipeline"))
    parser.add_argument("--dumps-dir", default=os.environ.get("ADO_DUMPS_DIR", str(_DEFAULT_DUMPS_DIR)),
                        help="Path to directory containing runs.json and approvals.json")
    parser.add_argument("--dbt-manifest", default=str(_DEFAULT_DBT_MANIFEST),
                        help="Path to dbt manifest.json")
    parser.add_argument("--dbt-run-results", default=str(_DEFAULT_DBT_RUN_RESULTS),
                        help="Optional path to dbt run_results.json")
    parser.add_argument("--dags-dir", default=str(_DEFAULT_DAGS_DIR), help="Path to Airflow DAGs directory")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without sending REST writes")
    parser.add_argument("--audit-log-path", default=None,
                        help="Path to audit_log.jsonl to push as OM table entity + sample data")
    parser.add_argument("--gold-table-fqns", default="postgres_pipeline.warehouse.public.dim_sf_members_scd2",
                        help="Comma-separated gold table FQNs for audit lineage edges")
    parser.add_argument("--seed-audit", action="store_true",
                        help="Rebuild audit_log.jsonl from ADO dumps before pushing to OM")
    args = parser.parse_args()

    run_results_path = pathlib.Path(args.dbt_run_results) if args.dbt_run_results else None
    audit_path = pathlib.Path(args.audit_log_path) if args.audit_log_path else None
    gold_fqns = [f.strip() for f in args.gold_table_fqns.split(",") if f.strip()] if args.gold_table_fqns else []

    sys.exit(
        run(
            args.host,
            args.email,
            args.password_b64,
            args.dag,
            pathlib.Path(args.dumps_dir),
            pathlib.Path(args.dbt_manifest),
            run_results_path,
            pathlib.Path(args.dags_dir),
            args.dry_run,
            audit_log_path=audit_path,
            gold_table_fqns=gold_fqns,
            seed_audit=args.seed_audit,
        )
    )


if __name__ == "__main__":
    main()
