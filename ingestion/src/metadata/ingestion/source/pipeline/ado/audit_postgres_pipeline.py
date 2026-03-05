"""Lightweight audit_postgres_pipeline DAG representation (Airflow-optional)."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from metadata.ingestion.source.pipeline.ado.airflow_postgres_ingestion import (
    AirflowPostgresIngestor,
)
from metadata.ingestion.source.pipeline.ado.postgres_ingestion import (
    AdoPostgresIngestor,
)

RETRY_DELAY = timedelta(minutes=5)
RETRIES = 2


def _is_dry_run() -> bool:
    return os.getenv("DRY_RUN", "false").lower() == "true"


def _yesterday(today: Optional[datetime] = None) -> datetime:
    today = today or datetime.now(timezone.utc)
    return today - timedelta(days=1)


class SimpleTask:
    def __init__(self, task_id: str, python_callable: Callable, retries: int, retry_delay: timedelta):
        self.task_id = task_id
        self.python_callable = python_callable
        self.retries = retries
        self.retry_delay = retry_delay
        self.downstream_task_ids: List[str] = []

    def set_downstream(self, task: "SimpleTask") -> None:
        if task.task_id not in self.downstream_task_ids:
            self.downstream_task_ids.append(task.task_id)

    def __rshift__(self, other: "SimpleTask") -> "SimpleTask":
        self.set_downstream(other)
        return other


class SimpleDag:
    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.tasks: List[SimpleTask] = []

    def add_task(self, task: SimpleTask) -> None:
        self.tasks.append(task)


def run_ado_ingestion(
    *,
    api_client: Any = None,
    repo: Any = None,
    project: Optional[str] = None,
) -> None:
    api_client = api_client or _noop_client()
    repo = repo or _noop_repo()
    project = project or os.getenv("ADO_PROJECT", "default_project")
    ingestor = AdoPostgresIngestor(
        api_client=api_client,
        repo=repo,
        project=project,
        dry_run=_is_dry_run(),
    )
    ingestor.ingest()


def run_airflow_ingestion(
    *,
    api_client: Any = None,
    repo: Any = None,
    airflow_instance: Optional[str] = None,
) -> None:
    api_client = api_client or _noop_client()
    repo = repo or _noop_repo()
    airflow_instance = airflow_instance or os.getenv("AIRFLOW_INSTANCE", "localhost")
    ingestor = AirflowPostgresIngestor(
        api_client=api_client,
        repo=repo,
        airflow_instance=airflow_instance,
        dry_run=_is_dry_run(),
    )
    ingestor.ingest()


def refresh_materialized_view(**_: Any) -> None:  # pragma: no cover - placeholder
    # In production this would execute `REFRESH CONCURRENTLY audit.daily_audit_summary`.
    return None


def _row_date_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    try:
        # date, str, or objects with isoformat()
        return value.isoformat() if hasattr(value, "isoformat") else str(value)
    except Exception:  # pragma: no cover - defensive
        return str(value)


def _fmt_rows(rows: Sequence[Dict[str, Any]], fmt: Callable[[Dict[str, Any]], str]) -> List[str]:
    if not rows:
        return ["- None"]
    return [fmt(row) for row in rows]


def generate_daily_report(
    output_dir: Optional[Path] = None,
    *,
    rows: Optional[List[Dict[str, Any]]] = None,
    report_date: Optional[str] = None,
    today: Optional[datetime] = None,
    fetcher: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
) -> Optional[Path]:
    """Render Markdown report from daily_audit_summary rows.

    Parameters are test-friendly: pass rows directly or a fetcher callable.
    """

    if _is_dry_run():
        return None

    output_dir = output_dir or Path("reports")
    output_dir.mkdir(parents=True, exist_ok=True)

    if report_date:
        date_str = report_date
    else:
        date_str = _yesterday(today).strftime("%Y-%m-%d")

    target_date = date_str

    if rows is None:
        rows = fetcher(target_date) if fetcher else []

    filtered_rows = [row for row in rows if _row_date_str(row.get("audit_date")) == target_date]

    total_runs = sum(row.get("total_runs", 0) for row in filtered_rows)
    success_count = sum(row.get("success_count", 0) for row in filtered_rows)
    failure_count = sum(row.get("failure_count", 0) for row in filtered_rows)
    approval_count = sum(row.get("approval_count", 0) for row in filtered_rows)

    success_rate = round((success_count * 100.0 / total_runs), 2) if total_runs else 0
    approval_pct = round((approval_count * 100.0 / total_runs), 2) if total_runs else 0

    top_failures = sorted(filtered_rows, key=lambda r: r.get("failure_count", 0), reverse=True)[:5]
    longest_runs = sorted(filtered_rows, key=lambda r: r.get("max_duration_seconds") or 0, reverse=True)[:5]
    missing_approvals = [
        row
        for row in filtered_rows
        if row.get("env", "").lower() == "prd" and row.get("approval_pct", 0) < 100
    ]

    def label(row: Dict[str, Any]) -> str:
        env = row.get("env") or ""
        job = row.get("job_name") or row.get("dag_id") or "unknown"
        src = row.get("source_system") or ""
        return f"{src} {env} {job}".strip()

    lines: List[str] = []
    lines.append(f"# Audit Report for {target_date}")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append(f"Total runs: {total_runs}")
    lines.append(f"Success rate: {success_rate}%")
    lines.append(f"Approvals: {approval_count}/{total_runs} ({approval_pct}%)")
    lines.append("")

    lines.append("## Top Failures")
    lines.extend(_fmt_rows(top_failures, lambda r: f"- {label(r)} — failures: {r.get('failure_count', 0)}"))
    lines.append("")

    lines.append("## Longest Runs")
    lines.extend(
        _fmt_rows(longest_runs, lambda r: f"- {label(r)} — max duration: {r.get('max_duration_seconds', 0)}s")
    )
    lines.append("")

    lines.append("## Missing Approvals")
    lines.extend(
        _fmt_rows(
            missing_approvals,
            lambda r: f"- {label(r)} — approval_pct: {r.get('approval_pct', 0)}%",
        )
    )
    lines.append("")

    lines.append("## Counts by Pipeline/DAG")
    lines.extend(
        _fmt_rows(
            filtered_rows,
            lambda r: f"- {label(r)} — total: {r.get('total_runs', 0)}, success: {r.get('success_count', 0)}, failure: {r.get('failure_count', 0)}",
        )
    )

    report_path = output_dir / f"audit-{target_date}.md"
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


_om_created = False


def _upsert_service(om_client: Any) -> None:
    for method in (
        "create_or_update_service",
        "upsert_service",
        "ensure_service",
    ):
        if hasattr(om_client, method):
            getattr(om_client, method)("audit_db", "postgres_pipeline")
            return


def _upsert_table(om_client: Any, name: str, schema: str) -> None:
    for method in (
        "create_or_update_table",
        "upsert_table",
        "ensure_table",
    ):
        if hasattr(om_client, method):
            getattr(om_client, method)(name, schema)
            return


def _upsert_view(om_client: Any, name: str, schema: str, *, owner: str, tier: str) -> None:
    for method in (
        "create_or_update_view",
        "upsert_view",
        "ensure_view",
    ):
        if hasattr(om_client, method):
            getattr(om_client, method)(name, schema, owner=owner, tier=tier)
            return


def setup_om_entities(om_client: Optional[Any] = None) -> None:
    global _om_created
    if _om_created:
        return

    if om_client:
        if hasattr(om_client, "ensure_entities"):
            om_client.ensure_entities()
        else:
            _upsert_service(om_client)
            schema_fqn = "audit_db.audit"
            _upsert_table(om_client, "ado_pipeline_runs", schema_fqn)
            _upsert_table(om_client, "airflow_dag_executions", schema_fqn)
            _upsert_view(om_client, "daily_audit_summary", schema_fqn, owner="DataPlatform", tier="Gold")
    # In production this would call OpenMetadata to register audit_db tables/view.
    _om_created = True


def _noop_client():
    class _Client:
        def fetch_runs(self, since):
            return []

    return _Client()


def _noop_repo():
    class _Repo:
        def max_queue_time(self):
            return None

        def max_execution_date(self):
            return None

        def upsert(self, row):
            return row

    return _Repo()


# Build DAG structure (Airflow-agnostic)
dag = SimpleDag(dag_id="audit_postgres_pipeline")

setup_task = SimpleTask(
    task_id="setup_om_entities",
    python_callable=setup_om_entities,
    retries=RETRIES,
    retry_delay=RETRY_DELAY,
)
ado_task = SimpleTask(
    task_id="ado_ingest",
    python_callable=run_ado_ingestion,
    retries=RETRIES,
    retry_delay=RETRY_DELAY,
)
airflow_task = SimpleTask(
    task_id="airflow_ingest",
    python_callable=run_airflow_ingestion,
    retries=RETRIES,
    retry_delay=RETRY_DELAY,
)
refresh_task = SimpleTask(
    task_id="refresh_mv",
    python_callable=refresh_materialized_view,
    retries=RETRIES,
    retry_delay=RETRY_DELAY,
)
report_task = SimpleTask(
    task_id="daily_report",
    python_callable=generate_daily_report,
    retries=RETRIES,
    retry_delay=RETRY_DELAY,
)

# Chain tasks in required order
setup_task >> ado_task >> airflow_task >> refresh_task >> report_task

for task in (setup_task, ado_task, airflow_task, refresh_task, report_task):
    dag.add_task(task)
