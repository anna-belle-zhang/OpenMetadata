---
created: 2026-03-05
updated: 2026-03-06
status: COMPLETED
owner: data-platform
---

# PostgreSQL Audit System — Local Deployment Guide

Deploys the audit pipeline to the local WSL2 dev environment:
- Native Postgres (`localhost:5432`)
- Dockerised Airflow (`om_airflow`, DAGs from `/mnt/e/A/infra-devops/openmetadata/airflow/dags`)

## Deployment status (2026-03-06)

All steps completed. DAG registered and all 5 tasks run to `success`.

| Step | Status |
|---|---|
| 1 — SQL schema | ✅ Done |
| 2 — Python files in container | ✅ Done |
| 3 — DAG wrapper | ✅ Done |
| 4 — Env vars | ✅ Done (added to docker-compose-infra.yml) |
| 5 — Dry run | ✅ Done (all tasks `success`) |
| 6 — Real run with live clients | ⏳ Next — real ADO/Airflow API clients not yet wired |

---

## Prerequisites

- `om_airflow` Docker container running (`docker ps | grep airflow`)
- Native Postgres running (`sudo service postgresql status`)
- OM server running (`curl -s http://localhost:8586/healthcheck`)

---

## Step 1 — Apply SQL schema

Creates `audit_db` database and the `audit` schema with both tables and the materialized view.

> **Lesson:** Native WSL2 Postgres uses peer authentication — `psql -U postgres` fails unless run
> via `sudo -u postgres psql`. All psql commands below use that form.

```bash
# Create the database (safe to re-run)
sudo -u postgres psql -c "CREATE DATABASE audit_db;" 2>/dev/null || echo "audit_db already exists"

# Grant access to airflow_user (used by the container — see Step 4)
sudo -u postgres psql -d audit_db -c "
  GRANT CONNECT ON DATABASE audit_db TO airflow_user;
  GRANT ALL PRIVILEGES ON SCHEMA audit TO airflow_user;
  GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA audit TO airflow_user;
  ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT ALL ON TABLES TO airflow_user;"

# Apply schema
sudo -u postgres psql -d audit_db -f /mnt/e/A/OpenMetadata/ingestion/sql/audit_schema.sql

# Verify
sudo -u postgres psql -d audit_db -c "\dt audit.*"
```

Expected output:
```
             List of relations
 Schema |          Name          | Type  |  Owner
--------+------------------------+-------+----------
 audit  | ado_pipeline_runs      | table | postgres
 audit  | airflow_dag_executions | table | postgres
```

> The materialized view `audit.daily_audit_summary` is created but empty until first refresh.

---

## Step 2 — Copy Python files into the Airflow container and DAGs directory

Two copies are needed:

1. **Container site-packages** — so `import metadata.ingestion.source.pipeline.ado.*` resolves
   at runtime when tasks execute.
2. **DAGs directory package tree** — so the `dag-processor` subprocess can import the module
   at parse time (the dag-processor runs with a fresh Python env that pre-loads the DAGs dir
   onto `sys.path` but not necessarily the right namespace).

> **Lesson:** `docker exec om_airflow python -c "import metadata, os; ..."` fails because
> `metadata/__init__.py` in this image imports `metadata.generated` which was never installed.
> Fix: replace the `__init__.py` with an empty stub (the generated schema is not needed by our
> pipeline code).

```bash
# 1. Stub out the broken metadata __init__ (one-time, lost on container restart)
docker exec om_airflow bash -c "
  cp /home/airflow/.local/lib/python3.10/site-packages/metadata/__init__.py \
     /home/airflow/.local/lib/python3.10/site-packages/metadata/__init__.py.bak
  echo '# stub' > /home/airflow/.local/lib/python3.10/site-packages/metadata/__init__.py"

# 2. Copy into container site-packages
SITE=/home/airflow/.local/lib/python3.10/site-packages
ADO=$SITE/metadata/ingestion/source/pipeline/ado
SRC=/mnt/e/A/OpenMetadata/ingestion/src/metadata/ingestion/source/pipeline/ado

docker exec om_airflow mkdir -p $ADO
docker cp $SRC/__init__.py                      om_airflow:$ADO/__init__.py
docker cp $SRC/models.py                        om_airflow:$ADO/models.py
docker cp $SRC/postgres_ingestion.py            om_airflow:$ADO/postgres_ingestion.py
docker cp $SRC/airflow_postgres_ingestion.py    om_airflow:$ADO/airflow_postgres_ingestion.py
docker cp $SRC/audit_postgres_pipeline.py       om_airflow:$ADO/audit_postgres_pipeline.py

# 3. Mirror the package tree into the DAGs directory
DAGS=/mnt/e/A/infra-devops/openmetadata/airflow/dags
for dir in \
  "$DAGS/metadata" \
  "$DAGS/metadata/ingestion" \
  "$DAGS/metadata/ingestion/source" \
  "$DAGS/metadata/ingestion/source/pipeline" \
  "$DAGS/metadata/ingestion/source/pipeline/ado"; do
  mkdir -p "$dir" && touch "$dir/__init__.py"
done

for f in models.py postgres_ingestion.py airflow_postgres_ingestion.py audit_postgres_pipeline.py; do
  cp "$SRC/$f" "$DAGS/metadata/ingestion/source/pipeline/ado/$f"
done
```

Verify:
```bash
docker exec om_airflow python -c \
  "from metadata.ingestion.source.pipeline.ado.audit_postgres_pipeline import setup_om_entities; print('OK')"
# → OK
```

> **Note:** The container copies are lost on restart. The DAGs directory tree persists (it is a
> host volume mount). On restart, repeat the container-side steps (stub + docker cp).

---

## Step 3 — Create the Airflow DAG file

> **Lessons from first deployment:**
>
> - Airflow 3 removed `schedule_interval` — use `schedule` instead.
> - `airflow.operators.python.PythonOperator` is deprecated in Airflow 3 — use
>   `airflow.providers.standard.operators.python.PythonOperator`.
> - The `dag-processor` subprocess has a fresh Python interpreter. It pre-loads the DAGs
>   directory onto `sys.path`, but `metadata.ingestion` is a **namespace package** (no
>   `__init__.py`). `_NamespacePath._get_parent_path()` looks up `sys.modules['metadata']`
>   when iterating — if `metadata` was not already imported it raises `KeyError`. Fix: pre-import
>   `metadata` and `metadata.ingestion` explicitly before the chain import.
> - `generate_daily_report` returns `pathlib.Path`. Airflow 3 tries to push it as XCom and
>   fails with `cannot serialize PosixPath`. Fix: `do_xcom_push=False` on that task.

The current DAG file at
`/mnt/e/A/infra-devops/openmetadata/airflow/dags/audit_postgres_pipeline_dag.py`:

```python
"""Airflow DAG wrapper for audit_postgres_pipeline."""
import sys
import importlib

# Pre-import so _NamespacePath._get_parent_path() finds sys.modules['metadata']
# in the dag-processor subprocess before the namespace-package chain is traversed.
for _pkg in ("metadata", "metadata.ingestion"):
    if _pkg not in sys.modules:
        importlib.import_module(_pkg)

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from metadata.ingestion.source.pipeline.ado.audit_postgres_pipeline import (
    setup_om_entities,
    run_ado_ingestion,
    run_airflow_ingestion,
    refresh_materialized_view,
    generate_daily_report,
    RETRIES,
    RETRY_DELAY,
)

default_args = {
    "owner": "data-platform",
    "retries": RETRIES,
    "retry_delay": RETRY_DELAY,
    "email_on_failure": False,
}

with DAG(
    dag_id="audit_postgres_pipeline",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["audit", "postgres"],
) as dag:

    t1 = PythonOperator(task_id="setup_om_entities",  python_callable=setup_om_entities)
    t2 = PythonOperator(task_id="ado_ingest",         python_callable=run_ado_ingestion)
    t3 = PythonOperator(task_id="airflow_ingest",     python_callable=run_airflow_ingestion)
    t4 = PythonOperator(task_id="refresh_mv",         python_callable=refresh_materialized_view)
    t5 = PythonOperator(task_id="daily_report",       python_callable=generate_daily_report,
                        do_xcom_push=False)

    t1 >> t2 >> t3 >> t4 >> t5
```

After saving, the dag-processor picks it up within 30 s. Verify via the PostgreSQL backend
(not SQLite — see Lessons):

```bash
sudo -u postgres psql -d airflow_db -c \
  "SELECT dag_id, is_paused, fileloc FROM dag WHERE dag_id = 'audit_postgres_pipeline';"
```

---

## Step 4 — Set environment variables

> **Lesson:** `docker exec om_airflow bash -c "export FOO=bar"` does NOT persist env vars
> to running Airflow processes. `airflow variables set` stores to the DB (accessible via
> `Variable.get()`) but NOT via `os.environ`. The pipeline reads `os.environ` directly.
> The only reliable method without a container restart is `docker-compose ... restart airflow`
> after adding to the compose file.
>
> **Lesson:** Do not use the `postgres` superuser from inside Docker — peer auth only works
> from WSL2. Use `airflow_user`/`airflow_pass` (already configured in the container) and
> grant it access to `audit_db` (done in Step 1).

Add to `docker-compose-infra.yml` under `services.airflow.environment`:

```yaml
      ADO_PAT: "<your-ado-pat>"
      ADO_ORG_URL: "https://dev.azure.com/<your-org>"
      ADO_PROJECT: "<your-project>"
      POSTGRES_HOST: "host.docker.internal"
      POSTGRES_PORT: "5432"
      POSTGRES_DB: "audit_db"
      POSTGRES_USER: "airflow_user"
      POSTGRES_PASSWORD: "airflow_pass"
      AIRFLOW_BASE_URL: "http://host.docker.internal:8080"
      AIRFLOW_INSTANCE: "localhost"
      LOOKBACK_DAYS: "7"
```

These take effect on next `restart airflow` (env vars set at container start time).

---

## Step 5 — Trigger a run

> **Lesson:** The `airflow dags` CLI inside the container uses SQLite by default (from
> `airflow.cfg`), while the running Airflow processes use the PostgreSQL `airflow_db` set by
> `ingestion_dependency.sh`. Always pass `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` when using
> the CLI, otherwise commands like `unpause` and `trigger` silently operate on the wrong DB.
>
> **Lesson:** New DAGs start paused. `airflow dags unpause` via the CLI may silently no-op
> against the wrong DB. Use a direct SQL update as a reliable fallback:
> `sudo -u postgres psql -d airflow_db -c "UPDATE dag SET is_paused = false WHERE dag_id = 'audit_postgres_pipeline';"`

```bash
# Unpause
sudo -u postgres psql -d airflow_db -c \
  "UPDATE dag SET is_paused = false WHERE dag_id = 'audit_postgres_pipeline';"

# Trigger
docker exec \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@host.docker.internal:5432/airflow_db" \
  om_airflow airflow dags trigger audit_postgres_pipeline

# Monitor task states
sudo -u postgres psql -d airflow_db -c "
  SELECT task_id, state, try_number FROM task_instance
  WHERE dag_id = 'audit_postgres_pipeline'
  ORDER BY updated_at DESC LIMIT 10;"
```

Expected: all 5 tasks reach `success` within ~60 s.

> **Current limitation:** The pipeline functions (`run_ado_ingestion`, `run_airflow_ingestion`)
> default to `_noop_client()` stubs when no real client is injected. Tasks complete
> successfully but ingest 0 rows. Wiring real ADO and Airflow REST clients is the next step.

---

## Step 6 — Real run + verify (pending real clients)

Once real API clients are wired:

```bash
# Check rows landed in Postgres
sudo -u postgres psql -d audit_db -c "SELECT count(*) FROM audit.ado_pipeline_runs;"
sudo -u postgres psql -d audit_db -c "SELECT count(*) FROM audit.airflow_dag_executions;"

# Refresh view manually if needed
sudo -u postgres psql -d audit_db -c \
  "REFRESH MATERIALIZED VIEW CONCURRENTLY audit.daily_audit_summary;"

# Check report was written
docker exec om_airflow ls /opt/airflow/reports/ 2>/dev/null
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `FATAL: Peer authentication failed for user "postgres"` | Use `sudo -u postgres psql` — peer auth only works from WSL2 shell, not from Docker |
| DAG not visible / `KeyError: 'metadata'` in dag-processor log | The `metadata` namespace package isn't in `sys.modules` at parse time — ensure the DAG wrapper pre-imports `metadata` and `metadata.ingestion` before the chain import |
| `ModuleNotFoundError: metadata.generated` | `metadata/__init__.py` in the container imports uninstalled generated schema — replace with stub (Step 2) |
| `airflow dags list` returns "No data found" | CLI is hitting SQLite, not PostgreSQL — pass `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` env var or query `airflow_db` directly |
| DAG stuck in `queued` state | DAG is paused — run the `UPDATE dag SET is_paused = false` SQL (Step 5) |
| `daily_report` task retries with `cannot serialize PosixPath` | `generate_daily_report` returns `Path` — set `do_xcom_push=False` on the task (already fixed in current DAG file) |
| `Connection refused` to Postgres from container | Use `host.docker.internal` not `localhost` in `POSTGRES_HOST` |
| `duration_seconds` insert error | Column is generated — do not insert it manually |
| Container restart wipes container-side copies | Re-run the stub + docker cp commands from Step 2; DAGs directory tree persists |
| `openmetadata_managed_apis` plugin import error in CLI output | Non-fatal warning — same `metadata.generated` gap; does not affect DAG execution |
