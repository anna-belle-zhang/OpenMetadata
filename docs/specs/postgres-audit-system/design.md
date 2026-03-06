# PostgreSQL Audit System Design

## Architecture

Single Airflow DAG (`audit_postgres_pipeline`, daily 02:00) in OM-managed Airflow:

```
[setup_om_entities] → [ado_ingest] → [airflow_ingest] → [refresh_mv] → [daily_report]
```

Secrets (ADO PAT, Postgres creds, prod Airflow creds) injected via environment variables set on the Airflow container.

## Components

| Component | Responsibility |
|---|---|
| `ado_postgres_ingestion.py` | Calls live ADO REST API; maps `AdoRun`/`AdoApproval` → `ado_pipeline_runs`; incremental upsert |
| `airflow_postgres_ingestion.py` | Calls prod Airflow REST API (`AIRFLOW_BASE_URL`); maps DAG runs → `airflow_dag_executions`; incremental upsert |
| `audit_daily_report.py` | Queries `daily_audit_summary`; writes `reports/audit-YYYY-MM-DD.md` |
| `setup_om_entities` (DAG task) | Registers `audit_db` service + tables + view in OM via SDK; idempotent |
| `audit_db.audit` schema | Postgres schema with two base tables and one materialized view |

## Data Flow

```
ADO REST API → AdoRun/AdoApproval (existing models) → mapper → ado_pipeline_runs (upsert)
Prod Airflow REST API → raw dict → mapper → airflow_dag_executions (upsert)
Both tables → REFRESH CONCURRENTLY → daily_audit_summary
daily_audit_summary → SQL query → audit-YYYY-MM-DD.md
```

**Since-marker pattern:** Both ingestors read `max(queue_time / execution_date) WHERE ingested_by = '<script_name>'` to determine the lower bound; fallback to `now() - LOOKBACK_DAYS` (default 30).

## Model Change

`AdoRun.pipeline_type` extended: `Literal["image_build", "image_deploy", "infra_deploy"]`

**Mapper:** `AdoRun.run_type` → `pipeline_type` column; `AdoRun.pipeline_type` → `pipeline_subtype` column.

## Error Handling

- 429/5xx: exponential backoff with jitter, max 3 retries
- Malformed rows: log + skip; never abort the run
- `raw_data JSONB` retained for replay
- DAG task retries: `retries=2`, `retry_delay=5min`
- `DRY_RUN=True`: prints rows without writing to Postgres or OM

## Dependencies

- Environment variables for all credentials (`ADO_PAT`, `POSTGRES_*`, `AIRFLOW_BASE_URL`, etc.)
- Live ADO REST API (PAT auth)
- Prod Airflow REST API (`AIRFLOW_BASE_URL`, configurable; defaults to `localhost:8080`)
- OM Python SDK (`ometa_api.OpenMetadata`) for entity registration
- `asyncpg` + `aiohttp` for async Postgres + HTTP
