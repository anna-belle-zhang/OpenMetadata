---
created: 2026-03-04
status: DESIGN
owner: data-platform
---

# PostgreSQL Audit System — Design (Daily Reporting Scope)

## 1) Scope & Goals
- Store ADO pipeline runs and Airflow DAG runs in Postgres (`audit_db.audit`).
- Generate a single daily audit report from Postgres; no weekly/monthly reports.
- Surface tables (and optional summary view) in OpenMetadata via existing `postgres_pipeline` service.
- Keep ingestion incremental, idempotent, and low-ops; retain prior jsonl flow only as fallback.

## 2) Non-Goals
- No real-time streaming; daily batch is sufficient.
- No BI dashboards in this phase; Markdown daily report to disk only.
- No schema changes to gold tables; lineage remains lightweight.
- No cron/systemd timers; Airflow DAG is the scheduler.

## 3) Architecture

Single chained Airflow DAG (`audit_postgres_pipeline`, scheduled daily 02:00) running in OM-managed Airflow:

```
[setup_om_entities] → [ADO Ingest] → [Airflow Ingest] → [Refresh MV] → [Daily Report]
```

```
ADO REST API ──────────────────────────────▶ audit.ado_pipeline_runs
                                                       │
Prod Airflow REST API (AIRFLOW_BASE_URL) ──▶ audit.airflow_dag_executions
                                                       │
                                             REFRESH CONCURRENTLY
                                             audit.daily_audit_summary
                                                       │
                                             audit_daily_report.py
                                             → reports/audit-YYYY-MM-DD.md (disk)

audit_db exposed to OpenMetadata (postgres_pipeline service, registered by DAG on first run)
```

**Secrets:** All credentials (ADO PAT, Postgres, prod Airflow user/pass) retrieved from OM Secret Manager at runtime.

**Airflow URL:** Configurable via `AIRFLOW_BASE_URL` env var; defaults to `localhost:8080` (self) for local dev; points to separate prod Airflow in production.

## 4) Data Model

- Database/schema: `audit_db.audit` (separate from `public`).

### Model update required
Add `"image_deploy"` to existing `AdoRun.pipeline_type` in `ingestion/src/metadata/ingestion/source/pipeline/ado/models.py`:
```python
pipeline_type: Literal["image_build", "image_deploy", "infra_deploy"]
```

### Mapper: `AdoRun` → `ado_pipeline_runs`

| `AdoRun` field | DB column |
|---|---|
| `id` | `run_id` |
| `run_type` (`build`/`deploy`) | `pipeline_type` |
| `pipeline_type` (`image_build`/`image_deploy`/`infra_deploy`) | `pipeline_subtype` |
| `git_sha`, `image`, `version`, `env`, `start_time` | direct |

### Table `ado_pipeline_runs`
- PK `(run_id, ado_project)`
- Columns: run_number, pipeline_name, pipeline_type ENUM('build','deploy'), pipeline_subtype ENUM('image_build','image_deploy','infra_deploy'), result ENUM('succeeded','failed','canceled','running'), env ENUM('dev','uat','prd'), queue_time, start_time, finish_time, duration_seconds (generated), git_sha, source_branch, requested_by, approved_by, approval_timestamp, image_tag, version, ado_org_url, raw_data JSONB, ingested_at default now(), ingested_by.
- Indexes: date(queue_time), pipeline_name, result, env, (pipeline_type, pipeline_subtype).

### Table `airflow_dag_executions`
- PK `(dag_run_id, dag_id, airflow_instance)`
- Columns: execution_date, start_date, end_date, duration_seconds (generated), state ENUM('success','failed','running','upstream_failed','skipped'), total_tasks, successful_tasks, failed_tasks, skipped_tasks, data_volume_gb, processed_records, conf JSONB, env ENUM('dev','uat','prd'), upstream_datasets TEXT[], downstream_datasets TEXT[], error_message, failed_task_id, raw_data JSONB, ingested_at default now(), ingested_by.
- Indexes: date(execution_date), dag_id, state, env.

- Optional partitioning: monthly partitions on queue_time/execution_date once volume >10M rows.

## 5) Ingestion Services

Two async scripts (`asyncio` + `asyncpg` + `aiohttp`), each running as a task in the Airflow DAG.

**`ado_postgres_ingestion.py`**
1. Fetch ADO PAT + Postgres creds from OM Secret Manager.
2. Since-marker: `SELECT max(queue_time) FROM audit.ado_pipeline_runs WHERE ingested_by = 'ado_postgres_ingestion'`; fallback to `now() - LOOKBACK_DAYS` (default 30).
3. Page ADO Runs API (`GET /{org}/{project}/_apis/pipelines/runs?minTime=...`); fetch approvals separately, join on `run_id`.
4. Parse into `AdoRun`/`AdoApproval` (reuse existing models from `ingestion/.../ado/models.py`).
5. Map via mapper (run_type→pipeline_type, pipeline_type→pipeline_subtype).
6. Upsert batch: `INSERT ... ON CONFLICT (run_id, ado_project) DO UPDATE` for mutable fields (result, approval fields, raw_data, ingested_at).
7. Retry with jitter on 429/5xx (max 3); log + skip malformed rows; `DRY_RUN=True` prints without writing.

**`airflow_postgres_ingestion.py`**
1. Fetch prod Airflow creds + Postgres creds from OM Secret Manager.
2. Connect to `AIRFLOW_BASE_URL` (default `localhost:8080`).
3. Since-marker: `max(execution_date)` from `airflow_dag_executions`; fallback to `now() - LOOKBACK_DAYS`.
4. Page `GET /api/v1/dags` → `GET /api/v1/dags/{dag_id}/dagRuns?start_date_gte=...`.
5. Map to row dict; upsert on `(dag_run_id, dag_id, airflow_instance)`.
6. Same retry/skip/dry-run pattern.

Config env vars: `ADO_PAT`, `ADO_ORG_URL`, `ADO_PROJECT`, `AIRFLOW_BASE_URL`, `POSTGRES_*` (host/port/db/user/password), `LOOKBACK_DAYS`, `BATCH_SIZE`, `DRY_RUN`.

## 6) Daily Summary & Report

- Materialized view `audit.daily_audit_summary` (refreshed by DAG after both ingestors complete): groups by audit_date, source_system ('ADO'/'Airflow'), env, job_name.
- Metrics per group: total runs, successes, failures, success_rate, avg/max duration, approval_count & approval_pct (ADO deploys, prd env).
- Report generator `audit_daily_report.py`:
  - Inputs: Postgres creds (from OM Secret Manager), `REPORT_DATE` (default yesterday), `REPORT_OUTPUT_DIR` (default `reports/`).
  - Sections: executive summary by source/env; top N failures; longest runs; missing approvals (prd deploys with approval_pct < 100%); counts by pipeline/dag.
  - Output: `reports/audit-YYYY-MM-DD.md` (disk only).

## 7) OpenMetadata Integration

**`setup_om_entities` task (first task in DAG, idempotent):**
1. `create_or_update` `DatabaseService` for `audit_db` under existing `postgres_pipeline` service.
2. `create_or_update` `Database` → `Schema` → `Table` for `ado_pipeline_runs`, `airflow_dag_executions`, `daily_audit_summary`.
3. Set owner `DataPlatform`, tier `Gold` on `daily_audit_summary`.
4. Add lineage edges: `daily_audit_summary` → configured gold table FQNs (optional).
5. Uses OM Python SDK `create_or_update`; safe to re-run — no state file needed.

Disable sample-data push; rely on Postgres previews.

**UI self-serve (future):** "ADO Audit" and "Airflow Audit" connector forms in OM UI; registers runs as `ingestionPipeline` entities. Until UI ships, config is env vars fed to DAGs.

## 8) Operations & Monitoring

- Airflow UI: primary observability for task success/failure and duration.
- Alerts: Airflow email/Slack on DAG failure; alert if `audit_date` gap > 24h detected in summary view; approval_pct < 90% for prd deploys.
- Staleness check: `SELECT max(ingested_at) FROM audit.ado_pipeline_runs`.
- Retention: base tables 24 months; raw_data trimmed to 90 days (future column split); monthly partition pruning when enabled.
- DAG task retries: `retries=2`, `retry_delay=5min`.

## 9) Testing Strategy

- Unit: mapper functions (`AdoRun` → row dict), since-marker logic, enum validation, report templating.
- Integration: Postgres in CI (`pytest` + `asyncpg`); recorded API fixtures; assert upsert idempotency, PK constraints, view aggregates.
- E2E (dev): `DRY_RUN=True` DAG run against real APIs; validate report output contains key sections.

## 10) Risks & Mitigations

- API shape drift (ADO/Airflow): JSON-schema contract tests; tolerate extra fields; log-and-skip unknown enums.
- Data volume growth: enable monthly partitions; add indexes on newest partitions only; compress raw_data.
- Approval sparsity: report highlights missing approvals; keeps raw_data for audit.
- Locking on REFRESH: use `CONCURRENTLY`; schedule during low-load window.

## 11) Rollout

- Week 1: create schema/tables/indexes/view; deploy DAG skeleton with `setup_om_entities` only.
- Week 2: enable ADO ingest task; validate upserts and daily report.
- Week 3: enable Airflow ingest task; enable summary refresh; add optional lineage.
- Week 4: tune indexes/partitions if needed; tighten alert thresholds.
