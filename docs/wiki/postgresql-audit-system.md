# PostgreSQL Audit System (Daily Reporting)

**Owner:** data-platform  \
**Status:** Draft → align with design doc  \
**Last updated:** 2026-03-05  \
**Scope:** Store ADO + Airflow runs in Postgres (`audit_db.audit`), refresh daily summary, generate Markdown report, register entities in OpenMetadata (OM).

Source design: `docs/plans/2026-03-04-postgresql-audit-system-design.md`.

## Overview
- Daily Airflow DAG `audit_postgres_pipeline` ingests ADO pipeline runs and prod Airflow DAG runs into Postgres `audit_db.audit`.
- Materialized view `audit.daily_audit_summary` aggregates by date/source/env/job.
- Report writer outputs `reports/audit-YYYY-MM-DD.md` (defaults to yesterday).
- OM registration task creates/updates `audit_db` service, tables, and view (idempotent).

## Architecture
```
[setup_om_entities]
        ↓
[ADO Ingest] → audit.ado_pipeline_runs
        ↓
[Airflow Ingest] → audit.airflow_dag_executions
        ↓
[Refresh MV] → audit.daily_audit_summary (REFRESH CONCURRENTLY)
        ↓
[Daily Report] → reports/audit-YYYY-MM-DD.md

OM exposure: postgres_pipeline › audit_db › audit schema (tables + view)
```

## Components
- **ADO → Postgres ingestion** (`ingestion/src/metadata/ingestion/source/pipeline/ado/postgres_ingestion.py`)
  - Since marker: `max(queue_time)` by `ingested_by`; fallback `now()-LOOKBACK_DAYS`.
  - Validates with `AdoRun` model; maps `run_type`→`pipeline_type`, `pipeline_type`→`pipeline_subtype` (includes `image_deploy`).
  - Upsert on PK `(run_id, ado_project)`; mutable fields updated.
  - `DRY_RUN=true` prints rows only.

- **Airflow → Postgres ingestion** (`ingestion/src/metadata/ingestion/source/pipeline/ado/airflow_postgres_ingestion.py`)
  - Since marker: `max(execution_date)`; same fallback and dry-run behavior.
  - Upsert on PK `(dag_run_id, dag_id, airflow_instance)`.

- **Materialized view refresh** (`refresh_materialized_view` task)
  - Intended command: `REFRESH MATERIALIZED VIEW CONCURRENTLY audit.daily_audit_summary` after both ingestors.

- **Daily report generator** (`ingestion/src/metadata/ingestion/source/pipeline/ado/audit_postgres_pipeline.py`)
  - `REPORT_DATE` optional; defaults to yesterday (UTC).
  - Sections: Executive Summary, Top Failures, Longest Runs, Missing Approvals (prd deploys approval_pct<100), Counts by Pipeline/DAG.
  - Writes to `reports/audit-YYYY-MM-DD.md`.

- **OM registration** (`setup_om_entities` task in same module)
  - Creates/updates `DatabaseService` `audit_db` under existing `postgres_pipeline` service.
  - Registers tables `ado_pipeline_runs`, `airflow_dag_executions` and view `daily_audit_summary` under schema `audit_db.audit`.
  - Sets owner `DataPlatform`, tier `Gold` on the view. Idempotent guard `_om_created`.

## Data Model
DDL file: `ingestion/sql/audit_schema.sql` (apply via `psql -f ingestion/sql/audit_schema.sql`).

**audit.ado_pipeline_runs**
- PK `(run_id, ado_project)`.
- Key columns: `pipeline_type` CHECK in ('build','deploy'); `pipeline_subtype` CHECK in ('image_build','image_deploy','infra_deploy'); `duration_seconds` generated always as `finish_time - start_time` (seconds);
  `result`, `env`, `git_sha`, `image_tag`, `version`, `approved_by`, `raw_data JSONB`, `ingested_at/by`.

**audit.airflow_dag_executions**
- PK `(dag_run_id, dag_id, airflow_instance)`.
- Columns: `execution_date`, `state`, `end_date`, `duration_seconds`, `raw_data`, `ingested_at/by`.

**audit.daily_audit_summary (materialized view)**
- Group keys: `audit_date`, `source_system` ('ADO'|'Airflow'), `env`, `job_name`.
- Metrics: `total_runs`, `success_count`, `failure_count`, `success_rate`, `avg_duration_seconds`, `max_duration_seconds`, `approval_count`, `approval_pct`.
- Defined with `WITH NO DATA`; refresh concurrently post-ingestion.

## Configuration
- Environment variables: `ADO_PAT`, `ADO_PROJECT`, `ADO_ORG_URL`; `AIRFLOW_BASE_URL`; `POSTGRES_*` (host/port/db/user/password); `LOOKBACK_DAYS` (default 30); `DRY_RUN`; `REPORT_DATE` (optional override); `REPORT_OUTPUT_DIR` (defaults `reports/`).
- Airflow instance tag: `AIRFLOW_INSTANCE` env var (defaults `localhost`).

## DAG Behavior
- Tasks chained: `setup_om_entities` → `ado_ingest` → `airflow_ingest` → `refresh_mv` → `daily_report`.
- Retries: 2, delay 5m (see `RETRY_DELAY`, `RETRIES`).
- Dry-run short-circuits writes and report creation.

## Operations & Runbook
1) **Bootstrap DDL**: run `psql -f ingestion/sql/audit_schema.sql` on the audit Postgres instance.
2) **Schedule DAG**: daily at 02:00 (per design); ensure secrets env vars set on Airflow.
3) **Refresh MV**: rely on DAG task; manual refresh if schema changes: `REFRESH MATERIALIZED VIEW CONCURRENTLY audit.daily_audit_summary`.
4) **Report location**: `reports/audit-YYYY-MM-DD.md` on the DAG worker; ship to artifact store if needed.
5) **OM visibility**: verify `audit_db` service and tables in OM UI; rerun DAG to reconcile.
6) **Staleness check**: `SELECT max(ingested_at) FROM audit.ado_pipeline_runs;` (should be <24h old).

## Monitoring & Alerts
- Airflow task alerts on failure; consider Slack/email for DAG failure.
- Approval coverage: alert if `approval_pct` for prod deploys <90%.
- Locking risk: MV refresh uses CONCURRENTLY; schedule during low-load window.

## Testing
- Unit tests added:
  - Schema delta: `ingestion/tests/unit/topology/pipeline/test_audit_db_schema_delta.py`.
  - Report generator: `ingestion/tests/unit/topology/pipeline/test_audit_daily_report.py`.
  - OM registration + DAG chain: `ingestion/tests/unit/topology/pipeline/test_om_audit_registration_delta.py`, `test_audit_postgres_dag.py`.
- Integration (recommended): Postgres + recorded API fixtures to validate upsert/idempotency and MV aggregates.

## Rollout
- Week 1: apply DDL; deploy DAG skeleton with `setup_om_entities` only.
- Week 2: enable ADO ingest; validate upserts and report output.
- Week 3: enable Airflow ingest; enable MV refresh; backfill as needed.
- Week 4: tune indexes/partitions; tighten alert thresholds.

## References
- Spec deltas: `docs/specs/postgres-audit-system/specs/audit-db-schema-delta.md`, `audit-report-delta.md`, `om-audit-registration-delta.md`.
- Design doc: `docs/plans/2026-03-04-postgresql-audit-system-design.md`.
