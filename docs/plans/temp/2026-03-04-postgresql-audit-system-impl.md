# PostgreSQL Audit System Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Persist ADO and Airflow pipeline runs in Postgres `audit_db.audit`, refresh a daily summary view, and produce a daily Markdown/HTML audit report.

**Architecture:** Two async ingestors write directly to Postgres tables; a materialized view aggregates daily metrics; a report script queries the view; Postgres is exposed to OpenMetadata via existing `postgres_pipeline` service.

**Tech Stack:** Python 3.10, asyncpg, aiohttp, Jinja2, cron/systemd timers, PostgreSQL 14+, pytest.

---

### Task 1: Database DDL (schema, tables, view)

**Files:**
- Create: `ingestion/sql/audit_schema.sql`
- Modify: `docs/plans/2026-03-04-postgresql-audit-system-design.md` (reference link to DDL if needed)
- Test: `ingestion/tests/sql/test_audit_schema.py`

**Step 1: Write DDL script**
- Add `CREATE SCHEMA audit;` and table definitions for `audit.ado_pipeline_runs` and `audit.airflow_dag_executions` with PKs, CHECK enums, generated `duration_seconds`, indexes, and materialized view `audit.daily_audit_summary` per design.

**Step 2: Add smoke test for DDL**
- In `test_audit_schema.py`, spin up temp Postgres (pytest-postgresql fixture or existing helper), run DDL file, assert tables, columns, and indexes exist; assert view refresh succeeds.

**Step 3: Document apply command**
- In file header comment, include `psql -f ingestion/sql/audit_schema.sql` and `REFRESH MATERIALIZED VIEW CONCURRENTLY audit.daily_audit_summary;` instructions.

### Task 2: ADO Ingestor

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/ado_postgres_ingestion.py`
- Create: `ingestion/tests/unit/topology/pipeline/test_ado_postgres_ingestion.py`

**Step 1: Implement config + client**
- Add `AdoIngestConfig` dataclass (ADO creds, PG creds, lookback, batch_size, dry_run) and asyncpg connection pool helper.

**Step 2: Implement fetch since marker**
- `_get_last_ingestion_timestamp` reading max `queue_time` where `ingested_by='ado_ingestion_script'`; fallback `utcnow - lookback_days`.

**Step 3: Implement async fetch + mapping**
- Use aiohttp to call ADO builds endpoint with date filter; map payload to row tuple including approvals, env, raw_data JSONB.

**Step 4: Implement bulk upsert**
- `INSERT ... ON CONFLICT (run_id, ado_project) DO UPDATE` updating mutable fields and `ingested_at=NOW()`.

**Step 5: Unit tests**
- Mock ADO API with aiohttp server/fixtures; mock asyncpg using pg_temp; assert since marker logic, upsert dedup, and data mapping enums.

**Step 6: CLI entry + cron example**
- Add `if __name__ == '__main__': asyncio.run(main())`; include env-driven defaults; docstring with cron snippet (15 min + daily full lookback).

### Task 3: Airflow Ingestor

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/airflow_postgres_ingestion.py`
- Create: `ingestion/tests/unit/topology/pipeline/test_airflow_postgres_ingestion.py`

**Step 1: Implement config + client**
- PG pool helper shared pattern; BasicAuth session from env.

**Step 2: Implement fetch since marker**
- `_get_last_execution_date` from table; fallback `utcnow - lookback_days`.

**Step 3: Fetch DAG runs + task stats**
- Call `/api/v1/dags/~/dagRuns` with `start_date_gte`; enrich with task instances to compute success/fail/skip counts and failed task id.

**Step 4: Bulk upsert**
- `INSERT ... ON CONFLICT (dag_run_id, dag_id, airflow_instance) DO UPDATE` for state, end_date, task counts, raw_data, ingested_at.

**Step 5: Unit tests**
- aiohttp mock API; pg_temp; assert dedup, state updates, task stats computed, env mapping.

**Step 6: CLI + cron**
- Env-driven main; cron every 10 min + daily full lookback at 03:00.

### Task 4: Daily Summary Refresh + Report

**Files:**
- Create: `ingestion/src/metadata/ingestion/source/pipeline/audit_daily_report.py`
- Create: `ingestion/tests/unit/topology/pipeline/test_audit_daily_report.py`
- Modify: `ingestion/sql/audit_schema.sql` (ensure view definition matches report needs)

**Step 1: View refresh helper**
- Function to `REFRESH MATERIALIZED VIEW CONCURRENTLY audit.daily_audit_summary` with logging and error handling.

**Step 2: Report generator**
- Query summary for target date; build sections (exec summary, failures, longest runs, missing approvals) using Jinja2 templates; output Markdown default, HTML optional.

**Step 3: CLI**
- Flags: `--date`, `--output`, `--format`, `--slack-webhook` (optional), `--email` (optional noop placeholder if not implemented); default output `reports/audit-YYYY-MM-DD.md`.

**Step 4: Tests**
- Use seeded pg_temp data to verify summary metrics, missing approvals logic, and rendering contains required sections; snapshot Markdown string.

### Task 5: OpenMetadata Wiring

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/smoke_ingest.py`
- Create: `ingestion/tests/unit/topology/pipeline/test_smoke_ingest_audit_pg.py`

**Step 1: Add Postgres audit entities**
- Functions to upsert database, schema, tables, and view under `postgres_pipeline` service with correct column definitions.

**Step 2: Optional lineage**
- Add feature-flagged lineage edges from `audit.daily_audit_summary` to configured gold FQNs.

**Step 3: Disable sample data**
- Ensure OM uses Postgres preview; remove/preserve existing jsonl push path behind flag for rollback.

**Step 4: Tests**
- Mock OM client; assert create/update calls, lineage flag behavior.

### Task 6: Ops & Monitoring

**Files:**
- Create: `ingestion/ops/audit_ingestion_cron.md`
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/ado_postgres_ingestion.py` and `airflow_postgres_ingestion.py` (metrics hooks)
- Create: `ingestion/tests/unit/topology/pipeline/test_audit_metrics.py`

**Step 1: Metrics hooks**
- Emit counts/timings to stdout JSON lines; make optional Prometheus pushgateway URL env var.

**Step 2: Cron/systemd examples**
- Document schedules, env vars, log paths, and restart policy in `audit_ingestion_cron.md`.

**Step 3: Alert thresholds**
- Define conditions (zero rows 24h, approval_pct <90% prd deploys, refresh failure) and how to integrate with existing alerting (stub commands or webhook notes).

### Task 7: Backfill & Retention

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/ado_postgres_ingestion.py`
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/airflow_postgres_ingestion.py`
- Create: `ingestion/tests/unit/topology/pipeline/test_audit_backfill.py`

**Step 1: Backfill mode**
- Add `--backfill-days` flag that overrides since marker for single run; guard against overlapping cron via lock file or advisory lock.

**Step 2: Retention toggle**
- Add optional pruning logic (SQL delete older than 24 months) behind env flag; unit test SQL generation only (no destructive default).

**Step 3: Tests**
- Validate backfill flag forces expected since date; retention flag builds correct SQL and is disabled by default.

### Task 8: Verification & Docs

**Files:**
- Modify: `docs/plans/2026-03-04-postgresql-audit-system-design.md` (link to plan + runbook)
- Create: `docs/runbooks/audit-postgres.md`

**Step 1: Verification script**
- Document end-to-end verification checklist: run ingestors with dry_run, refresh view, generate report, validate OM entities, validate alert queries.

**Step 2: Runbook**
- Add common failures (API auth, enum mismatch, REFRESH lock), and recovery steps.

