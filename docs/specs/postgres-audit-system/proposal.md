# PostgreSQL Audit System Proposal

## Intent

Replace ad-hoc `audit_log.jsonl` file with a durable PostgreSQL store for ADO pipeline runs and Airflow DAG executions. Enables incremental ingestion, SQL-queryable history, and a structured daily Markdown report — all orchestrated by a single Airflow DAG in the existing OM-managed Airflow.

## Scope

**In scope:**
- Postgres schema `audit_db.audit` with `ado_pipeline_runs` and `airflow_dag_executions` tables + `daily_audit_summary` materialized view
- `ado_postgres_ingestion.py` — incremental, idempotent ingestor calling live ADO REST API
- `airflow_postgres_ingestion.py` — incremental, idempotent ingestor calling prod Airflow REST API
- `audit_daily_report.py` — daily Markdown report to disk from the materialized view
- Single Airflow DAG (`audit_postgres_pipeline`) orchestrating all four steps
- OM entity registration (tables + view) automated by DAG on first run
- `AdoRun.pipeline_type` extended with `"image_deploy"` value

**Out of scope:**
- Real-time or streaming ingestion
- BI dashboards; Markdown report only
- Weekly/monthly reports
- Email or Slack delivery of reports (disk only)
- Schema changes to existing gold tables
- UI self-serve connector forms (future phase)

## Impact

- **Users affected:** Data platform engineers monitoring ADO and Airflow pipeline health
- **Systems affected:** `ado/models.py` (model change), new Postgres schema, new Airflow DAG, OM `postgres_pipeline` service gains new entities
- **Risk:** Low — additive; existing `audit_log.jsonl` flow retained as fallback

## Success Criteria

- [ ] Both tables upsert correctly from live APIs; no duplicate rows on re-run
- [ ] `daily_audit_summary` view refreshes without locking errors
- [ ] Daily Markdown report renders with all five sections
- [ ] OM entities for both tables and the view exist after first DAG run
- [ ] DAG completes end-to-end in dev environment with `DRY_RUN=True`
