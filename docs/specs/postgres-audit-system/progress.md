## Plan
- [x] Task 1: Extend AdoRun model to accept image_deploy pipeline type
- [x] Task 2: Implement ADO → Postgres ingestion (incremental, lookback fallback, mapper run_type→pipeline_type/subtype, skip malformed, dry-run, idempotent upsert)
- [x] Task 3: Implement Airflow → Postgres ingestion (configurable base URL, incremental with lookback fallback, skip malformed, dry-run, idempotent upsert)
- [x] Task 4: Build audit_postgres_pipeline Airflow DAG (task order + retries + dry-run propagation, idempotent setup_om_entities registering audit_db tables/view)
- [x] Task 5: Apply audit_db schema DDL (create schema/tables PKs, pipeline_subtype allows image_deploy, materialized view columns, duration generated)
- [x] Task 6: Generate audit daily report Markdown (writes 5 sections, defaults to yesterday, flags missing prd approvals, empty summary renders zeros)
- [x] Task 7: Register audit DB entities in OM (service + tables + view owner/tier, idempotent reruns)

## Issues
- Running `python -m pytest tests/unit/topology/pipeline/ -x -q` currently fails early because `metadata.generated.schema` package is not available in this environment (downstream tests outside new additions).

## Commits
(empty)
