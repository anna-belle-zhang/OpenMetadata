# OM Audit Registration Delta Spec

## ADDED

### audit_db service registered in OM
GIVEN the `setup_om_entities` task runs for the first time
WHEN it completes
THEN a `DatabaseService` named `audit_db` exists under the `postgres_pipeline` service in OM

### Both tables and view registered
GIVEN `setup_om_entities` runs
WHEN it completes
THEN Table entities exist in OM for `ado_pipeline_runs`, `airflow_dag_executions`, and `daily_audit_summary`
AND all three are under schema `audit_db.audit`

### daily_audit_summary has correct ownership and tier
GIVEN `setup_om_entities` runs
WHEN it completes
THEN `daily_audit_summary` has owner `DataPlatform` and tier `Gold`

### Registration is idempotent
GIVEN all OM entities already exist
WHEN `setup_om_entities` runs again
THEN no duplicate entities are created
AND the task exits successfully
