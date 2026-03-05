# Audit DAG Delta Spec

## ADDED

### DAG runs tasks in order
GIVEN the `audit_postgres_pipeline` DAG is triggered
WHEN it executes
THEN tasks run in order: setup_om_entities → ado_ingest → airflow_ingest → refresh_mv → daily_report
AND each task only starts after the previous succeeds

### setup_om_entities is idempotent
GIVEN OM entities for `audit_db` already exist from a previous DAG run
WHEN `setup_om_entities` runs again
THEN no duplicate entities are created
AND the task succeeds

### DAG task retries on failure
GIVEN a task fails with a transient error
WHEN Airflow retries the task
THEN it retries up to 2 times with a 5-minute delay before marking the DAG run failed

### Dry run mode propagates
GIVEN `DRY_RUN=True` is set in the DAG config or environment
WHEN the DAG runs
THEN both ingest tasks write nothing to Postgres
AND the report task writes nothing to disk
