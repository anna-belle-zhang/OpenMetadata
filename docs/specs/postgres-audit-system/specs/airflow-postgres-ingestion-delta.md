# Airflow Postgres Ingestion Delta Spec

## ADDED

### Incremental ingest from prod Airflow API
GIVEN `airflow_dag_executions` has rows with max `execution_date = T`
WHEN the ingestor runs
THEN only DAG runs with `execution_date > T` are fetched and upserted

### Fallback to lookback window
GIVEN `airflow_dag_executions` is empty
WHEN the ingestor runs
THEN DAG runs from `now() - LOOKBACK_DAYS` are fetched

### Configurable Airflow URL
GIVEN `AIRFLOW_BASE_URL` is set to `http://prod-airflow:8080`
WHEN the ingestor runs
THEN it connects to `http://prod-airflow:8080/api/v1/dags`

### Defaults to self when URL not set
GIVEN `AIRFLOW_BASE_URL` is not set
WHEN the ingestor runs
THEN it connects to `http://localhost:8080/api/v1/dags`

### Upsert is idempotent
GIVEN a DAG run with `(dag_run_id, dag_id, airflow_instance)` already exists
WHEN the ingestor runs again with the same run data
THEN exactly one row exists for that PK
AND mutable fields (state, end_date, duration_seconds, raw_data, ingested_at) are updated

### Malformed DAG run is skipped
GIVEN the Airflow API returns one DAG run that cannot be mapped
WHEN the ingestor processes the batch
THEN that run is logged and skipped
AND all other valid runs in the batch are upserted

### Dry run writes nothing
GIVEN `DRY_RUN=True`
WHEN the ingestor runs
THEN no rows are written to Postgres
AND mapped rows are printed to stdout
