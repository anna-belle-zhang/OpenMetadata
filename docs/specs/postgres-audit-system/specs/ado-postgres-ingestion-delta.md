# ADO Postgres Ingestion Delta Spec

## ADDED

### Incremental ingest from ADO API
GIVEN `ado_pipeline_runs` has rows with max `queue_time = T`
WHEN the ingestor runs
THEN only ADO runs with `queue_time > T` are fetched and upserted

### Fallback to lookback window
GIVEN `ado_pipeline_runs` is empty
WHEN the ingestor runs
THEN runs from `now() - LOOKBACK_DAYS` are fetched

### Upsert is idempotent
GIVEN a run with `run_id=42, ado_project='myproj'` already exists
WHEN the ingestor runs again with the same run data
THEN exactly one row exists for `(42, 'myproj')`
AND mutable fields (result, approved_by, raw_data, ingested_at) are updated

### Mapper: run_type becomes pipeline_type
GIVEN an `AdoRun` with `run_type = 'deploy'` and `pipeline_type = 'infra_deploy'`
WHEN the mapper produces a row dict
THEN `pipeline_type = 'deploy'` and `pipeline_subtype = 'infra_deploy'`

### Malformed run is skipped
GIVEN the ADO API returns one run that fails `AdoRun` validation
WHEN the ingestor processes the batch
THEN that run is logged and skipped
AND all other valid runs in the batch are upserted

### Dry run writes nothing
GIVEN `DRY_RUN=True`
WHEN the ingestor runs
THEN no rows are written to Postgres
AND mapped rows are printed to stdout
