# DAG Enricher - Living Spec

## Behaviors

### Enrich DAG entity with active image build ref
GIVEN ADO Pipeline entities exist in OM with `pipeline_type="image_build"`
AND an Airflow DAG entity exists with a `PipelineStatus.endDate`
WHEN the DAG enricher runs
THEN the DAG entity's `image_build_ref` custom property is set to the FQN of the most recent
image build Pipeline whose date is on or before the DAG run's `endDate`

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Enrich DAG entity with active infra deploy ref
GIVEN ADO Pipeline entities exist in OM with `pipeline_type="infra_deploy"`
AND an Airflow DAG entity exists with a `PipelineStatus.endDate`
WHEN the DAG enricher runs
THEN the DAG entity's `infra_deploy_ref` custom property is set to the FQN of the most recent
infra deploy Pipeline whose date is on or before the DAG run's `endDate`

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Enrich DAG entity with git_sha from active image build
GIVEN the active image build Pipeline entity has a `git_sha` custom property
WHEN the DAG enricher runs
THEN the DAG entity's `git_sha` custom property is set to the same value as the active build's `git_sha`

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Enrich DAG entity with run_end_timestamp
GIVEN an Airflow DAG entity has `PipelineStatus.endDate` set by the Airflow connector
WHEN the DAG enricher runs
THEN the DAG entity's `run_end_timestamp` custom property is set to that `endDate` value as an ISO-8601 string

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Join uses most recent deploy on or before DAG run date
GIVEN two image build Pipeline entities exist with dates 2026-02-01 and 2026-02-18
AND a DAG run has `endDate` of 2026-02-20
WHEN the DAG enricher runs
THEN `image_build_ref` points to the 2026-02-18 build entity (not the 2026-02-01 one)

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Idempotent re-run
GIVEN a DAG entity already has `image_build_ref`, `infra_deploy_ref`, `git_sha`, and `run_end_timestamp` set
AND no new ADO deploy has occurred since the last enricher run
WHEN the DAG enricher runs again
THEN the custom property values on the DAG entity are unchanged

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Skip DAG with no prior deploy
GIVEN an Airflow DAG entity has a `PipelineStatus.endDate`
AND no ADO Pipeline entity of type `image_build` exists with a date on or before that `endDate`
WHEN the DAG enricher runs
THEN `image_build_ref` is not set on that DAG entity
AND the enricher logs a warning
AND continues processing remaining DAG entities without error

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Skip DAG with no run history
GIVEN an Airflow DAG entity exists but has no `PipelineStatus` (never ran)
WHEN the DAG enricher runs
THEN that DAG entity is skipped
AND no custom properties are set on it
AND the enricher continues without error

*Added: 2026-03-03 via 3d-lineage-ingestion*
