# ADO Connector - Living Spec

## Behaviors

### Create ADO PipelineService
GIVEN `ado-dumps/runs.json` exists and is valid
AND OpenMetadata is running and accessible
WHEN the ADO connector runs
THEN a `PipelineService` named `ADO` exists in OpenMetadata

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Create Pipeline entity per build run
GIVEN `ado-dumps/runs.json` contains build pipeline runs with `git_sha` and `pipeline_type="image_build"`
WHEN the ADO connector runs
THEN a `Pipeline` entity exists for each build run under `PipelineService: ADO`
AND each entity has properties `run_id`, `image` (built image tag), `date`, `git_sha`, and `pipeline_type="image_build"`

*Added: 2026-03-03 via 3d-lineage-ingestion*
*Modified: 2026-03-03 via 3d-lineage-ingestion (was: properties run_id, image, date only)*

### Create Pipeline entity per deploy run
GIVEN `ado-dumps/runs.json` contains infrastructure deploy pipeline runs with `git_sha` and `pipeline_type="infra_deploy"`
WHEN the ADO connector runs
THEN a `Pipeline` entity exists for each deploy run under `PipelineService: ADO`
AND each entity has properties `run_id`, `version`, `env`, `date`, `git_sha`, and `pipeline_type="infra_deploy"`

*Added: 2026-03-03 via 3d-lineage-ingestion*
*Modified: 2026-03-03 via 3d-lineage-ingestion (was: properties run_id, version, env, date only)*

### Attach approver to deploy run entity
GIVEN `ado-dumps/approvals.json` contains an approval record for a deploy run
WHEN the ADO connector runs
THEN the corresponding deploy run `Pipeline` entity has properties `approved_by` and `approved_at`

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Append-only — do not overwrite historical run entities
GIVEN ADO Pipeline entities for previous runs already exist in OpenMetadata
WHEN the ADO connector runs with new dump files containing additional runs
THEN existing run entities are not modified
AND new run entities are created for runs not yet in OpenMetadata

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Skip run on missing approval
GIVEN a deploy run exists in `runs.json` but has no matching record in `approvals.json`
WHEN the ADO connector runs
THEN the deploy run entity is created without `approved_by` / `approved_at` properties
AND no error is raised

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Skip on malformed dump file
GIVEN `ado-dumps/runs.json` exists but contains invalid JSON
WHEN the ADO connector runs
THEN the connector logs an error
AND exits with a non-zero code

*Added: 2026-03-03 via 3d-lineage-ingestion*
