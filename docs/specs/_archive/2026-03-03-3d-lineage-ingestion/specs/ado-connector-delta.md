# ADO Connector Delta Spec

## ADDED

### Create ADO PipelineService
GIVEN `ado-dumps/runs.json` exists and is valid
AND OpenMetadata is running and accessible
WHEN the ADO connector runs
THEN a `PipelineService` named `ADO` exists in OpenMetadata

### Create Pipeline entity per build run
GIVEN `ado-dumps/runs.json` contains build pipeline runs
WHEN the ADO connector runs
THEN a `Pipeline` entity exists for each build run under `PipelineService: ADO`
AND each entity has properties `run_id`, `image` (built image tag), and `date`

### Create Pipeline entity per deploy run
GIVEN `ado-dumps/runs.json` contains infrastructure deploy pipeline runs
WHEN the ADO connector runs
THEN a `Pipeline` entity exists for each deploy run under `PipelineService: ADO`
AND each entity has properties `run_id`, `version`, `env`, and `date`

### Attach approver to deploy run entity
GIVEN `ado-dumps/approvals.json` contains an approval record for a deploy run
WHEN the ADO connector runs
THEN the corresponding deploy run `Pipeline` entity has properties `approved_by` and `approved_at`

### Append-only — do not overwrite historical run entities
GIVEN ADO Pipeline entities for previous runs already exist in OpenMetadata
WHEN the ADO connector runs with new dump files containing additional runs
THEN existing run entities are not modified
AND new run entities are created for runs not yet in OpenMetadata

### Skip run on missing approval
GIVEN a deploy run exists in `runs.json` but has no matching record in `approvals.json`
WHEN the ADO connector runs
THEN the deploy run entity is created without `approved_by` / `approved_at` properties
AND no error is raised

### Skip on malformed dump file
GIVEN `ado-dumps/runs.json` exists but contains invalid JSON
WHEN the ADO connector runs
THEN the connector logs an error
AND exits with a non-zero code

## MODIFIED

### Create Pipeline entity per build run
**Was:** Each build run Pipeline entity has properties: `run_id`, `image` (built image tag), `date`.
**Now:** Each build run Pipeline entity also has custom properties `git_sha` and `pipeline_type=image_build`.
**Reason:** DAG enricher needs `git_sha` on the build entity to propagate it to DAG entities.

GIVEN `ado-dumps/runs.json` contains a build pipeline run with `git_sha` and `pipeline_type="image_build"`
WHEN the ADO connector runs
THEN the corresponding Pipeline entity in OM has custom property `git_sha` equal to the run's git SHA
AND the entity has custom property `pipeline_type` equal to `"image_build"`

### Create Pipeline entity per deploy run
**Was:** Each deploy run Pipeline entity has properties: `run_id`, `version`, `env`, `date`, `approved_by`, `approved_at`.
**Now:** Each deploy run Pipeline entity also has custom properties `git_sha` and `pipeline_type=infra_deploy`.
**Reason:** Enricher filters ADO entities by `pipeline_type` to find the most recent infra deploy.

GIVEN `ado-dumps/runs.json` contains an infra deploy run with `git_sha` and `pipeline_type="infra_deploy"`
WHEN the ADO connector runs
THEN the corresponding Pipeline entity in OM has custom property `git_sha` equal to the run's git SHA
AND the entity has custom property `pipeline_type` equal to `"infra_deploy"`
