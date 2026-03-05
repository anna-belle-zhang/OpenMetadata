# ado-dump Delta Spec

## ADDED

### Dump ADO pipeline run history
GIVEN `ADO_PAT` env var is set with a valid PAT token
AND the ADO project contains pipeline runs
WHEN `ado_dump.py` is run
THEN `ado-dumps/runs.json` is written containing run ID, pipeline name, start time, result, and built image version for each run

### Dump ADO approval records
GIVEN `ADO_PAT` env var is set
AND pipeline runs have associated approval records
WHEN `ado_dump.py` is run
THEN `ado-dumps/approvals.json` is written containing run ID, approver display name, and approval timestamp for each approval

### Append-only history
GIVEN `ado-dumps/runs.json` already contains records from a previous run
WHEN `ado_dump.py` is run again
THEN new run records are appended and existing records are not modified or removed

### First-run full history
GIVEN `ado-dumps/runs.json` does not exist
WHEN `ado_dump.py` is run
THEN all available ADO pipeline run history is fetched and written

### Incremental sync
GIVEN `ado-dumps/runs.json` exists and contains a most-recent run timestamp
WHEN `ado_dump.py` is run
THEN only runs newer than that timestamp are fetched from the ADO API

### Fail fast on auth error
GIVEN `ADO_PAT` is missing or invalid
WHEN `ado_dump.py` is run
THEN the script exits with a non-zero code and writes no output files

## MODIFIED

### Dump ADO pipeline run history
**Was:** Each run record contains: run ID, pipeline name, start time, result, and built image version.
**Now:** Each run record also contains `git_sha` (the repo commit that triggered the run) and `pipeline_type` (`"image_build"` or `"infra_deploy"`).
**Reason:** Audit matrix requires knowing which code commit each run built/deployed, and distinguishing build runs from infra runs for the DAG enricher join logic.

GIVEN `ADO_PAT` env var is set with a valid PAT token
AND the ADO project contains pipeline runs
WHEN `ado_dump.py` is run
THEN `ado-dumps/runs.json` contains `git_sha` and `pipeline_type` for each run
AND `git_sha` is the `sourceVersion` from the ADO run resource
AND `pipeline_type` is `"image_build"` for runs on the image build pipeline definition
AND `pipeline_type` is `"infra_deploy"` for runs on the infra deploy pipeline definition

### Classify pipeline type by definition name
**Was:** No classification — all runs treated uniformly.
**Now:** Pipeline type is derived from the pipeline definition name at dump time.
**Reason:** Keeps classification logic in one place; connector and enricher stay simple.

GIVEN a run belongs to a pipeline whose definition name matches the configured image-build pipeline name
WHEN `ado_dump.py` classifies that run
THEN `pipeline_type` is `"image_build"` in the output record

GIVEN a run belongs to a pipeline whose definition name matches the configured infra-deploy pipeline name
WHEN `ado_dump.py` classifies that run
THEN `pipeline_type` is `"infra_deploy"` in the output record
