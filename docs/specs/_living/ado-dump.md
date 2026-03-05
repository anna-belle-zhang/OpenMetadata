# ADO Dump - Living Spec

## Behaviors

### Dump ADO pipeline run history
GIVEN `ADO_PAT` env var is set with a valid PAT token
AND the ADO project contains pipeline runs
WHEN `ado_dump.py` is run
THEN `ado-dumps/runs.json` is written containing run ID, pipeline name, start time, result,
built image version, `git_sha` (the `sourceVersion` from the ADO run resource),
and `pipeline_type` (`"image_build"` or `"infra_deploy"`)

*Added: 2026-03-03 via 3d-lineage-ingestion*
*Modified: 2026-03-03 via 3d-lineage-ingestion (was: run ID, pipeline name, start time, result, image version only)*

### Classify pipeline type by definition name
GIVEN a run belongs to a pipeline whose definition name matches the configured image-build pipeline name
WHEN `ado_dump.py` classifies that run
THEN `pipeline_type` is `"image_build"` in the output record

GIVEN a run belongs to a pipeline whose definition name matches the configured infra-deploy pipeline name
WHEN `ado_dump.py` classifies that run
THEN `pipeline_type` is `"infra_deploy"` in the output record

*Modified: 2026-03-03 via 3d-lineage-ingestion (was: no classification — all runs treated uniformly)*

### Dump ADO approval records
GIVEN `ADO_PAT` env var is set
AND pipeline runs have associated approval records
WHEN `ado_dump.py` is run
THEN `ado-dumps/approvals.json` is written containing run ID, approver display name, and approval timestamp for each approval

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Append-only history
GIVEN `ado-dumps/runs.json` already contains records from a previous run
WHEN `ado_dump.py` is run again
THEN new run records are appended and existing records are not modified or removed

*Added: 2026-03-03 via 3d-lineage-ingestion*

### First-run full history
GIVEN `ado-dumps/runs.json` does not exist
WHEN `ado_dump.py` is run
THEN all available ADO pipeline run history is fetched and written

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Incremental sync
GIVEN `ado-dumps/runs.json` exists and contains a most-recent run timestamp
WHEN `ado_dump.py` is run
THEN only runs newer than that timestamp are fetched from the ADO API

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Fail fast on auth error
GIVEN `ADO_PAT` is missing or invalid
WHEN `ado_dump.py` is run
THEN the script exits with a non-zero code and writes no output files

*Added: 2026-03-03 via 3d-lineage-ingestion*
