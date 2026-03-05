# ADO Models - Living Spec

## Behaviors

### AdoRun carries git_sha and pipeline_type
GIVEN a `runs.json` record with a `git_sha` field and a `pipeline_type` field
WHEN `AdoRun` is parsed from that record
THEN `AdoRun.git_sha` equals the value from the record
AND `AdoRun.pipeline_type` equals `"image_build"` or `"infra_deploy"`

*Modified: 2026-03-03 via 3d-lineage-ingestion (was: AdoRun contained run_id, pipeline_name, start_time, result, image_tag, approved_by, approved_at only)*

### AdoRun requires git_sha
GIVEN a `runs.json` record missing the `git_sha` field
WHEN `AdoRun` is parsed
THEN a `ValidationError` is raised

*Modified: 2026-03-03 via 3d-lineage-ingestion (was: git_sha was not a required field)*
