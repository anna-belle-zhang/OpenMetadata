# ADO Models Delta Spec

## MODIFIED

### AdoRun carries git_sha and pipeline_type
**Was:** `pipeline_type` accepts `"image_build"` or `"infra_deploy"` only
**Now:** `pipeline_type` also accepts `"image_deploy"`
**Reason:** ADO pipelines include an image_deploy subtype not previously modelled

GIVEN a `runs.json` record with `pipeline_type = "image_deploy"`
WHEN `AdoRun` is parsed from that record
THEN `AdoRun.pipeline_type` equals `"image_deploy"` without validation error
