# ADO Models Delta Spec

## MODIFIED

### AdoRun carries git_sha and pipeline_type
**Was:** `AdoRun` contains: `run_id`, `pipeline_name`, `start_time`, `result`, `image_tag`, `approved_by`, `approved_at`.
**Now:** `AdoRun` also contains `git_sha: str` and `pipeline_type: Literal["image_build", "infra_deploy"]`.
**Reason:** Downstream connector and enricher both need these fields; model is the single source of truth for the parsed dump record.

GIVEN a `runs.json` record with a `git_sha` field and a `pipeline_type` field
WHEN `AdoRun` is parsed from that record
THEN `AdoRun.git_sha` equals the value from the record
AND `AdoRun.pipeline_type` equals `"image_build"` or `"infra_deploy"`

GIVEN a `runs.json` record missing the `git_sha` field
WHEN `AdoRun` is parsed
THEN a `ValidationError` is raised
