# 3D Lineage Ingestion — Stage 1 + 1.5 Progress

**Date:** 2026-03-02
**Branch:** main
**Tests:** 42/42 passing
**Smoke test:** ✅ Passed against http://localhost:8585 (2026-03-01)

---

## Implementation Status

| Component | Files | Status |
|-----------|-------|--------|
| ACI Pydantic models | `aci/models.py` | ✅ Done |
| ACI connection config | `aci/connection.py` | ✅ Done |
| ACI connector | `aci/metadata.py` | ✅ Done (partial — see gaps) |
| ACI dump script | `aci/az_dump.sh` | ✅ Done |
| ACI lineage stitcher | `aci/stitch_lineage.py` | ✅ Done |
| ADO Pydantic models | `ado/models.py` | ✅ Done — `git_sha` + `pipeline_type` added |
| ADO connection config | `ado/connection.py` | ✅ Done |
| ADO connector | `ado/metadata.py` | ✅ Done — `git_sha` + `pipeline_type` stored as tags |
| ADO dump script | `ado/ado_dump.py` | ✅ Done — extracts `git_sha`, classifies `pipeline_type` |
| DAG enricher | `dag_enricher.py` | ✅ Done (Stage 1.5) |
| Example dump files | `examples/lineage-dumps/` | ✅ Done — updated with `git_sha` + `pipeline_type` |
| Test fixtures | `tests/unit/resources/datasets/` | ✅ Done — updated with `git_sha` + `pipeline_type` |

---

## Spec Coverage

### Stage 1 — Original 26 scenarios

**✅ Covered (16/26)**

| Scenario | Test |
|----------|------|
| ado-dump: Dump run history → runs.json | `test_writes_runs_json_on_first_run` |
| ado-dump: First-run full history | `test_writes_runs_json_on_first_run` |
| ado-dump: Append-only history | `test_appends_new_runs_on_second_run` |
| ado-dump: Fail fast on auth error | `test_missing_pat_raises` |
| aci-connector: Pipeline with image_version + env tags | `test_build_pipeline_entity_has_image_version_tag` |
| aci-connector: Pipeline name matches ACI group | `test_build_pipeline_entity_name` |
| aci-connector: Skip on malformed aci.json (raises) | `test_malformed_aci_json_raises` |
| ado-connector: Create ADO PipelineService | `test_creates_ado_pipeline_service` |
| ado-connector: Pipeline per build run (run_id, image) | `test_creates_pipeline_per_build_run` + `test_build_run_has_image_property` |
| ado-connector: Attach approver to deploy run | `test_deploy_run_has_approved_by` |
| ado-connector: Append-only (existing entities not re-created) | `test_existing_entity_not_duplicated` |
| ado-connector: Skip run on missing approval (no error) | `test_missing_approval_does_not_raise` |
| ado-connector: Malformed runs.json → exception | `test_malformed_runs_json_exits_nonzero` |
| stitch-lineage: ADO build → ACR image edge | `test_posts_ado_build_to_acr_edge` |
| stitch-lineage: ADO deploy → ACI pipeline edge | `test_posts_ado_deploy_to_aci_edge` |
| stitch-lineage: Skip missing entity, continue others | `test_skips_edge_when_from_entity_not_found` + `test_other_edges_posted_when_one_skipped` |
| stitch-lineage: Idempotent re-run | `test_idempotent_add_lineage_called_once_per_edge` |

**❌ Still missing from Stage 1 (9/26)**

| # | Scenario | Effort |
|---|----------|--------|
| 1 | ado-dump: Dump approval records → `approvals.json` content verified | Small |
| 2 | ado-dump: Incremental sync (only fetch runs newer than last timestamp) | Small |
| 3 | aci-connector: `run()` calls `get_or_create_service` with `ACI-prd` | Small |
| 4 | aci-connector: Task objects carry `image=` and `cpu=` tags | Small |
| 5 | aci-connector: Create ACR `StorageService` named `ACR` | Medium |
| 6 | aci-connector: Create ACR `StorageContainers` with `latest_tag` | Medium |
| 7 | aci-connector: Idempotent re-run (no duplicate entities) | Small |
| 8 | ado-connector: Deploy run has `version=` and `env=` tags | Small |
| 9 | stitch-lineage: ACI → Airflow DAG edge is posted | Small |

| # | Scenario | Reason |
|---|----------|--------|
| 10 | stitch-lineage: Full cross-dimensional path traversable in OM UI | ✅ Verified live — all 4 pipeline nodes + ACR container visible in lineage UI |

### Stage 1.5 — Deployment Audit Matrix (17 new scenarios)

**✅ All 17 covered**

| Scenario | Test |
|----------|------|
| ado-models: `AdoRun` parses `git_sha` and `pipeline_type` | `test_ado_run_has_git_sha_and_pipeline_type` |
| ado-models: infra deploy run has `pipeline_type=infra_deploy` | `test_ado_run_infra_has_pipeline_type_infra_deploy` |
| ado-models: missing `git_sha` raises `ValidationError` | `test_ado_run_missing_git_sha_raises` |
| ado-dump: `runs.json` contains `git_sha` from `resources.repositories.self.version` | `test_runs_json_contains_git_sha` |
| ado-dump: build run classified as `pipeline_type=image_build` | `test_runs_json_classifies_pipeline_type_build` |
| ado-dump: deploy run classified as `pipeline_type=infra_deploy` | `test_runs_json_classifies_pipeline_type_infra_deploy` |
| ado-connector: build entity tag includes `git_sha` | `test_build_run_has_git_sha_tag` |
| ado-connector: build entity tag includes `pipeline_type=image_build` | `test_build_run_has_pipeline_type_image_build_tag` |
| ado-connector: deploy entity tag includes `pipeline_type=infra_deploy` | `test_deploy_run_has_pipeline_type_infra_deploy_tag` |
| dag-enricher: sets `image_build_ref` on DAG entity | `test_enrich_sets_image_build_ref` |
| dag-enricher: sets `infra_deploy_ref` on DAG entity | `test_enrich_sets_infra_deploy_ref` |
| dag-enricher: propagates `git_sha` from active build | `test_enrich_propagates_git_sha` |
| dag-enricher: sets `run_end_timestamp` as ISO-8601 string | `test_enrich_sets_run_end_timestamp` |
| dag-enricher: picks most recent build on or before DAG run date | `test_enrich_picks_most_recent_build_before_dag_run` |
| dag-enricher: idempotent re-run produces identical props | `test_enrich_is_idempotent` |
| dag-enricher: skips DAG with no prior build (warning logged, others continue) | `test_enrich_skips_dag_with_no_prior_build` |
| dag-enricher: skips DAG with no run history silently | `test_enrich_skips_dag_with_no_run_history` |

---

## Smoke Test Results (2026-03-01)

**Script:** `ingestion/src/metadata/ingestion/source/pipeline/smoke_ingest.py`
**Target:** http://localhost:8585 (admin/admin)
**Dump files:** `ingestion/examples/lineage-dumps/`

| Entity | FQN | Status |
|--------|-----|--------|
| Pipeline service | `ADO` | ✅ Created |
| Pipeline service | `ACI-prd` | ✅ Created |
| Pipeline service | `airflow_pipeline` | ✅ Created |
| Storage service | `ACR` | ✅ Created |
| Pipeline | `ADO.build-232` | ✅ Created |
| Pipeline | `ADO.infra-deploy-233` | ✅ Created |
| Pipeline | `ACI-prd.aci-pipeline-prd` | ✅ Created |
| Container | `ACR.airflow` | ✅ Created |
| Pipeline | `airflow_pipeline.aci_sf_encrypted_pipeline` | ✅ Created |
| Lineage | `ADO.build-232` → `ACR.airflow` [builds] | ✅ Posted |
| Lineage | `ADO.infra-deploy-233` → `ACI-prd.aci-pipeline-prd` [deploys] | ✅ Posted |
| Lineage | `ACI-prd.aci-pipeline-prd` → `airflow_pipeline.aci_sf_encrypted_pipeline` [runs] | ✅ Posted |

All 4 pipeline nodes visible in OM lineage UI. Full 3D cross-dimensional path traversable.

---

## Known Production Issues Fixed

| Issue | File | Fix Applied |
|-------|------|-------------|
| `get_by_name` missing `entity=` arg | `stitch_lineage.py` | Lazy import + `entity=entity_cls, fqn=` |
| `get_by_name(name=...)` wrong kwarg | `ado/metadata.py` | Changed to `entity=Pipeline, fqn=` |
| Approval `runId` used pipeline definition ID | `ado/ado_dump.py` | Try `run.id` first, fall back to `pipeline.id` |

---

## Next Actions

1. **Write 9 missing Stage 1 unit tests** (items 1–9 above) — tests only, no new implementation
2. **Implement ACR StorageService + StorageContainers** in `aci/metadata.py` (items 5–6)
3. **Wire `dag_enricher.run()`** — implement `_load_ado_entities` + `_load_airflow_dags` against live OM API (currently stubs raising `NotImplementedError`)
4. **Archive specs** once all Stage 1 scenarios are covered

---

## File Locations

```
ingestion/src/metadata/ingestion/source/pipeline/
├── smoke_ingest.py        # Standalone smoke test / real-data ingestion script
├── dag_enricher.py        # DagEnricher — stamps DAG entities with audit join keys (Stage 1.5)
├── aci/
│   ├── __init__.py
│   ├── models.py          # AciContainer, AciDump
│   ├── connection.py      # AciConfig
│   ├── metadata.py        # AciConnector
│   ├── az_dump.sh         # Azure CLI dump script
│   └── stitch_lineage.py  # LineageStitcher, EdgeSpec, build_edges()
└── ado/
    ├── __init__.py
    ├── models.py          # AdoRun (+ git_sha, pipeline_type), AdoApproval, RunType
    ├── connection.py      # AdoConfig
    ├── metadata.py        # AdoConnector (+ git_sha, pipeline_type tags)
    └── ado_dump.py        # AdoDumper — extracts git_sha, classifies pipeline_type

ingestion/tests/unit/topology/pipeline/
├── conftest.py            # sys.path + metadata.generated stubs
├── test_aci_models.py     # 3 tests
├── test_aci_metadata.py   # 4 tests
├── test_ado_models.py     # 6 tests  (+ 3 new)
├── test_ado_metadata.py   # 10 tests (+ 3 new)
├── test_ado_dump.py       # 6 tests  (+ 3 new)
├── test_stitch_lineage.py # 5 tests
└── test_dag_enricher.py   # 8 tests  (all new, Stage 1.5)

ingestion/examples/lineage-dumps/
├── az-dumps/
│   ├── aci.json
│   ├── acr_tags_airflow.json
│   ├── acr_tags_crypt.json
│   ├── resources.json
│   └── blobs.json
└── ado-dumps/
    ├── runs.json           # updated with git_sha + pipeline_type
    └── approvals.json

docs/
├── 2026-03-01-3d-lineage-ingestion-design.md
├── plans/
│   ├── 2026-03-01-3d-lineage-ingestion.md
│   ├── 2026-03-02-deployment-audit-matrix-design.md
│   └── 2026-03-02-deployment-audit-matrix.md
└── specs/3d-lineage-ingestion/
    ├── proposal.md        # updated with Stage 1.5 scope
    ├── design.md          # updated with dag_enricher component
    ├── progress.md        # this file
    └── specs/
        ├── az-dump-delta.md
        ├── ado-dump-delta.md        # + MODIFIED section
        ├── aci-connector-delta.md
        ├── ado-connector-delta.md   # + MODIFIED section
        ├── ado-models-delta.md      # new (Stage 1.5)
        ├── stitch-lineage-delta.md
        └── dag-enricher-delta.md    # new (Stage 1.5)
```
