# 3D Lineage Stage 1 Gaps Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Finish Stage 1 spec coverage by adding missing unit tests and implementing ACR storage + idempotent ACI behavior; ensure deploy entities carry version/env and stitch posts ACI→Airflow edge.

**Architecture:** Tests-first per spec GIVEN/WHEN/THEN; minimal additions in existing connectors (`ado_dump`, `aci/metadata`, `ado/metadata`, `aci/stitch_lineage`). Use OM python client patterns already in repo; upsert entities to keep idempotent. ACR data pulled from az dump fixtures to populate StorageService/StorageContainers with tags.

**Tech Stack:** Python, Pydantic models, OpenMetadata ingestion connectors, pytest.

---

### Task 1: ado-dump writes approvals.json content

**Files:**
- Modify: `ingestion/tests/unit/topology/pipeline/test_ado_dump.py`
- Modify (expected impl only if needed): `ingestion/src/metadata/ingestion/source/pipeline/ado/ado_dump.py`

**Step 1: Write the failing test**
- Add test that runs `AdoDumper.dump_approvals` with sample API responses; assert `ado-dumps/approvals.json` contains runId, approver displayName, approvalTime.

**Step 2: Run test to verify it fails**
- `pytest ingestion/tests/unit/topology/pipeline/test_ado_dump.py::test_writes_approvals_json -q`
- Expected: FAIL (missing logic/fields).

**Step 3: Write minimal implementation**
- Ensure approvals are fetched and serialized with fields per spec; maintain append-only behavior if file exists.

**Step 4: Run test to verify it passes**
- Same pytest command; expect PASS.

**Step 5: Commit**
- `git add ingestion/tests/unit/topology/pipeline/test_ado_dump.py ingestion/src/metadata/ingestion/source/pipeline/ado/ado_dump.py`
- `git commit -m "test: cover ado approvals dump"`

### Task 2: ado-dump incremental sync only newer than last timestamp

**Files:**
- Modify: `ingestion/tests/unit/topology/pipeline/test_ado_dump.py`
- Modify (impl if needed): `ingestion/src/metadata/ingestion/source/pipeline/ado/ado_dump.py`

**Step 1: Write the failing test**
- Seed runs.json with latest timestamp T; mock API returns old+new runs; assert only newer than T are added and order preserved.

**Step 2: Run test to verify it fails**
- `pytest ingestion/tests/unit/topology/pipeline/test_ado_dump.py::test_incremental_sync_fetches_only_newer -q`
- Expected FAIL.

**Step 3: Write minimal implementation**
- Read existing runs, compute max start_time, request newer runs via `min_time` filter, append-only.

**Step 4: Run test to verify it passes**
- Same pytest command; expect PASS.

**Step 5: Commit**
- `git add ingestion/tests/unit/topology/pipeline/test_ado_dump.py ingestion/src/metadata/ingestion/source/pipeline/ado/ado_dump.py`
- `git commit -m "feat: incremental ado dump only fetch newer runs"`

### Task 3: aci connector run() creates ACI-prd service

**Files:**
- Modify: `ingestion/tests/unit/topology/pipeline/test_aci_metadata.py`
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`

**Step 1: Write the failing test**
- Mock `metadata.get_or_create` call; assert `get_or_create_service` invoked with name `ACI-prd` when `AciConnector.run()` executes.

**Step 2: Run test to verify it fails**
- `pytest ingestion/tests/unit/topology/pipeline/test_aci_metadata.py::test_run_calls_get_or_create_service -q`
- Expected FAIL.

**Step 3: Write minimal implementation**
- Ensure `run()` calls helper to create service before pipelines; reuse existing service creation helper.

**Step 4: Run test to verify it passes**
- Same pytest command; expect PASS.

**Step 5: Commit**
- `git add ingestion/tests/unit/topology/pipeline/test_aci_metadata.py ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`
- `git commit -m "test: ensure aci connector creates service"`

### Task 4: ACI tasks carry image and cpu tags

**Files:**
- Modify: `ingestion/tests/unit/topology/pipeline/test_aci_metadata.py`
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`

**Step 1: Write the failing test**
- For each container in fixture, assert resulting Task has tags `image=<repo>:<tag>` and `cpu=<value>`.

**Step 2: Run test to verify it fails**
- `pytest ingestion/tests/unit/topology/pipeline/test_aci_metadata.py::test_task_tags_include_image_and_cpu -q`
- Expected FAIL.

**Step 3: Write minimal implementation**
- When building Task, add tag Category/Tag to include image and cpu values per spec.

**Step 4: Run test to verify it passes**
- Same pytest command; expect PASS.

**Step 5: Commit**
- `git add ingestion/tests/unit/topology/pipeline/test_aci_metadata.py ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`
- `git commit -m "feat: tag aci tasks with image and cpu"`

### Task 5: ACR StorageService and StorageContainers with latest_tag

**Files:**
- Modify: `ingestion/tests/unit/topology/pipeline/test_aci_metadata.py`
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`

**Step 1: Write the failing test**
- Assert `StorageService` named `ACR` created; containers `airflow`, `crypt` under it; each has tag `latest_tag=<most recent tag>` from `acr_tags_*.json` fixtures.

**Step 2: Run test to verify it fails**
- `pytest ingestion/tests/unit/topology/pipeline/test_aci_metadata.py::test_creates_acr_storage_and_containers -q`
- Expected FAIL.

**Step 3: Write minimal implementation**
- Parse ACR tag dump files, compute most recent tag per repo, upsert StorageService/StorageContainer with tags; reuse OM client create/update.

**Step 4: Run test to verify it passes**
- Same pytest command; expect PASS.

**Step 5: Commit**
- `git add ingestion/tests/unit/topology/pipeline/test_aci_metadata.py ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`
- `git commit -m "feat: ingest acr storage containers with latest_tag"`

### Task 6: ACI idempotent re-run (no duplicates, tags updated)

**Files:**
- Modify: `ingestion/tests/unit/topology/pipeline/test_aci_metadata.py`
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`

**Step 1: Write the failing test**
- Run connector twice with updated dumps; assert counts unchanged (no duplicate pipelines/tasks/containers) and image_version/latest_tag reflect latest values.

**Step 2: Run test to verify it fails**
- `pytest ingestion/tests/unit/topology/pipeline/test_aci_metadata.py::test_idempotent_rerun_updates_tags_without_duplicates -q`
- Expected FAIL.

**Step 3: Write minimal implementation**
- Use upsert patterns and update tag values; ensure entity references reused not recreated.

**Step 4: Run test to verify it passes**
- Same pytest command; expect PASS.

**Step 5: Commit**
- `git add ingestion/tests/unit/topology/pipeline/test_aci_metadata.py ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`
- `git commit -m "feat: make aci connector idempotent"`

### Task 7: ADO deploy run carries version and env tags

**Files:**
- Modify: `ingestion/tests/unit/topology/pipeline/test_ado_metadata.py`
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/ado/metadata.py`

**Step 1: Write the failing test**
- For deploy run entity, assert custom properties `version=<run.version>` and `env=<run.environment>` set.

**Step 2: Run test to verify it fails**
- `pytest ingestion/tests/unit/topology/pipeline/test_ado_metadata.py::test_deploy_run_has_version_and_env -q`
- Expected FAIL.

**Step 3: Write minimal implementation**
- When building deploy Pipeline entity, set custom props version/env from run data (likely tags or properties map).

**Step 4: Run test to verify it passes**
- Same pytest command; expect PASS.

**Step 5: Commit**
- `git add ingestion/tests/unit/topology/pipeline/test_ado_metadata.py ingestion/src/metadata/ingestion/source/pipeline/ado/metadata.py`
- `git commit -m "feat: add version/env properties to deploy pipelines"`

### Task 8: Stitch lineage posts ACI → Airflow DAG edge

**Files:**
- Modify: `ingestion/tests/unit/topology/pipeline/test_stitch_lineage.py`
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/aci/stitch_lineage.py`

**Step 1: Write the failing test**
- Use fixtures with ACI pipeline and Airflow DAG; assert lineage edge created with description `runs` from `ACI-prd.aci-pipeline-prd` to `airflow_pipeline.aci_sf_encrypted_pipeline` (or equivalent FQN).

**Step 2: Run test to verify it fails**
- `pytest ingestion/tests/unit/topology/pipeline/test_stitch_lineage.py::test_posts_aci_to_airflow_edge -q`
- Expected FAIL.

**Step 3: Write minimal implementation**
- Add edge spec for ACI → Airflow DAG and post via OM client; ensure skip if DAG missing.

**Step 4: Run test to verify it passes**
- Same pytest command; expect PASS.

**Step 5: Commit**
- `git add ingestion/tests/unit/topology/pipeline/test_stitch_lineage.py ingestion/src/metadata/ingestion/source/pipeline/aci/stitch_lineage.py`
- `git commit -m "feat: stitch aci pipeline to airflow dag"`

### Task 9: Final verification

**Files:**
- N/A

**Step 1: Run full unit suite for pipeline topology**
- `pytest ingestion/tests/unit/topology/pipeline -q`
- Expected: all pass.

**Step 2: Optional smoke**
- If time, run `python ingestion/src/metadata/ingestion/source/pipeline/smoke_ingest.py` against dev OM (optional, not required if offline).

**Step 3: Commit aggregate**
- `git add .`
- `git commit -m "chore: close stage1 gaps for 3d lineage"`

**Step 4: Update progress**
- Mark tasks done in `docs/specs/3d-lineage-ingestion/progress.md` and note test additions.

