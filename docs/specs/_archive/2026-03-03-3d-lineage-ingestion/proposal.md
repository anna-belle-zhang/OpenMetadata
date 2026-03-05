# Three-Dimensional Lineage Ingestion Proposal

## Intent

Manual audit reports cannot answer cross-dimensional questions: "Which deployment is writing to this gold table?", "Who approved the change that broke this pipeline?", "What downstream data changed when we removed MySQL?".

This feature connects three lineage dimensions — DevOps (ADO pipeline runs + approvals), Infrastructure (ACI containers, ACR images), and Data (PostgreSQL tables, Airflow DAGs, DBT models) — inside OpenMetadata so these questions are answerable from a single UI.

## Scope

**In scope (Stage 1):**
- `az_dump.sh` — WSL script that dumps Azure resource state (ACI, ACR, Blob, ARM) to JSON files
- `ado_dump.py` — script that dumps ADO pipeline run + approval history to JSON files
- `aci` connector — reads `az-dumps/` JSON files, creates native OM `Pipeline` and `StorageContainer` entities with tags
- `ado` connector — reads `ado-dumps/` JSON files, creates native OM `Pipeline` entities with audit properties
- `stitch_lineage.py` — posts lineage edges connecting DevOps → Infra → Data entities
- Example dump files in `ingestion/examples/lineage-dumps/`

**In scope (Stage 1.5 — Deployment Audit Matrix):**
- Add `git_sha` and `pipeline_type` to ADO dump output and `AdoRun` model
- Store `git_sha` and `pipeline_type` as custom properties on ADO Pipeline entities
- New `dag_enricher.py` — patches Airflow DAG entities with `image_build_ref`, `infra_deploy_ref`, `git_sha`, `run_end_timestamp`
- Compliance audit matrix: WHO/WHAT/WHEN/WHERE/WHY/WHICH-DATA/WHICH-CODE answerable from OM for any production day
- Delta Lake time-travel key: `run_end_timestamp` on each DAG entity usable directly as `AS OF TIMESTAMP` value

**Out of scope (Stage 1 + 1.5):**
- Airflow DAG orchestration (Stage 2)
- Custom OM entity types, JSON schemas, UI pages (Stage 3)
- OpenMetadata source code changes (DB migrations, new API endpoints, new UI components)
- Automated `az login` / service principal auth
- Triggering rollbacks (informational only — OM provides the `AS OF` key; human executes the Delta query)

## Impact

- **Users affected:** Data engineers running manual lineage syncs from WSL
- **Systems affected:** OpenMetadata catalog (new entities + lineage edges added via existing REST API); no OM source changes
- **Risk:** Low — reads only (az CLI, ADO REST API); writes only to OM via existing `/lineage` and entity endpoints; no OM DB or UI changes

## Success Criteria

- [ ] `az_dump.sh` produces valid JSON files for ACI, ACR, Blob, and ARM resources
- [ ] `ado_dump.py` produces valid JSON files for ADO pipeline runs and approvals
- [ ] ACI connector creates `Pipeline` entity for `aci-pipeline-prd` with `image_version` tag visible in OM UI
- [ ] ADO connector creates one `Pipeline` entity per build/deploy run with `approved_by` property
- [ ] Lineage graph in OM UI shows: ADO build run → ACR image → ACI container → Airflow DAG → bronze table → silver table → gold table
- [ ] Re-running ingesters is idempotent (existing entities updated, not duplicated)
- [ ] ADO history is append-only (old run entities not overwritten)
- [ ] ADO Pipeline entities carry `git_sha` and `pipeline_type` custom properties
- [ ] Airflow DAG entities carry `image_build_ref`, `infra_deploy_ref`, `git_sha`, `run_end_timestamp` custom properties
- [ ] `run_end_timestamp` on a DAG entity is usable directly as a Delta `AS OF TIMESTAMP` value
- [ ] `dag_enricher.py` is idempotent — re-running does not duplicate or corrupt existing properties
- [ ] Enricher handles no prior deploy gracefully (logs warning, continues)
