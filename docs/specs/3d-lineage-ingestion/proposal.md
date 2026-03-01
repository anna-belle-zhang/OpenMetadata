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

**Out of scope (Stage 1):**
- Airflow DAG orchestration (Stage 2)
- Custom OM entity types, JSON schemas, UI pages (Stage 3)
- OpenMetadata source code changes (DB migrations, new API endpoints, new UI components)
- Automated `az login` / service principal auth

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
