# Three-Dimensional Lineage Ingestion Design

## Architecture

File-based ingestion in two phases: dump then ingest.

```
WSL2
├── az_dump.sh          → data/az-dumps/*.json     (current Azure state)
├── ado_dump.py         → data/ado-dumps/*.json    (ADO run history, append-only)
│
├── aci/metadata.py     reads az-dumps/ → OM API  (current state entities)
├── ado/metadata.py     reads ado-dumps/ → OM API (audit history entities)
└── stitch_lineage.py   POST /lineage edges        (cross-dimensional links)
```

No OpenMetadata source changes. All entities use native OM types. No DB migrations. No UI changes.

## Components

### az_dump.sh
WSL bash script. Requires `az login` (interactive, Stage 1). Outputs one JSON file per resource type to `ingestion/examples/lineage-dumps/az-dumps/`. Idempotent — re-run overwrites files with current state.

### ado_dump.py
Python script. Authenticates with ADO REST API via PAT token env var. Fetches pipeline runs and approvals since last sync (or all history on first run). Appends to `ingestion/examples/lineage-dumps/ado-dumps/`. Never overwrites existing run records.

### aci connector (`ingestion/src/metadata/ingestion/source/pipeline/aci/`)
Follows existing OM connector pattern (`metadata.py`, `models.py`, `connection.py`, `service_spec.py`). Reads `aci.json` and `acr_tags_*.json`. Creates/updates:
- `PipelineService: ACI-prd`
- `Pipeline: aci-pipeline-prd` with tag `image_version=<current>`
- `Task` per container with `image`, `cpu` tags
- `StorageService: ACR`
- `StorageContainer` per ACR repository with `latest_tag`

### ado connector (`ingestion/src/metadata/ingestion/source/pipeline/ado/`)
Same pattern. Reads `runs.json` and `approvals.json`. Creates:
- `PipelineService: ADO`
- `Pipeline` per build run: `run_id`, `image`, `date`
- `Pipeline` per deploy run: `run_id`, `version`, `env`, `approved_by`, `approved_at`

### stitch_lineage.py
Standalone script. Queries OM API to resolve entity FQNs, then posts lineage edges:
- `ADO/build-#{n}` → `ACR/{repo}` (builds)
- `ADO/deploy-#{n}` → `ACI-prd/aci-pipeline-prd` (deploys)
- `ACI-prd/aci-pipeline-prd` → `Airflow/{dag}` (runs)

### dag_enricher.py (Stage 1.5)
Standalone script. Runs after ADO connector and Airflow connector complete. For each Airflow DAG entity that has a `PipelineStatus.endDate`, finds the most recent ADO image build and infra deploy on or before that date, then patches four custom properties onto the DAG entity:
- `image_build_ref` — FQN of active ADO build Pipeline
- `infra_deploy_ref` — FQN of active ADO infra deploy Pipeline
- `git_sha` — propagated from the active build entity
- `run_end_timestamp` — ISO-8601 string from `PipelineStatus.endDate`; use directly as Delta `AS OF TIMESTAMP` value

## Data Flow

```
az CLI → az-dumps/*.json
                        ↓
              aci/metadata.py → POST /api/v1/services/pipelineServices
                              → POST /api/v1/pipelines
                              → POST /api/v1/services/storageServices
                              → POST /api/v1/containers

ADO REST API → ado-dumps/*.json
                        ↓
              ado/metadata.py → POST /api/v1/services/pipelineServices
                              → POST /api/v1/pipelines

              stitch_lineage.py → PUT /api/v1/lineage

              dag_enricher.py   → GET  /api/v1/pipelines (ADO entities, filter by pipeline_type)
                                → GET  /api/v1/pipelines (Airflow entities + PipelineStatus)
                                → PATCH /api/v1/pipelines/{fqn} (custom properties)
```

## Error Handling

- Dump scripts: fail fast on `az` / ADO API errors, do not write partial files
- Connectors: skip individual entities on mapping errors, log and continue; report summary at end
- Stitch: if a referenced entity FQN does not exist in OM, log warning and skip that edge
- All scripts: exit with non-zero code on any unrecoverable error
- `dag_enricher.py`: no prior ADO deploy before DAG run date → skip + warn, continue; DAG has no PipelineStatus → skip silently; OM API failure during patch → raise and halt

## Dependencies

- Azure CLI (`az`) installed in WSL2, authenticated via `az login`
- ADO PAT token in env var `ADO_PAT`
- OpenMetadata running at `http://localhost:8585` (or configurable via env var `OM_HOST`)
- OM admin credentials in env vars `OM_USER` / `OM_PASSWORD`
- Existing native connectors (PostgreSQL, Airflow, DBT) run first so Airflow DAG entities exist before stitch
