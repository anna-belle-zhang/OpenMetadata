# OpenMetadata: Three-Dimensional Lineage Ingestion Design

**Created:** 2026-03-01
**Status:** Design — Approved
**Scope:** Data lineage + Infrastructure lineage + DevOps lineage — Stage 1 file-based approach

---

## Problem

Manual audit reports (`audit-report-*.md`, `dag-dbt-version-audit-*.md`) cannot answer:

- "What data was affected when we removed MySQL on 2026-02-16?"
- "Which git commit caused the gold table to change last Tuesday?"
- "If I change `silver_customers.sql`, what downstream gold tables break?"
- "Who approved the deployment that is currently writing to `gold.dim_sf_members_scd2`?"

These questions span three lineage dimensions that today live in separate systems with no connections.

---

## Three Lineage Dimensions

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                      │
│   DevOps Lineage          Infra Lineage          Data Lineage        │
│   ─────────────           ─────────────          ─────────────       │
│   git commit              Terraform state        Parquet file        │
│       │                       │                      │               │
│   ADO PR                  Resource Group         Crypt service       │
│       │                       │                      │               │
│   pipeline run            ACI container          Airflow DAG         │
│       │                       │                      │               │
│   build artifact          ACR image              bronze table        │
│       │                       │                      │               │
│   deployment              PostgreSQL             silver table        │
│       │                       │                      │               │
│   approval record         schema/table           gold table          │
│                                                                      │
│   ──────────────────────── intersections ────────────────────────── │
│                                                                      │
│   DevOps ──produces──► Infra (ACR image built by pipeline)          │
│   Infra   ──runs──────► Data (ACI container runs Airflow DAG)        │
│   DevOps  ──changes───► Data (new DAG version changes output table)  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Staged Approach (YAGNI)

Prove value cheaply first. Invest in automation and fidelity only if earned.

| Stage | What | When |
|-------|------|------|
| **1 — File-based** | Manual WSL dump script + Python ingester, native OM entity types + tags | Now (~2-3 days) |
| **2 — Airflow-orchestrated** | Replace manual script with Airflow DAG for scheduling + audit log | If Stage 1 shows value |
| **3 — Custom entities + UI** | Proper OM custom entity types with JSON schemas and UI rendering | If native types are too limiting |

---

## Stage 1 — File-Based Ingestion

### Approach

1. Developer runs `az_dump.sh` in WSL2 → JSON files written to NTFS shared folder
2. Developer runs `azure_ingester.py` + `ado_ingester.py` in WSL venv → reads JSON files → pushes to OM API
3. All entities use **native OM types** (`Pipeline`, `StorageContainer`, `Table`) with **tags and custom properties**
4. **No OpenMetadata source changes. No DB migrations. No UI changes.**

### Dump Script

`infra-devops/openmetadata/scripts/az_dump.sh`:

```bash
az login  # interactive; swap for --service-principal when automating
OUT=/mnt/e/A/infra-devops/data/az-dumps
mkdir -p $OUT

az resource list --resource-group rg-airflowdbtdatapipeline-prd -o json > $OUT/resources.json
az container show --resource-group rg-airflowdbtdatapipeline-prd --name aci-pipeline-prd -o json > $OUT/aci.json
az acr repository list --name acrpipelinedevhbwx -o json > $OUT/acr_repos.json
az acr repository show-tags --name acrpipelinedevhbwx --repository pipeline/airflow -o json > $OUT/acr_tags_airflow.json
az acr repository show-tags --name acrpipelinedevhbwx --repository pipeline/crypt -o json > $OUT/acr_tags_crypt.json
az storage blob list --account-name stpipelinedata --container-name data -o json > $OUT/blobs.json
```

ADO data uses a separate `ado_dump.py` (PAT token auth) writing to `data/ado-dumps/`.

Each file is idempotent — re-running overwrites it with current state. ADO history is append-only.

### File Structure

```
infra-devops/
└── data/
    ├── az-dumps/
    │   ├── resources.json          # all ARM resources in RG
    │   ├── aci.json                # ACI container group detail (current state)
    │   ├── acr_repos.json          # ACR repository list
    │   ├── acr_tags_airflow.json   # airflow image tags
    │   ├── acr_tags_crypt.json     # crypt image tags
    │   └── blobs.json              # storage blob listing
    └── ado-dumps/
        ├── runs.json               # ADO pipeline run history (all runs)
        └── approvals.json          # ADO approval records
```

### Entity Mapping (Native OM Types + Tags)

**Current state entities** — rebuilt on each dump run, reflect "right now":

| Source (JSON file) | OM Entity Type | OM Service | Tags / Custom Properties |
|---|---|---|---|
| `aci.json` — container group | `Pipeline` | `PipelineService: ACI-prd` | `env=prd`, `image_version=231` |
| `aci.json` — each container | `Task` (inside Pipeline) | — | `image=airflow:231`, `cpu=1` |
| `acr_tags_airflow.json` | `StorageContainer` | `StorageService: ACR` | `registry=acrpipelinedevhbwx`, `latest_tag=231` |
| `acr_tags_crypt.json` | `StorageContainer` | `StorageService: ACR` | `registry=acrpipelinedevhbwx`, `latest_tag=231` |
| `resources.json` — Key Vault | `Pipeline` | `PipelineService: Infra-prd` | `type=keyvault` |
| `blobs.json` — containers | `StorageContainer` | `StorageService: AzureBlob` (existing) | `account=stpipelinedata` |

**Historical audit entities** — append-only, one entity per ADO pipeline run:

| Source | OM Entity Type | OM Service | Tags / Custom Properties |
|---|---|---|---|
| `ado-dumps/runs.json` — build run | `Pipeline` | `PipelineService: ADO` | `run_id=232`, `image=airflow:231`, `date=2026-02-18` |
| `ado-dumps/runs.json` — deploy run | `Pipeline` | `PipelineService: ADO` | `run_id=233`, `version=231`, `env=prd` |
| `ado-dumps/approvals.json` | Custom property on deploy Pipeline | — | `approved_by=Abner Zhang`, `approved_at=2026-02-18` |

### Image Version — Current State + Audit History

Image version changes every ADO build. Two complementary views:

- **Current state**: `image_version=231` tag on the ACI Pipeline entity — visible immediately when opening the entity in OM UI
- **Audit history**: each ADO build/deploy run is its own Pipeline entity — shows full timeline of version changes with approver and date

### Lineage Edges

```python
# DevOps → Infra
Pipeline:ADO/build-#232        ──[builds]──►   StorageContainer:ACR/airflow (tag=231)
Pipeline:ADO/infra-deploy-#233 ──[deploys]──►  Pipeline:ACI-prd/aci-pipeline-prd

# Infra → Data (native Airflow connector provides this automatically)
Pipeline:ACI-prd/aci-pipeline-prd ──[runs]──►  Pipeline:Airflow/aci_sf_encrypted_pipeline

# Data lineage (native Airflow + DBT connectors)
StorageContainer:blobs/encrypted  ──[task]──►  Table:bronze.sf_members
Table:bronze.sf_members           ──[dbt]───►  Table:silver.silver_sf_members
Table:silver.silver_sf_members    ──[dbt]───►  Table:gold.dim_sf_members_scd2
```

All edges use `PUT /api/v1/lineage` with FQNs. No new OM API endpoints needed.

### Code Structure

Follows the existing OM connector pattern under `ingestion/src/metadata/ingestion/source/pipeline/`.

```
OpenMetadata/
├── ingestion/
│   ├── examples/
│   │   └── lineage-dumps/              # example/sample dump files (gitignored real data)
│   │       ├── az-dumps/
│   │       │   ├── resources.json
│   │       │   ├── aci.json
│   │       │   ├── acr_tags_airflow.json
│   │       │   ├── acr_tags_crypt.json
│   │       │   └── blobs.json
│   │       └── ado-dumps/
│   │           ├── runs.json
│   │           └── approvals.json
│   └── src/metadata/ingestion/source/pipeline/
│       ├── aci/                        # ACI + ACR ingester (current state entities)
│       │   ├── __init__.py
│       │   ├── connection.py
│       │   ├── metadata.py             # reads az-dumps/ → OM Pipeline/StorageContainer entities
│       │   ├── models.py               # Pydantic models for ACI/ACR JSON
│       │   └── service_spec.py
│       └── ado/                        # ADO ingester (audit history entities)
│           ├── __init__.py
│           ├── connection.py
│           ├── metadata.py             # reads ado-dumps/ → OM Pipeline entities
│           ├── models.py               # Pydantic models for ADO run/approval JSON
│           └── service_spec.py
└── docs/
    └── 2026-03-01-3d-lineage-ingestion-design.md  ← this file
```

**Dump scripts** (run manually in WSL before ingesting):

```
ingestion/src/metadata/ingestion/source/pipeline/
├── aci/
│   └── az_dump.sh      # az login + az cli commands → lineage-dumps/az-dumps/
└── ado/
    └── ado_dump.py     # ADO REST API (PAT auth) → lineage-dumps/ado-dumps/
```

**Stitch lineage** runs as the final step after both ingesters complete, posting edges via `PUT /api/v1/lineage`. Can be a standalone script or an additional task in the native OM Airflow DAG in Stage 2.

### No OM Source Changes

Native types + tags means:
- **No DB schema migrations** in OpenMetadata
- **No new UI pages or components**
- **No custom entity JSON schemas**
- All custom metadata stored as tags and custom properties on `Pipeline`, `StorageContainer`, `Table`

---

## Stage 2 — Airflow-Orchestrated (if Stage 1 proves value)

Replace the manual WSL script with an Airflow DAG. JSON files land on the NTFS volume mounted into the Airflow container.

```python
# airflow/dags/openmetadata_lineage_sync.py
with DAG("openmetadata_lineage_sync", schedule_interval="0 * * * *"):

    dump_azure    = BashOperator(task_id="dump_azure",    bash_command="bash /opt/scripts/az_dump.sh")
    dump_ado      = PythonOperator(task_id="dump_ado",    python_callable=run_ado_dump)
    ingest_native = PythonOperator(task_id="ingest_native", ...)   # PostgreSQL + Airflow + DBT connectors
    ingest_azure  = PythonOperator(task_id="ingest_azure", ...)    # reads az-dumps/
    ingest_ado    = PythonOperator(task_id="ingest_ado",   ...)    # reads ado-dumps/
    stitch        = PythonOperator(task_id="stitch",       ...)    # POST lineage edges

    [dump_azure, dump_ado] >> ingest_native >> [ingest_azure, ingest_ado] >> stitch
```

Adds: scheduling, retry logic, run history visible in Airflow UI, DAG-level audit trail.

---

## Stage 3 — Custom Entities + UI (if native types are too limiting)

Only if Stage 2 shows that tags/properties on `Pipeline`/`Table` are insufficient for queries or UI navigation:

- Define proper OM custom entity types (`TerraformResource`, `ACRImage`, `ACIContainerGroup`, `ADOPipelineRun`) via JSON schemas
- Add OM UI rendering for custom entity pages
- Migrate existing tag-based data to typed entity properties

The Stage 1/2 data model is forward-compatible — entity FQNs and lineage edges carry over.

---

## What You Can Query After Stage 1

**"Which version is currently deployed to PRD?"**
→ Open `Pipeline: aci-pipeline-prd` → tag `image_version=231`

**"Show all PRD deployments with approver for February audit"**
→ Filter `PipelineService: ADO` pipelines by `env=prd`, `date BETWEEN 2026-02-01 AND 2026-02-28`
→ Each result shows `approved_by`, `version`, `date`

**"Which deployment is writing to gold.dim_sf_members_scd2?"**
→ Lineage upstream from `Table:gold.dim_sf_members_scd2`
→ DBT model → Airflow DAG → ACI Pipeline (`image_version=231`) → ADO deploy run #233 (`approved_by=Abner Zhang`)

**"If silver_customers.sql changes, what breaks?"**
→ Lineage downstream from `Table:silver.silver_sf_members`
→ All downstream gold tables visible in OM lineage graph

---

## Audit Reports Replaced

| Manual Report | OpenMetadata Equivalent |
|---------------|------------------------|
| `audit-report-2026-02.md` | ADO Pipeline entities filtered by `env=prd`, `date=2026-02` |
| `dag-dbt-version-audit-2026-02.md` | ACR StorageContainer tag history + ACI Pipeline `image_version` |
| `dev-mysql-diag.txt` | ACI Pipeline entity + lineage showing MySQL task removal |
| Monthly lineage diagram | Auto-generated lineage graph in OM UI |

---

## Key Design Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Ingestion source | File-based (az CLI dump to NTFS) | Simplest start; no SDK auth complexity in Stage 1 |
| File location | `E:\A\infra-devops\data\az-dumps\` (NTFS) | Accessible from WSL + Airflow volume mount |
| Entity modeling | Native OM types + tags | Zero OM source changes; fastest to prove value |
| Image version | Tag on current ACI entity + historical ADO Pipeline entities | Both "what is now" and "what changed when" |
| Progression | Stage 1 → 2 → 3 only if value proven | YAGNI — avoid over-engineering before value is clear |

---

*Related: `docs/wsl2-native-dev-setup.md`, `E:\A\infra-devops\docs\plans\2026-02-28-openmetadata-lineage-design.md` (original design)*
