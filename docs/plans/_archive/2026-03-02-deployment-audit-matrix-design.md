# Deployment Audit Matrix Design

**Created:** 2026-03-02
**Status:** Design — Approved
**Scope:** Stage 1.5 — extends 3D lineage ingestion with compliance audit matrix and Delta time-travel index

---

## Problem

Stage 1 lineage graph answers "what is currently deployed." It does not answer:

- "Which dbt code was running on 2026-02-18?"
- "Who approved the deployment that produced today's gold data?"
- "What was the exact gold snapshot before the Feb 18 deploy?"
- "When did the infra change (MySQL → PostgreSQL) relative to gold table versions?"

These questions span three systems — ADO run history, Airflow DAG runs, Delta Lake — with no join key between them.

---

## Solution

OM becomes the **join index** across all three systems.

For any given day, OM stores the join keys that link:
- which ADO image build was active (code version + git SHA)
- which ADO infra deploy was active (approved by whom)
- when the Airflow DAG completed (the exact `AS OF` timestamp for Delta queries)

Querying gold data for a specific day becomes two steps:
1. Open the DAG run entity in OM → read `run_end_timestamp`
2. `SELECT * FROM gold.dim_sf_members AS OF TIMESTAMP '2026-02-18T14:32:00Z'`

OM holds the metadata. Delta holds the data. No duplication.

---

## Context

- **One ADO repo** — dbt models, Airflow DAGs, Dockerfile, Terraform, ADO pipeline definitions all live in a single repo. One git SHA covers everything at a point in time.
- **Two ADO pipeline types** — image build (1–2×/month) and infra deploy (infrequent). Both reference the same repo; SHAs are independent per run.
- **Gold data is Delta Lake** — supports `AS OF TIMESTAMP` time travel natively.
- **Daily Airflow runs** — one DAG run per day writes new Delta versions on gold tables.

---

## Full Lineage Graph

```
ADO repo @ abc123
    │
    ├──[image build]──► ADO.build-232
    │                       │  git_sha=abc123
    │                       │  pipeline_type=image_build
    │                       │  image_tag=airflow:231
    │                       │
    │                   ACR.airflow:231
    │                       │
    │                   ADO.infra-deploy-233
    │                       │  git_sha=abc123
    │                       │  pipeline_type=infra_deploy
    │                       │  approved_by=Abner Zhang
    │                       │  env=prd
    │                       │
    │                   ACI-prd.aci-pipeline-prd
    │                       │  image_version=231
    │                       │
    │                   Airflow DAG: aci_sf_encrypted_pipeline
    │                       │  image_build_ref=ADO.build-232
    │                       │  infra_deploy_ref=ADO.infra-deploy-233
    │                       │  git_sha=abc123
    │                       │  run_end_timestamp=2026-02-18T14:32:00Z
    │                       │
    └──[dbt @ abc123]──► silver.silver_sf_members
                               │
                           gold.dim_sf_members_scd2
                               └── AS OF '2026-02-18T14:32:00Z' ✓
```

---

## Audit Matrix

For any date D, OM answers all compliance questions:

| Question | Source | Field |
|----------|--------|-------|
| **Who** deployed | ADO.infra-deploy-N | `approved_by` |
| **What** was deployed | ADO.build-N | `image_tag`, `git_sha` |
| **When** it was deployed | ADO.infra-deploy-N | `deploy_date` |
| **Where** it was deployed | ADO.infra-deploy-N | `env=prd` |
| **Why** it was approved | ADO.infra-deploy-N | `approval_record` |
| **Which data** it produced | Airflow DAG run | `run_end_timestamp` → Delta `AS OF` |
| **Which code** ran | Airflow DAG run | `git_sha` → ADO repo @ that commit |

---

## What Needs to Be Built

Three targeted additions to Stage 1. No new OM entity types. No DB migrations. No UI changes.

### 1. Enhance ADO dump + models (small)

Add two fields to `AdoRun`:

```python
# ado/models.py
class AdoRun(BaseModel):
    # existing fields ...
    git_sha: str                                              # ADO: run.resources.repositories.self.version
    pipeline_type: Literal["image_build", "infra_deploy"]    # by pipeline definition name
```

`ado_dump.py` extracts `sourceVersion` (git SHA) and classifies `pipeline_type` by matching the pipeline definition name against configured names.

### 2. Enhance ADO connector (small)

Store the two new fields as custom properties on each ADO Pipeline entity:

```python
# ado/metadata.py
custom_properties = {
    "git_sha":        run.git_sha,
    "pipeline_type":  run.pipeline_type,
    # existing: run_id, image_tag, env, approved_by, date ...
}
```

### 3. New: DAG enricher

New script: `dag_enricher.py`, alongside `stitch_lineage.py`.

Runs after Airflow connector + ADO connector both complete.

```python
# dag_enricher.py (pseudocode)

builds  = om.list_pipelines(service="ADO", filter=pipeline_type="image_build")
infras  = om.list_pipelines(service="ADO", filter=pipeline_type="infra_deploy")

for dag in om.list_pipelines(service="airflow_pipeline"):
    run_date = dag.pipeline_status.endDate
    if not run_date:
        continue  # never ran

    active_build = max(b for b in builds if b.date <= run_date, default=None)
    active_infra = max(i for i in infras  if i.date <= run_date, default=None)

    if not active_build:
        log.warning(f"No image build found before {run_date} for {dag.fqn}")
        continue

    om.patch_custom_properties(dag.fqn, {
        "image_build_ref":   active_build.fqn,
        "infra_deploy_ref":  active_infra.fqn if active_infra else None,
        "git_sha":           active_build.git_sha,
        "run_end_timestamp": run_date,
    })
```

---

## Run Order

```
ado_dump.py
az_dump.sh
    │
    ▼
ado_connector      (creates ADO Pipeline entities with git_sha, pipeline_type)
aci_connector      (creates ACI Pipeline, ACR StorageContainer entities)
airflow_connector  (creates Airflow DAG entities with PipelineStatus)
    │
    ▼
stitch_lineage     (posts cross-dimensional lineage edges)
dag_enricher       (patches DAG entities with image_build_ref, infra_deploy_ref, git_sha, run_end_timestamp)
```

---

## Example Queries After Implementation

**"What code ran on 2026-02-18?"**
→ Airflow DAG entity for that date → `git_sha=abc123` → browse ADO repo at that commit

**"Who approved the code writing to gold today?"**
→ Airflow DAG entity → `infra_deploy_ref=ADO.infra-deploy-233` → `approved_by=Abner Zhang`

**"What was the gold snapshot before the Feb 18 deploy?"**
→ Find infra deploy before Feb 18 → find preceding DAG run → `run_end_timestamp=2026-02-17T14:28:00Z`
→ `SELECT * FROM gold.dim_sf_members AS OF TIMESTAMP '2026-02-17T14:28:00Z'`

**"Did the MySQL → PostgreSQL infra change affect gold data?"**
→ Find infra deploy date → find DAG runs before/after → compare `run_end_timestamp` values
→ Query Delta `AS OF <before>` vs `AS OF <after>`

---

*Specs: `docs/specs/3d-lineage-ingestion/`*
*Builds on: `docs/2026-03-01-3d-lineage-ingestion-design.md`*
