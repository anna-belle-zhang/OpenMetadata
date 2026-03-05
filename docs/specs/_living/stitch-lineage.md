# Stitch Lineage - Living Spec

## Behaviors

### Link ADO build run to ACR image
GIVEN a `Pipeline: ADO/build-#{n}` entity exists with property `image=airflow:231`
AND a `StorageContainer: ACR/airflow` entity exists with tag `latest_tag=231`
WHEN `stitch_lineage.py` runs
THEN a lineage edge exists from `Pipeline: ADO/build-#{n}` to `StorageContainer: ACR/airflow` with description `builds`

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Link ADO deploy run to ACI container group
GIVEN a `Pipeline: ADO/deploy-#{n}` entity exists with property `version=231`
AND a `Pipeline: ACI-prd/aci-pipeline-prd` entity exists with tag `image_version=231`
WHEN `stitch_lineage.py` runs
THEN a lineage edge exists from `Pipeline: ADO/deploy-#{n}` to `Pipeline: ACI-prd/aci-pipeline-prd` with description `deploys`

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Link ACI container group to Airflow DAG
GIVEN a `Pipeline: ACI-prd/aci-pipeline-prd` entity exists
AND a `Pipeline: Airflow/aci_sf_encrypted_pipeline` entity exists (created by native Airflow connector)
WHEN `stitch_lineage.py` runs
THEN a lineage edge exists from `Pipeline: ACI-prd/aci-pipeline-prd` to `Pipeline: Airflow/aci_sf_encrypted_pipeline` with description `runs`

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Skip edge when referenced entity does not exist
GIVEN a lineage edge references an entity FQN that does not exist in OpenMetadata
WHEN `stitch_lineage.py` runs
THEN that edge is skipped
AND a warning is logged with the missing FQN
AND other edges are still posted

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Idempotent re-run
GIVEN lineage edges already exist in OpenMetadata from a previous run
WHEN `stitch_lineage.py` runs again
THEN no duplicate edges are created
AND existing edges are unchanged

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Full cross-dimensional path is traversable
GIVEN all three connectors have run and stitch has completed
WHEN a user navigates upstream lineage from `Table: gold.dim_sf_members_scd2` in the OM UI
THEN the lineage graph includes: DBT model → Airflow DAG → ACI Pipeline → ADO deploy run → ADO build run → ACR image

*Added: 2026-03-03 via 3d-lineage-ingestion*
