# AZ Dump - Living Spec

## Behaviors

### Dump ACI container group state
GIVEN `az login` has been completed in the WSL session
AND the ACI container group `aci-pipeline-prd` exists in the resource group
WHEN `az_dump.sh` is run
THEN `az-dumps/aci.json` is written containing the container group name, container names, image tags, and CPU/memory allocations

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Dump ACR image tags
GIVEN `az login` has been completed
AND the ACR `acrpipelinedevhbwx` contains repositories `pipeline/airflow` and `pipeline/crypt`
WHEN `az_dump.sh` is run
THEN `az-dumps/acr_tags_airflow.json` and `az-dumps/acr_tags_crypt.json` are written containing all available image tags

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Dump ARM resource list
GIVEN `az login` has been completed
WHEN `az_dump.sh` is run
THEN `az-dumps/resources.json` is written containing all resources in `rg-airflowdbtdatapipeline-prd`

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Dump Azure Blob container listing
GIVEN `az login` has been completed
AND the storage account `stpipelinedata` is accessible
WHEN `az_dump.sh` is run
THEN `az-dumps/blobs.json` is written containing blob container names

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Idempotent re-run
GIVEN `az-dumps/` already contains files from a previous run
WHEN `az_dump.sh` is run again
THEN each output file is overwritten with the current Azure state (no duplicates, no merged content)

*Added: 2026-03-03 via 3d-lineage-ingestion*

### Fail fast on az error
GIVEN `az login` has not been completed or the resource group does not exist
WHEN `az_dump.sh` is run
THEN the script exits with a non-zero code and writes no partial output files

*Added: 2026-03-03 via 3d-lineage-ingestion*
