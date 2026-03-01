# ACI Connector Delta Spec

## ADDED

### Create ACI PipelineService
GIVEN `az-dumps/aci.json` exists and is valid
AND OpenMetadata is running and accessible
WHEN the ACI connector runs
THEN a `PipelineService` named `ACI-prd` exists in OpenMetadata

### Create ACI Pipeline entity with current image version tag
GIVEN `az-dumps/aci.json` contains a container group with a running image tag
WHEN the ACI connector runs
THEN a `Pipeline` entity named `aci-pipeline-prd` exists under `PipelineService: ACI-prd`
AND the entity has a tag `image_version=<current tag>` (e.g. `231`)
AND the entity has a tag `env=prd`

### Create Task per container
GIVEN `az-dumps/aci.json` contains multiple containers in the group
WHEN the ACI connector runs
THEN a `Task` entity exists for each container
AND each Task has tags `image=<repo>:<tag>` and `cpu=<value>`

### Create ACR StorageService
GIVEN `az-dumps/acr_tags_airflow.json` exists
WHEN the ACI connector runs
THEN a `StorageService` named `ACR` exists in OpenMetadata

### Create ACR StorageContainer per repository
GIVEN `az-dumps/acr_tags_airflow.json` contains image tags
WHEN the ACI connector runs
THEN a `StorageContainer` named `airflow` exists under `StorageService: ACR`
AND the container has tag `latest_tag=<most recent tag>`
AND a `StorageContainer` named `crypt` exists with its own `latest_tag`

### Idempotent re-run
GIVEN ACI and ACR entities already exist in OpenMetadata from a previous run
WHEN the ACI connector runs again with updated dump files
THEN existing entities are updated (not duplicated)
AND the `image_version` tag reflects the current value from the dump file

### Skip on malformed dump file
GIVEN `az-dumps/aci.json` exists but contains invalid JSON
WHEN the ACI connector runs
THEN the connector logs an error for the malformed file
AND continues processing other valid files
AND exits with a non-zero code summarising skipped entities
