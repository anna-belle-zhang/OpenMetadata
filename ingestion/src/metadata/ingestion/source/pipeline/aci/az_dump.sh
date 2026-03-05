#!/usr/bin/env bash
# az_dump.sh — dump Azure resource state to JSON files for lineage ingestion
# Usage: bash az_dump.sh [output_dir]
#
# Prerequisites:
#   - az login completed in the current shell
#   - Env vars: AZURE_RG, AZURE_ACR, AZURE_ACI_NAME, AZURE_STORAGE_ACCOUNT
#
# Defaults match the prd environment.

set -euo pipefail

OUT="${1:-$(pwd)/ingestion/examples/lineage-dumps/az-dumps}"
RG="${AZURE_RG:-rg-airflowdbtdatapipeline-prd}"
ACR="${AZURE_ACR:-acrpipelinedevhbwx}"
ACI_NAME="${AZURE_ACI_NAME:-aci-pipeline-prd}"
STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-stpipelinedata}"

mkdir -p "$OUT"

echo "Dumping ACI container group: $ACI_NAME"
az container show \
  --resource-group "$RG" \
  --name "$ACI_NAME" \
  --query "{name:name, resourceGroup:resourceGroup, location:location, containers:containers[].{name:name, image:image, resources:resources}}" \
  -o json > "$OUT/aci.json"

echo "Dumping ACR tags: $ACR/pipeline/airflow"
az acr repository show-tags \
  --name "$ACR" \
  --repository pipeline/airflow \
  -o json > "$OUT/acr_tags_airflow.json"

echo "Dumping ACR tags: $ACR/pipeline/crypt"
az acr repository show-tags \
  --name "$ACR" \
  --repository pipeline/crypt \
  -o json > "$OUT/acr_tags_crypt.json"

echo "Dumping ARM resource list"
az resource list \
  --resource-group "$RG" \
  --query "[].{name:name, type:type, resourceGroup:resourceGroup}" \
  -o json > "$OUT/resources.json"

echo "Dumping Blob containers: $STORAGE_ACCOUNT"
az storage container list \
  --account-name "$STORAGE_ACCOUNT" \
  --auth-mode login \
  --query "[].{name:name}" \
  -o json > "$OUT/blobs.json"

echo "Done. Files written to: $OUT"
ls -lh "$OUT"
