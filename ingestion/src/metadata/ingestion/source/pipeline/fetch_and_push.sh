#!/usr/bin/env bash
# fetch_and_push.sh — pull fresh ADO + Airflow data, then seed and push to OM.
#
# Required env vars:
#   ADO_PAT         Azure DevOps personal access token
#   ADO_ORG_URL     e.g. https://dev.azure.com/myorg
#   ADO_PROJECT     e.g. my-project
#
# Optional env vars (defaults shown):
#   AIRFLOW_HOST    http://pipeline-prd-9tto.australiaeast.azurecontainer.io:8080
#   AIRFLOW_USER    admin
#   AIRFLOW_PASS    fetched from Key Vault (kv-pipeline-9tto) if unset
#   KV_NAME         kv-pipeline-9tto
#   DUMPS_DIR       <repo>/ingestion/examples/lineage-dumps/ado-dumps
#   AUDIT_LOG_PATH  <repo>/ingestion/audit_log.jsonl
#   GOLD_TABLE_FQNS postgres_pipeline.warehouse.public.dim_sf_members_scd2
#   OM_HOST         http://localhost:8585
#   OM_EMAIL        admin@open-metadata.org
#   OM_PASSWORD_B64 YWRtaW4=  (base64 of "admin")
#
# Usage:
#   export ADO_PAT=... ADO_ORG_URL=... ADO_PROJECT=...
#   bash fetch_and_push.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../../.." && pwd)"

DUMPS_DIR="${DUMPS_DIR:-$REPO_ROOT/ingestion/examples/lineage-dumps/ado-dumps}"
AUDIT_LOG_PATH="${AUDIT_LOG_PATH:-$REPO_ROOT/ingestion/audit_log.jsonl}"
GOLD_TABLE_FQNS="${GOLD_TABLE_FQNS:-postgres_pipeline.warehouse.public.dim_sf_members_scd2}"

AIRFLOW_HOST="${AIRFLOW_HOST:-http://pipeline-prd-9tto.australiaeast.azurecontainer.io:8080}"
AIRFLOW_USER="${AIRFLOW_USER:-admin}"
KV_NAME="${KV_NAME:-kv-pipeline-9tto}"

die() { echo "ERROR: $*" >&2; exit 1; }
banner() { echo ""; echo "══════════════════════════════════════════════════════"; echo "  $*"; echo "══════════════════════════════════════════════════════"; }

# ── 1. ADO dump ────────────────────────────────────────────────────────────────
banner "Step 1 — ADO runs + approvals"

[[ -n "${ADO_PAT:-}" ]]     || die "ADO_PAT is required"
[[ -n "${ADO_ORG_URL:-}" ]] || die "ADO_ORG_URL is required"
[[ -n "${ADO_PROJECT:-}" ]] || die "ADO_PROJECT is required"

mkdir -p "$DUMPS_DIR"

ADO_DUMPS_DIR="$DUMPS_DIR" \
ADO_ORG_URL="$ADO_ORG_URL" \
ADO_PROJECT="$ADO_PROJECT" \
ADO_PAT="$ADO_PAT" \
  python3 "$SCRIPT_DIR/ado/ado_dump.py"

echo "  runs.json      → $(python3 -c "import json; d=json.load(open('$DUMPS_DIR/runs.json')); print(len(d), 'runs')")"
echo "  approvals.json → $(python3 -c "import json; d=json.load(open('$DUMPS_DIR/approvals.json')); print(len(d), 'approvals')")"

# ── 2. Airflow DAG last run end_dates ──────────────────────────────────────────
banner "Step 2 — Airflow DAG last run end_dates"

if [[ -z "${AIRFLOW_PASS:-}" ]]; then
  echo "  Fetching Airflow password from Key Vault: $KV_NAME …"
  AIRFLOW_PASS="$(az keyvault secret show \
    --name airflow-admin-password \
    --vault-name "$KV_NAME" \
    --query value -o tsv)"
fi

AIRFLOW_RUNS_FILE="$DUMPS_DIR/airflow_dag_runs.json"

echo "  Connecting to $AIRFLOW_HOST …"
DAGS_RESPONSE="$(curl -sf --max-time 15 \
  -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
  "$AIRFLOW_HOST/api/v1/dags?limit=100" )"

DAG_IDS="$(echo "$DAGS_RESPONSE" | python3 -c "
import json, sys
for d in json.load(sys.stdin).get('dags', []):
    if d['dag_id'].startswith('aci_'):
        print(d['dag_id'])
")"

if [[ -z "$DAG_IDS" ]]; then
  echo "  WARNING: no aci_* DAGs found — skipping Airflow dump"
else
  echo "  Found DAGs: $(echo "$DAG_IDS" | tr '\n' ' ')"
  echo "[]" > "$AIRFLOW_RUNS_FILE"

  while IFS= read -r dag_id; do
    [[ -z "$dag_id" ]] && continue
    run_json="$(curl -sf --max-time 10 \
      -u "$AIRFLOW_USER:$AIRFLOW_PASS" \
      "$AIRFLOW_HOST/api/v1/dags/$dag_id/dagRuns?limit=1&order_by=-execution_date" \
      2>/dev/null || echo '{}')"
    end_date="$(echo "$run_json" | python3 -c "
import json, sys
runs = json.load(sys.stdin).get('dag_runs', [])
print(runs[0].get('end_date') or runs[0].get('execution_date') or '' if runs else '')
" 2>/dev/null || true)"
    if [[ -n "$end_date" && "$end_date" != "None" ]]; then
      python3 - "$AIRFLOW_RUNS_FILE" "$dag_id" "$end_date" <<'PYEOF'
import json, sys
path, dag_id, end_date = sys.argv[1], sys.argv[2], sys.argv[3]
data = json.loads(open(path).read())
data.append({"dag_id": dag_id, "end_date": end_date})
open(path, "w").write(json.dumps(data, indent=2))
PYEOF
      echo "    $dag_id → $end_date"
    else
      echo "    $dag_id → (no completed run)"
    fi
  done <<< "$DAG_IDS"

  count="$(python3 -c "import json; print(len(json.load(open('$AIRFLOW_RUNS_FILE'))))")"
  echo "  Written $count DAG run records → $AIRFLOW_RUNS_FILE"
fi

# ── 3. Seed audit log + push to OM ────────────────────────────────────────────
banner "Step 3 — Seed audit_log.jsonl + push to OpenMetadata"

cd "$REPO_ROOT/ingestion"

extra_args=()
[[ -n "${OM_HOST:-}" ]]         && extra_args+=(--host "$OM_HOST")
[[ -n "${OM_EMAIL:-}" ]]        && extra_args+=(--email "$OM_EMAIL")
[[ -n "${OM_PASSWORD_B64:-}" ]] && extra_args+=(--password-b64 "$OM_PASSWORD_B64")

python3 src/metadata/ingestion/source/pipeline/smoke_ingest.py \
  --audit-log-path "$AUDIT_LOG_PATH" \
  --gold-table-fqns "$GOLD_TABLE_FQNS" \
  --seed-audit \
  "${extra_args[@]}"

banner "Done"
echo "  Audit rows live at: audit_db.audit_db.public.daily_deployment_log"
echo "  Open: ${OM_HOST:-http://localhost:8585}"
