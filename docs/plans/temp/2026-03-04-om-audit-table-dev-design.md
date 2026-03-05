---
created: 2026-03-04
status: DESIGN
owner: data-platform
---

# OM Audit Table Entity (Dev) — BDD Spec

## Scope
- Dev environment only.
- `audit_log_pusher` reads `audit_log.jsonl`, ensures OM table entity exists, and sets metadata.
- Covers entity existence, schema fidelity, idempotency, metadata, row-count snapshot, freshness SLA, and failure surfacing.

## Non-Goals
- UI validation (search/preview) and lineage edges (future phase).
- UAT/PRD environments.

## Scenarios (Gherkin-ready)
1. **Entity exists after push**  
   GIVEN `audit_log.jsonl` has at least one row  
   WHEN `audit_log_pusher` completes in dev  
   THEN a Table entity `audit_db.public.daily_deployment_log` exists in OM with status "active"

2. **Schema matches JSONL**  
   GIVEN `audit_log.jsonl` schema is inferred from keys/types  
   WHEN the entity is (re)created  
   THEN OM columns equal the JSONL fields (name, dataType, nullable) and include an `ingested_at` metadata field

3. **Idempotent re-run**  
   GIVEN the entity already exists in dev  
   WHEN `audit_log_pusher` runs again on the same file  
   THEN no duplicate table is created and column definitions remain unchanged

4. **Owner and tier metadata**  
   GIVEN the pusher has owner and tier config  
   WHEN the entity is written  
   THEN `owner` = DataPlatform team and `tier` = Gold are set on the table

5. **Environment label**  
   GIVEN the target is dev  
   WHEN the entity is written  
   THEN a tag/label `env:dev` is attached to the table

6. **Row-count snapshot**  
   GIVEN the file has N rows  
   WHEN the run finishes  
   THEN the table's `tableProfile.rowCount` (or equivalent) equals N

7. **Freshness SLA**  
   GIVEN `audit_log_pusher` finished at time T  
   WHEN T + 10 minutes elapse  
   THEN the OM entity reflects that run (exists, schema + row count updated)

8. **Failure surfacing**  
   GIVEN OM write fails (API/validation)  
   WHEN the run ends  
   THEN the job exits non-zero and logs an error summarizing the OM failure

## Notes
- Table FQN is fixed: `audit_db.public.daily_deployment_log`.
- Owner/tier/env values come from configuration; default to fail-fast if missing.
- Row-count source of truth is the ingested JSONL length; row-level sample push is already covered in prior specs.

