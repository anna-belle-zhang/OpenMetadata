# Audit Log - Living Spec

## Behaviors

### Append audit row after successful DAG enrichment
GIVEN a DAG entity was successfully patched by dag_enricher
WHEN the enricher completes for that DAG
THEN one row is appended to audit_log.jsonl with fields:
  audit_date, dag_fqn, run_end_timestamp, image_build_ref,
  git_sha, infra_deploy_ref (or null), approved_by (or null), env (or null)

*Added: 2026-03-03 via daily-audit-log*

### Idempotent: no duplicate rows
GIVEN a row with (audit_date, dag_fqn) already exists in audit_log.jsonl
WHEN dag_enricher runs again for the same DAG on the same date
THEN no duplicate row is written

*Added: 2026-03-03 via daily-audit-log*

### Two DAGs on same date both appended
GIVEN two DAGs both ran on the same date
WHEN dag_enricher processes both
THEN audit_log.jsonl has two rows, one per dag_fqn

*Added: 2026-03-03 via daily-audit-log*

### Audit row carries approved_by from infra entity
GIVEN the active infra AuditEntity has an approved_by field
WHEN dag_enricher appends the audit row
THEN the row's approved_by matches the infra entity's approved_by

*Added: 2026-03-03 via daily-audit-log*

### Audit row carries env from infra entity
GIVEN the active infra AuditEntity has an env field
WHEN dag_enricher appends the audit row
THEN the row's env matches the infra entity's env

*Added: 2026-03-03 via daily-audit-log*

### approved_by and env are null when no infra entity
GIVEN no infra entity is active at DAG run time
WHEN dag_enricher appends the audit row
THEN approved_by and env are null in the row

*Added: 2026-03-03 via daily-audit-log*

### Audit table entity created in OM
GIVEN audit_log.jsonl has rows
WHEN audit_log_pusher runs
THEN a Table entity "audit_db.public.daily_deployment_log" exists in OM
  with columns matching the jsonl schema

*Added: 2026-03-03 via daily-audit-log*

### Audit table carries owner, tier, env
GIVEN audit_log_pusher runs in dev
WHEN it creates/updates the audit table entity
THEN owner=DataPlatform, tier=Gold, and tag env:dev are set on the table

*Added: 2026-03-04 via om-audit-table-dev*

### Row count snapshot recorded
GIVEN audit_log.jsonl has N rows
WHEN audit_log_pusher completes
THEN the table profile rowCount equals N

*Added: 2026-03-04 via om-audit-table-dev*

### Freshness within 10 minutes
GIVEN audit_log_pusher finished at time T
WHEN T + 10 minutes elapse
THEN the OM entity reflects that run (exists, schema and row count updated)

*Added: 2026-03-04 via om-audit-table-dev*

### OM failures surface as job failures
GIVEN an OM API write fails during audit_log_pusher
WHEN the run ends
THEN the job exits non-zero and logs the OM failure

*Added: 2026-03-04 via om-audit-table-dev*

### Rows pushed as sample data
GIVEN the audit table entity exists
AND audit_log.jsonl has N rows
WHEN audit_log_pusher pushes sample data
THEN OM TableData endpoint is called with all N rows

*Added: 2026-03-03 via daily-audit-log*

### Lineage from audit table to gold table
GIVEN audit table entity exists in OM
AND gold_table_fqns is configured with at least one FQN
WHEN audit_log_pusher posts lineage
THEN one lineage edge exists from audit table to each gold table FQN

*Added: 2026-03-03 via daily-audit-log*

### Pusher is idempotent
GIVEN audit_log_pusher has already run
WHEN it runs again with the same audit_log.jsonl
THEN get_or_create_table and push_table_data are called with the same arguments
  (no errors, no duplicate entities)

*Added: 2026-03-03 via daily-audit-log*
