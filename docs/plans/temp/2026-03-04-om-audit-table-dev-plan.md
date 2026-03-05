# OM Audit Table Entity (Dev) Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ensure `audit_log_pusher` in dev creates/updates OM table `audit_db.public.daily_deployment_log` with correct schema, metadata (owner/tier/env), row-count snapshot, idempotency, freshness (<10 min), and clear failures.

**Architecture:** Reuse existing `AuditLogPusher` to read `audit_log.jsonl`, create the OM table, push rows, and post lineage. Extend it to pass column definitions with types, attach tags/owner/tier, set table profile row count, and propagate OM errors. Unit tests mock `metadata` client to verify calls.

**Tech Stack:** Python, OpenMetadata ingestion library, pytest.

---

### Task 1: Add metadata configuration to AuditLogPusher

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/audit_log_pusher.py`
- Modify: `ingestion/tests/unit/topology/pipeline/test_audit_log_pusher.py`

**Step 1: Define optional params**
- Add params `owner`, `tier`, and `env_label` (default `None`) to `AuditLogPusher.__init__`.

**Step 2: Validate presence**
- Fail fast (`ValueError`) in `run` if any of the required metadata values is missing (owner, tier, env_label) for dev.

**Step 3: Pass to metadata client**
- Update `get_or_create_table` call to include `owner`, `tags` (tier + env_label), and schema columns (with data types) in kwargs.

**Step 4: Update unit tests**
- Add fixture values for owner/tier/env_label and assert they are passed to `get_or_create_table` (inspect call kwargs).

---

### Task 2: Ensure schema fidelity and ingested_at field

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/audit_log_pusher.py`
- Modify: `ingestion/tests/unit/topology/pipeline/test_audit_log_pusher.py`

**Step 1: Build column definitions**
- Derive columns from JSON keys with inferred OM data types; append synthetic `ingested_at` TIMESTAMP column.

**Step 2: Pass columns to metadata client**
- Update `AUDIT_COLUMNS` to hold `(name, type, nullable)` tuples or a small dataclass; ensure `get_or_create_table` receives full column list.

**Step 3: Test schema match**
- Add test: when pusher runs, `get_or_create_table` gets columns matching JSON keys plus `ingested_at`.

---

### Task 3: Row-count snapshot and idempotency

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/audit_log_pusher.py`
- Modify: `ingestion/tests/unit/topology/pipeline/test_audit_log_pusher.py`

**Step 1: Compute N**
- Count rows read from JSONL; store in local variable.

**Step 2: Push profile**
- After `push_table_data`, call `metadata.update_table_profile(table.id, row_count=N)` (or equivalent API stub). If method absent, add a wrapper in mock and production path.

**Step 3: Preserve idempotency**
- Ensure repeated runs reuse same table and do not alter column definitions; add assertions in tests comparing call args across runs.

**Step 4: Add tests**
- Test row count update is called with N.
- Extend existing idempotency test to assert column args unchanged across runs.

---

### Task 4: Freshness SLA instrumentation

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/audit_log_pusher.py`
- Modify: `ingestion/tests/unit/topology/pipeline/test_audit_log_pusher.py`

**Step 1: Capture run timestamp**
- Record `run_completed_at = datetime.utcnow()` at end of processing.

**Step 2: Set updatedAt**
- When calling `get_or_create_table`, pass `last_updated=run_completed_at` (or `updated_at`) so OM reflects freshness immediately.

**Step 3: Add SLA test**
- Mock `datetime` to fixed value; assert `last_updated` passed equals mocked time (ensuring it can satisfy the 10-minute freshness window when scheduled appropriately).

---

### Task 5: Failure surfacing

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/audit_log_pusher.py`
- Modify: `ingestion/tests/unit/topology/pipeline/test_audit_log_pusher.py`

**Step 1: Propagate exceptions**
- Wrap OM calls in try/except; log error and re-raise to ensure non-zero exit for scheduler.

**Step 2: Add test**
- Mock `push_table_data` to raise; assert `run` raises and logs once.

---

### Task 6: Wiring and docs

**Files:**
- Modify: `docs/specs/_living/audit-log.md`
- Modify: `docs/plans/2026-03-04-om-audit-table-dev-design.md`

**Step 1: Add scenarios**
- Append new BDD scenarios (owner/tier/env, row count, freshness, failures) to the living spec.

**Step 2: Update design notes**
- Ensure design doc references row-count and SLA behaviors implemented.

---

### Task 7: Test execution

**Files:**
- Tests only.

**Step 1: Run unit tests**
- Command: `pytest ingestion/tests/unit/topology/pipeline/test_audit_log_pusher.py -q`

**Step 2: Fix & rerun**
- Iterate until green.

**Step 3: (Optional) lint**
- Command: `python -m compileall ingestion/src/metadata/ingestion/source/pipeline/audit_log_pusher.py`

---

### Task 8: Commit guidance

**Step 1: Stage changes**
- `git add ingestion/src/metadata/ingestion/source/pipeline/audit_log_pusher.py`
- `git add ingestion/tests/unit/topology/pipeline/test_audit_log_pusher.py`
- `git add docs/specs/_living/audit-log.md docs/plans/2026-03-04-om-audit-table-dev-design.md`

**Step 2: Commit message**
- `git commit -m "feat: harden audit log pusher metadata for dev"`

---

**Execution choice:**
- Option 1: Subagent-driven in this session (use superpowers:subagent-driven-development per task).
- Option 2: Parallel session using executing-plans skill.

