# Audit Report Delta Spec

## ADDED

### Report written to disk
GIVEN `daily_audit_summary` has rows for yesterday's date
WHEN `audit_daily_report.py` runs
THEN `reports/audit-YYYY-MM-DD.md` is created on disk
AND the file contains all five sections: executive summary, top N failures, longest runs, missing approvals, counts by pipeline/dag

### Report uses yesterday by default
GIVEN `REPORT_DATE` is not set
WHEN the report runs
THEN it queries `daily_audit_summary` for `audit_date = today - 1 day`

### Missing approvals section flags prd deploys
GIVEN `daily_audit_summary` has a prd deploy row with `approval_pct < 100%`
WHEN the report renders
THEN that row appears in the "Missing Approvals" section

### Empty summary produces report with zeros
GIVEN `daily_audit_summary` has no rows for the requested date
WHEN the report runs
THEN the report file is still created
AND all metric fields show zero or empty
