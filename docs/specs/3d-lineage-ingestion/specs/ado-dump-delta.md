# ado-dump Delta Spec

## ADDED

### Dump ADO pipeline run history
GIVEN `ADO_PAT` env var is set with a valid PAT token
AND the ADO project contains pipeline runs
WHEN `ado_dump.py` is run
THEN `ado-dumps/runs.json` is written containing run ID, pipeline name, start time, result, and built image version for each run

### Dump ADO approval records
GIVEN `ADO_PAT` env var is set
AND pipeline runs have associated approval records
WHEN `ado_dump.py` is run
THEN `ado-dumps/approvals.json` is written containing run ID, approver display name, and approval timestamp for each approval

### Append-only history
GIVEN `ado-dumps/runs.json` already contains records from a previous run
WHEN `ado_dump.py` is run again
THEN new run records are appended and existing records are not modified or removed

### First-run full history
GIVEN `ado-dumps/runs.json` does not exist
WHEN `ado_dump.py` is run
THEN all available ADO pipeline run history is fetched and written

### Incremental sync
GIVEN `ado-dumps/runs.json` exists and contains a most-recent run timestamp
WHEN `ado_dump.py` is run
THEN only runs newer than that timestamp are fetched from the ADO API

### Fail fast on auth error
GIVEN `ADO_PAT` is missing or invalid
WHEN `ado_dump.py` is run
THEN the script exits with a non-zero code and writes no output files
