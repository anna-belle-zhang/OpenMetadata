# Audit DB Schema Delta Spec

## ADDED

### Schema and tables created
GIVEN a Postgres instance with no `audit_db.audit` schema
WHEN the DDL migration runs
THEN schema `audit_db.audit` exists
AND table `ado_pipeline_runs` exists with PK `(run_id, ado_project)`
AND table `airflow_dag_executions` exists with PK `(dag_run_id, dag_id, airflow_instance)`

### pipeline_subtype accepts image_deploy
GIVEN `ado_pipeline_runs` table exists
WHEN a row is inserted with `pipeline_subtype = 'image_deploy'`
THEN the row is accepted without constraint violation

### Materialized view exists
GIVEN both base tables exist
WHEN `REFRESH CONCURRENTLY audit.daily_audit_summary` runs
THEN the view contains one row per `(audit_date, source_system, env, job_name)` group
AND each row has: total_runs, success_count, failure_count, success_rate, avg_duration_seconds, max_duration_seconds, approval_count, approval_pct

### Duration is generated
GIVEN a row in `ado_pipeline_runs` with `start_time` and `finish_time`
WHEN the row is read
THEN `duration_seconds` equals the difference in seconds (generated column, no manual insert needed)
