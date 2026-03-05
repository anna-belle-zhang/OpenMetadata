-- Audit DB schema and materialized view for audit_postgres_pipeline
-- Apply with: psql -f ingestion/sql/audit_schema.sql

CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.ado_pipeline_runs (
    run_id BIGINT NOT NULL,
    ado_project TEXT NOT NULL,
    pipeline_type TEXT NOT NULL CHECK (pipeline_type IN ('build', 'deploy')),
    pipeline_subtype TEXT NOT NULL CHECK (pipeline_subtype IN ('image_build', 'image_deploy', 'infra_deploy')),
    queue_time TIMESTAMPTZ,
    start_time TIMESTAMPTZ,
    finish_time TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (finish_time - start_time))) STORED,
    result TEXT,
    env TEXT,
    version TEXT,
    image_tag TEXT,
    git_sha TEXT,
    approved_by TEXT,
    raw_data JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingested_by TEXT NOT NULL,
    PRIMARY KEY (run_id, ado_project)
);

CREATE TABLE IF NOT EXISTS audit.airflow_dag_executions (
    dag_run_id TEXT NOT NULL,
    dag_id TEXT NOT NULL,
    airflow_instance TEXT NOT NULL,
    execution_date TIMESTAMPTZ,
    state TEXT,
    end_date TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION,
    raw_data JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingested_by TEXT NOT NULL,
    PRIMARY KEY (dag_run_id, dag_id, airflow_instance)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS audit.daily_audit_summary AS
WITH base AS (
    SELECT
        (start_time AT TIME ZONE 'UTC')::date AS audit_date,
        'ADO'::text AS source_system,
        COALESCE(env, 'unknown') AS env,
        COALESCE(raw_data->>'name', pipeline_subtype) AS job_name,
        result AS run_state,
        duration_seconds,
        (approved_by IS NOT NULL) AS has_approval
    FROM audit.ado_pipeline_runs
    UNION ALL
    SELECT
        (execution_date AT TIME ZONE 'UTC')::date AS audit_date,
        'Airflow'::text AS source_system,
        'unknown'::text AS env,
        dag_id AS job_name,
        state AS run_state,
        duration_seconds,
        FALSE AS has_approval
    FROM audit.airflow_dag_executions
)
SELECT
    audit_date,
    source_system,
    env,
    job_name,
    COUNT(*) AS total_runs,
    COUNT(*) FILTER (WHERE run_state IN ('succeeded', 'success')) AS success_count,
    COUNT(*) FILTER (WHERE run_state IN ('failed', 'error')) AS failure_count,
    CASE WHEN COUNT(*) > 0
        THEN ROUND((COUNT(*) FILTER (WHERE run_state IN ('succeeded', 'success')) * 100.0) / COUNT(*), 2)
        ELSE 0
    END AS success_rate,
    ROUND(AVG(duration_seconds)::numeric, 2) AS avg_duration_seconds,
    MAX(duration_seconds) AS max_duration_seconds,
    COUNT(*) FILTER (WHERE has_approval) AS approval_count,
    CASE WHEN COUNT(*) > 0
        THEN ROUND((COUNT(*) FILTER (WHERE has_approval) * 100.0) / COUNT(*), 2)
        ELSE 0
    END AS approval_pct
FROM base
GROUP BY audit_date, source_system, env, job_name
WITH NO DATA;

-- Refresh concurrently after loading tables:
-- REFRESH MATERIALIZED VIEW CONCURRENTLY audit.daily_audit_summary;
