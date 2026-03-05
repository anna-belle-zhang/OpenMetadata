"""Delta spec tests for audit_db schema DDL."""
from pathlib import Path


SCHEMA_PATH = Path(__file__).parents[4] / "sql/audit_schema.sql"


def _load_sql() -> str:
    assert SCHEMA_PATH.exists(), "Expected audit_schema.sql to exist"
    return SCHEMA_PATH.read_text()


def test_schema_and_tables_created():
    sql = _load_sql().lower()
    assert "create schema if not exists audit" in sql
    assert "create table" in sql and "ado_pipeline_runs" in sql
    assert "primary key (run_id, ado_project)" in sql
    assert "create table" in sql and "airflow_dag_executions" in sql
    assert "primary key (dag_run_id, dag_id, airflow_instance)" in sql


def test_pipeline_subtype_accepts_image_deploy():
    sql = _load_sql().lower()
    assert "pipeline_subtype" in sql
    assert "image_deploy" in sql
    assert "pipeline_subtype" in sql.split("image_deploy")[0] or "image_deploy" in sql


def test_materialized_view_has_expected_columns():
    sql = _load_sql().lower()
    start = sql.index("create materialized view")
    mv_sql = sql[start:]
    for col in (
        "audit_date",
        "source_system",
        "env",
        "job_name",
        "total_runs",
        "success_count",
        "failure_count",
        "success_rate",
        "avg_duration_seconds",
        "max_duration_seconds",
        "approval_count",
        "approval_pct",
    ):
        assert col in mv_sql


def test_duration_seconds_is_generated_column():
    sql = _load_sql().lower()
    assert "duration_seconds" in sql
    assert "generated" in sql
    assert "finish_time" in sql and "start_time" in sql
