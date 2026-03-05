"""Delta spec tests for audit daily report generation."""
from datetime import datetime, timezone
from pathlib import Path

from metadata.ingestion.source.pipeline.ado import audit_postgres_pipeline as dag_mod


def _row(**kwargs) -> dict:
    base = {
        "audit_date": "2026-03-04",
        "source_system": "ADO",
        "env": "dev",
        "job_name": "build-image",
        "total_runs": 3,
        "success_count": 2,
        "failure_count": 1,
        "success_rate": 66.6,
        "avg_duration_seconds": 120.0,
        "max_duration_seconds": 300.0,
        "approval_count": 2,
        "approval_pct": 66.6,
    }
    base.update(kwargs)
    return base


def test_report_written_with_five_sections(tmp_path: Path):
    rows = [_row()]
    report_path = dag_mod.generate_daily_report(output_dir=tmp_path, rows=rows, report_date="2026-03-04")
    content = report_path.read_text()
    assert report_path.exists()
    for heading in (
        "Executive Summary",
        "Top Failures",
        "Longest Runs",
        "Missing Approvals",
        "Counts by Pipeline/DAG",
    ):
        assert heading in content


def test_report_uses_yesterday_by_default(tmp_path: Path):
    today = datetime(2026, 3, 5, 10, 0, tzinfo=timezone.utc)
    report_path = dag_mod.generate_daily_report(output_dir=tmp_path, rows=[], today=today)
    assert report_path.name == "audit-2026-03-04.md"


def test_missing_approvals_flags_prd_deploys(tmp_path: Path):
    rows = [
        _row(env="prd", job_name="deploy-app", approval_pct=50.0, total_runs=1, approval_count=0),
    ]
    report_path = dag_mod.generate_daily_report(output_dir=tmp_path, rows=rows, report_date="2026-03-04")
    content = report_path.read_text()
    assert "Missing Approvals" in content
    assert "deploy-app" in content
    assert "prd" in content


def test_empty_summary_produces_report_with_zeros(tmp_path: Path):
    report_path = dag_mod.generate_daily_report(output_dir=tmp_path, rows=[], report_date="2026-03-04")
    content = report_path.read_text()
    assert "Total runs: 0" in content
    assert "Success rate: 0%" in content
