"""Tests for ADO dump Pydantic models."""
import json
from pathlib import Path

from metadata.ingestion.source.pipeline.ado.models import AdoRun, AdoApproval, RunType

FIXTURE = Path(__file__).parent.parent.parent / "resources/datasets/ado_dump.json"


def test_ado_run_parses_build_run():
    data = json.loads(FIXTURE.read_text())
    run = AdoRun.model_validate(data[0])
    assert run.id == 232
    assert run.run_type == RunType.BUILD
    assert run.image == "airflow:231"


def test_ado_run_parses_deploy_run():
    data = json.loads(FIXTURE.read_text())
    run = AdoRun.model_validate(data[1])
    assert run.id == 233
    assert run.run_type == RunType.DEPLOY
    assert run.version == "231"
    assert run.env == "prd"


def test_ado_approval_parses():
    approval = AdoApproval.model_validate({
        "runId": 233,
        "approver": "Abner Zhang",
        "approvedAt": "2026-02-18T01:55:00Z",
    })
    assert approval.run_id == 233
    assert approval.approver == "Abner Zhang"
