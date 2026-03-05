"""Tests for ADO dump Pydantic models."""
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

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


def test_ado_run_has_git_sha_and_pipeline_type():
    # S1: GIVEN record with git_sha + pipeline_type WHEN parsed THEN fields populated
    data = json.loads(FIXTURE.read_text())
    run = AdoRun.model_validate(data[0])
    assert run.git_sha == "abc123def456"
    assert run.pipeline_type == "image_build"


def test_ado_run_infra_has_pipeline_type_infra_deploy():
    # S1 (deploy): GIVEN infra deploy record WHEN parsed THEN pipeline_type=infra_deploy
    data = json.loads(FIXTURE.read_text())
    run = AdoRun.model_validate(data[1])
    assert run.pipeline_type == "infra_deploy"


def test_ado_run_accepts_image_deploy_pipeline_type():
    # Delta: GIVEN record with pipeline_type=image_deploy WHEN parsed THEN accepted without validation error
    data = json.loads(FIXTURE.read_text())
    run = AdoRun.model_validate(next(r for r in data if r["pipeline_type"] == "image_deploy"))
    assert run.pipeline_type == "image_deploy"


def test_ado_run_missing_git_sha_raises():
    # S2: GIVEN record without git_sha WHEN parsed THEN ValidationError raised
    data = {
        "id": 999,
        "name": "test",
        "type": "build",
        "pipeline_type": "image_build",
    }
    with pytest.raises(ValidationError):
        AdoRun.model_validate(data)
