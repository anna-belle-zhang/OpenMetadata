"""Tests for ado_dump.py — mocks ADO REST API responses."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from metadata.ingestion.source.pipeline.ado.ado_dump import AdoDumper, AdoDumperConfig

MOCK_RUNS_RESPONSE = {
    "value": [
        {
            "id": 232,
            "name": "build-231",
            "pipeline": {"name": "build-pipeline"},
            "result": "succeeded",
            "createdDate": "2026-02-18T01:00:00Z",
            "templateParameters": {"image_tag": "airflow:231"},
            "variables": {},
            "resources": {"repositories": {"self": {"version": "abc123def456"}}},
        },
        {
            "id": 233,
            "name": "infra-deploy-231",
            "pipeline": {"name": "infra-deploy-pipeline"},
            "result": "succeeded",
            "createdDate": "2026-02-18T02:00:00Z",
            "templateParameters": {},
            "variables": {},
            "resources": {"repositories": {"self": {"version": "abc123def456"}}},
        },
    ]
}


@pytest.fixture
def config(tmp_path):
    return AdoDumperConfig(
        out_dir=tmp_path,
        org_url="https://dev.azure.com/myorg",
        project="mypipeline",
        pat="fake-pat",
    )


def test_writes_runs_json_on_first_run(config):
    # GIVEN runs.json does not exist AND ADO API returns 2 runs
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = MOCK_RUNS_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()
        dumper = AdoDumper(config)
        dumper.dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    assert len(runs) == 2


def test_appends_new_runs_on_second_run(config):
    # GIVEN runs.json already has run 232
    existing = [{"id": 232, "name": "build-231", "startTime": "2026-02-18T01:00:00Z", "type": "build"}]
    (config.out_dir / "runs.json").write_text(json.dumps(existing))
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {"value": [
            {"id": 233, "name": "infra-deploy-231", "createdDate": "2026-02-18T02:00:00Z",
             "pipeline": {"name": "infra-deploy"}, "result": "succeeded",
             "templateParameters": {}, "variables": {}}
        ]}
        mock_get.return_value.raise_for_status = MagicMock()
        dumper = AdoDumper(config)
        dumper.dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    ids = [r["id"] for r in runs]
    assert 232 in ids and 233 in ids
    assert ids.count(232) == 1  # not duplicated


def test_runs_json_contains_git_sha(config):
    # S3: GIVEN ADO API returns runs with resources.repositories.self.version
    # WHEN dumped THEN each record has git_sha
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = MOCK_RUNS_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()
        AdoDumper(config).dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    assert all("git_sha" in r for r in runs)
    assert runs[0]["git_sha"] == "abc123def456"


def test_runs_json_classifies_pipeline_type_build(config):
    # S4: build pipeline run → pipeline_type="image_build"
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = MOCK_RUNS_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()
        AdoDumper(config).dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    build_run = next(r for r in runs if r["id"] == 232)
    assert build_run["pipeline_type"] == "image_build"


def test_runs_json_classifies_pipeline_type_infra_deploy(config):
    # S5: infra deploy pipeline run → pipeline_type="infra_deploy"
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = MOCK_RUNS_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()
        AdoDumper(config).dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    deploy_run = next(r for r in runs if r["id"] == 233)
    assert deploy_run["pipeline_type"] == "infra_deploy"


def test_missing_pat_raises(tmp_path):
    # GIVEN ADO_PAT is not set
    config = AdoDumperConfig(
        out_dir=tmp_path, org_url="https://dev.azure.com/myorg",
        project="p", pat="",
    )
    dumper = AdoDumper(config)
    with pytest.raises(ValueError, match="ADO_PAT"):
        dumper.dump()


def test_writes_approvals_json(config):
    # GIVEN approvals API returns approver + timestamp
    approvals_response = {
        "value": [
            {
                "run": {"id": 233},
                "steps": [
                    {
                        "assignedApprover": {"displayName": "Abner Zhang"},
                        "lastModifiedOn": "2026-02-18T02:05:00Z",
                    }
                ],
            }
        ]
    }
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        run_resp = MagicMock()
        run_resp.json.return_value = MOCK_RUNS_RESPONSE
        run_resp.raise_for_status = MagicMock()
        approval_resp = MagicMock()
        approval_resp.json.return_value = approvals_response
        approval_resp.raise_for_status = MagicMock()
        mock_get.side_effect = [run_resp, approval_resp]

        AdoDumper(config).dump()

    approvals = json.loads((config.out_dir / "approvals.json").read_text())
    assert approvals[0]["runId"] == 233
    assert approvals[0]["approver"] == "Abner Zhang"
    assert approvals[0]["approvedAt"] == "2026-02-18T02:05:00Z"


def test_incremental_fetch_uses_last_timestamp(config):
    # GIVEN existing runs with latest timestamp
    existing = [
        {
            "id": 232,
            "name": "build-231",
            "startTime": "2026-02-18T01:00:00Z",
            "type": "build",
        }
    ]
    (config.out_dir / "runs.json").write_text(json.dumps(existing))

    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_run_resp = MagicMock()
        mock_run_resp.json.return_value = MOCK_RUNS_RESPONSE
        mock_run_resp.raise_for_status = MagicMock()
        approval_resp = MagicMock()
        approval_resp.json.return_value = {"value": []}
        approval_resp.raise_for_status = MagicMock()
        mock_get.side_effect = [mock_run_resp, approval_resp]

        AdoDumper(config).dump()

    # THEN requests.get should be called with createdAfter using last timestamp
    first_call_kwargs = mock_get.call_args_list[0].kwargs
    assert first_call_kwargs.get("params", {}).get("createdAfter") == "2026-02-18T01:00:00Z"
