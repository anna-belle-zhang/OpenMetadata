"""Tests for ADO connector metadata.py."""
from pathlib import Path
from unittest.mock import MagicMock

import pytest

DUMPS_DIR = (
    Path(__file__).parent.parent.parent.parent.parent
    / "examples/lineage-dumps"
)


@pytest.fixture
def config():
    from metadata.ingestion.source.pipeline.ado.connection import AdoConfig
    return AdoConfig(dumps_dir=DUMPS_DIR / "ado-dumps")


@pytest.fixture
def mock_om():
    om = MagicMock()
    om.get_by_name.return_value = None  # entity not found = new entity
    return om


def test_creates_ado_pipeline_service(config, mock_om):
    # GIVEN ado-dumps/runs.json exists
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    connector.run()
    calls = [str(c) for c in mock_om.get_or_create_service.call_args_list]
    assert any("ADO" in c for c in calls)


def test_creates_pipeline_per_build_run(config, mock_om):
    # GIVEN runs.json contains a build run with id=232 and image=airflow:231
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    names = [str(r.name) for r in requests]
    assert any("232" in n or "build" in n for n in names)


def test_build_run_has_image_property(config, mock_om):
    # GIVEN build run with image=airflow:231
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    build_req = next(r for r in requests if "232" in str(r.name) or "build" in str(r.name).lower())
    assert any("231" in str(tag) for tag in (build_req.tags or []))


def test_deploy_run_has_approved_by(config, mock_om):
    # GIVEN deploy run 233 has approval from Abner Zhang in approvals.json
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    deploy_req = next(r for r in requests if "233" in str(r.name) or "deploy" in str(r.name).lower())
    tag_strs = [str(t) for t in (deploy_req.tags or [])]
    assert any("Abner" in t for t in tag_strs)


def test_missing_approval_does_not_raise(config, mock_om):
    # GIVEN a deploy run with no matching approval record
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    connector._approvals = {}  # empty approvals
    requests = connector.build_pipeline_requests()
    # THEN deploy run entity is still created
    assert any("233" in str(r.name) or "deploy" in str(r.name).lower() for r in requests)


def test_existing_entity_not_duplicated(config, mock_om):
    # GIVEN the entity already exists in OM (get_by_name returns a result)
    mock_om.get_by_name.return_value = MagicMock()
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    connector.run()
    # THEN create_or_update is NOT called for existing entities
    mock_om.create_or_update.assert_not_called()


def test_build_run_has_git_sha_tag(config, mock_om):
    # S6: GIVEN build run with git_sha WHEN connector builds requests
    # THEN tags include git_sha value
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    build_req = next(r for r in requests if "232" in str(r.name))
    tag_strs = [str(t) for t in (build_req.tags or [])]
    assert any("abc123def456" in t for t in tag_strs)


def test_build_run_has_pipeline_type_image_build_tag(config, mock_om):
    # S7: GIVEN build run WHEN connector builds requests
    # THEN tags include pipeline_type=image_build
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    build_req = next(r for r in requests if "232" in str(r.name))
    tag_strs = [str(t) for t in (build_req.tags or [])]
    assert any("image_build" in t for t in tag_strs)


def test_deploy_run_has_pipeline_type_infra_deploy_tag(config, mock_om):
    # S8: GIVEN deploy run WHEN connector builds requests
    # THEN tags include pipeline_type=infra_deploy
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    deploy_req = next(r for r in requests if "233" in str(r.name))
    tag_strs = [str(t) for t in (deploy_req.tags or [])]
    assert any("infra_deploy" in t for t in tag_strs)


def test_deploy_run_has_version_and_env_tags(config, mock_om):
    # Missing S8 extension: deploy runs carry version and env tags
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    deploy_req = next(r for r in requests if "233" in str(r.name))
    tag_strs = [str(t) for t in (deploy_req.tags or [])]
    assert any("version=" in t for t in tag_strs)
    assert any("env=" in t for t in tag_strs)


def test_deploy_run_has_git_sha_tag(config, mock_om):
    # MODIFIED scenario: deploy entity carries git_sha
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    deploy_req = next(r for r in requests if "233" in str(r.name))
    tag_strs = [str(t) for t in (deploy_req.tags or [])]
    assert any("abc123def456" in t for t in tag_strs)


def test_malformed_runs_json_exits_nonzero(tmp_path, mock_om):
    # GIVEN ado-dumps/runs.json contains invalid JSON
    (tmp_path / "runs.json").write_text("not json")
    from metadata.ingestion.source.pipeline.ado.connection import AdoConfig
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    config = AdoConfig(dumps_dir=tmp_path)
    connector = AdoConnector(config=config, metadata=mock_om)
    with pytest.raises(Exception):
        connector.run()
