"""Tests for ACI connector metadata.py — mocked OpenMetadata client."""
import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

DUMPS_DIR = (
    Path(__file__).parent.parent.parent.parent.parent
    / "examples/lineage-dumps"
)


@pytest.fixture
def config():
    from metadata.ingestion.source.pipeline.aci.connection import AciConfig
    return AciConfig(
        dumps_dir=DUMPS_DIR / "az-dumps",
        om_host="http://localhost:8585",
        om_user="admin",
        om_password="admin",
    )


@pytest.fixture
def mock_om():
    return MagicMock()


def test_build_pipeline_entity_has_two_tasks(config, mock_om):
    # GIVEN aci.json has 2 containers
    from metadata.ingestion.source.pipeline.aci.metadata import AciConnector
    connector = AciConnector(config=config, metadata=mock_om)
    # WHEN build_pipeline_entity is called
    req = connector.build_pipeline_entity()
    # THEN the Pipeline request has 2 tasks
    assert len(req.tasks) == 2
    # AND each task has image and cpu tags
    assert all(any("image=" in t for t in task.tags) for task in req.tasks)
    assert all(any("cpu=" in t for t in task.tags) for task in req.tasks)


def test_build_pipeline_entity_has_image_version_tag(config, mock_om):
    # GIVEN aci.json contains image tag 231
    from metadata.ingestion.source.pipeline.aci.metadata import AciConnector
    connector = AciConnector(config=config, metadata=mock_om)
    req = connector.build_pipeline_entity()
    # THEN tags include image_version=231 and env=prd
    tag_values = [str(t) for t in req.tags]
    assert any("231" in v for v in tag_values)
    assert any("prd" in v for v in tag_values)


def test_build_pipeline_entity_name(config, mock_om):
    # GIVEN aci.json has name aci-pipeline-prd
    from metadata.ingestion.source.pipeline.aci.metadata import AciConnector
    connector = AciConnector(config=config, metadata=mock_om)
    req = connector.build_pipeline_entity()
    # THEN Pipeline name matches
    assert str(req.name) == "aci-pipeline-prd"


def test_malformed_aci_json_raises(tmp_path, mock_om):
    # GIVEN az-dumps/aci.json contains invalid JSON
    bad_dir = tmp_path / "az-dumps"
    bad_dir.mkdir()
    (bad_dir / "aci.json").write_text("{ not valid json }")
    # provide valid acr tags so storage creation can continue
    (bad_dir / "acr_tags_airflow.json").write_text(json.dumps(["229", "230", "231"]))
    (bad_dir / "acr_tags_crypt.json").write_text(json.dumps(["229", "230", "231"]))
    from metadata.ingestion.source.pipeline.aci.connection import AciConfig
    from metadata.ingestion.source.pipeline.aci.metadata import AciConnector
    config = AciConfig(
        dumps_dir=bad_dir,
        om_host="http://localhost:8585",
        om_user="admin",
        om_password="admin",
    )
    connector = AciConnector(config=config, metadata=mock_om)
    # WHEN run THEN logs error, continues storage, returns non-zero
    rc = connector.run()
    assert rc != 0
    # storage still processed
    storage_calls = [str(c) for c in mock_om.create_or_update.call_args_list]
    assert any("latest_tag=231" in c for c in storage_calls)


def test_run_creates_service_and_pipeline(config, mock_om):
    # GIVEN ACI connector run
    mock_om.get_by_name.return_value = None
    from metadata.ingestion.source.pipeline.aci.metadata import AciConnector
    connector = AciConnector(config=config, metadata=mock_om)
    connector.run()
    calls = [str(c) for c in mock_om.get_or_create_service.call_args_list]
    assert any("ACI-prd" in c for c in calls)
    mock_om.create_or_update.assert_called()


def test_run_is_idempotent_when_entities_exist(config, mock_om):
    # GIVEN pipeline already exists AND rerun should update tags, not duplicate
    mock_om.get_by_name.return_value = MagicMock()
    from metadata.ingestion.source.pipeline.aci.metadata import AciConnector
    connector = AciConnector(config=config, metadata=mock_om)
    connector.run()
    # THEN create_or_update still called to upsert updates (idempotent upsert)
    mock_om.create_or_update.assert_called()


def test_creates_acr_storage_containers_with_latest_tag(config, mock_om):
    # GIVEN acr_tags_* dumps present
    mock_om.get_by_name.return_value = None
    from metadata.ingestion.source.pipeline.aci.metadata import AciConnector
    connector = AciConnector(config=config, metadata=mock_om)
    connector._create_storage_entities()
    # THEN storage service created and containers created with latest_tag
    service_calls = [str(c) for c in mock_om.get_or_create_service.call_args_list]
    assert any("ACR" in c for c in service_calls)
    create_calls = [c for c in mock_om.create_or_update.call_args_list]
    assert any("airflow" in str(c) and "latest_tag=231" in str(c) for c in create_calls)
    assert any("crypt" in str(c) and "latest_tag=231" in str(c) for c in create_calls)


def _copy_az_dumps(tmp_path: Path) -> Path:
    dst = tmp_path / "az-dumps"
    shutil.copytree(DUMPS_DIR / "az-dumps", dst)
    return dst


def test_idempotent_rerun_updates_tags_without_duplicates(tmp_path, mock_om):
    # GIVEN initial dumps with image tag 231 and ACR latest_tag 231
    dumps_dir = _copy_az_dumps(tmp_path)
    from metadata.ingestion.source.pipeline.aci.connection import AciConfig
    from metadata.ingestion.source.pipeline.aci.metadata import AciConnector

    config = AciConfig(
        dumps_dir=dumps_dir,
        om_host="http://localhost:8585",
        om_user="admin",
        om_password="admin",
    )
    connector = AciConnector(config=config, metadata=mock_om)
    connector.run()

    # WHEN dumps are updated with newer tags AND connector reruns
    aci_path = dumps_dir / "aci.json"
    aci_data = json.loads(aci_path.read_text())
    aci_data["containers"][0]["image"] = "acrpipelinedevhbwx.azurecr.io/pipeline/airflow:232"
    aci_data["containers"][1]["image"] = "acrpipelinedevhbwx.azurecr.io/pipeline/crypt:232"
    aci_path.write_text(json.dumps(aci_data))

    for acr_file in ("acr_tags_airflow.json", "acr_tags_crypt.json"):
        path = dumps_dir / acr_file
        tags = json.loads(path.read_text())
        tags.append("232")
        path.write_text(json.dumps(tags))

    mock_om.create_or_update.reset_mock()
    connector.run()

    # THEN latest_tag and image_version reflect newest values and no duplicates are created
    call_strs = [str(c) for c in mock_om.create_or_update.call_args_list]
    assert any("image_version=232" in c for c in call_strs)
    assert any("latest_tag=232" in c for c in call_strs)
