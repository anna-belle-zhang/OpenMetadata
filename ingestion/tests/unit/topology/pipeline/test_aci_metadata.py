"""Tests for ACI connector metadata.py — mocked OpenMetadata client."""
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
    from metadata.ingestion.source.pipeline.aci.connection import AciConfig
    from metadata.ingestion.source.pipeline.aci.metadata import AciConnector
    config = AciConfig(
        dumps_dir=bad_dir,
        om_host="http://localhost:8585",
        om_user="admin",
        om_password="admin",
    )
    connector = AciConnector(config=config, metadata=mock_om)
    # WHEN run THEN raises ValueError
    with pytest.raises((ValueError, Exception)):
        connector.build_pipeline_entity()
