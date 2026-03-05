"""Tests for stitch_lineage.py."""
from unittest.mock import MagicMock

import pytest

from metadata.ingestion.source.pipeline.aci.stitch_lineage import LineageStitcher, EdgeSpec, build_edges


@pytest.fixture
def mock_om():
    om = MagicMock()
    om.get_by_name.return_value = MagicMock(id="some-uuid")
    return om


def test_posts_ado_build_to_acr_edge(mock_om):
    # GIVEN ADO build run and ACR container entities exist
    stitcher = LineageStitcher(metadata=mock_om)
    edge = EdgeSpec(
        from_type="pipeline", from_fqn="ADO.build-232",
        to_type="container", to_fqn="ACR.airflow",
        description="builds",
    )
    stitcher.post_edge(edge)
    # THEN add_lineage is called once
    mock_om.add_lineage.assert_called_once()


def test_posts_ado_deploy_to_aci_edge(mock_om):
    # GIVEN ADO deploy run and ACI pipeline entities exist
    stitcher = LineageStitcher(metadata=mock_om)
    edge = EdgeSpec(
        from_type="pipeline", from_fqn="ADO.infra-deploy-233",
        to_type="pipeline", to_fqn="ACI-prd.aci-pipeline-prd",
        description="deploys",
    )
    stitcher.post_edge(edge)
    mock_om.add_lineage.assert_called_once()


def test_skips_edge_when_from_entity_not_found(mock_om, caplog):
    # GIVEN from entity does not exist in OM
    mock_om.get_by_name.return_value = None
    stitcher = LineageStitcher(metadata=mock_om)
    edge = EdgeSpec(
        from_type="pipeline", from_fqn="ADO.build-999",
        to_type="container", to_fqn="ACR.airflow",
        description="builds",
    )
    with caplog.at_level("WARNING"):
        stitcher.post_edge(edge)
    # THEN add_lineage is NOT called
    mock_om.add_lineage.assert_not_called()
    assert any("missing" in r.message or "not found" in r.message for r in caplog.records)


def test_other_edges_posted_when_one_skipped(mock_om, caplog):
    # GIVEN one edge has a missing entity, the other is fine
    call_count = 0
    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return None if call_count == 1 else MagicMock(id="uuid")
    mock_om.get_by_name.side_effect = side_effect

    stitcher = LineageStitcher(metadata=mock_om)
    edges = [
        EdgeSpec("pipeline", "ADO.build-999", "container", "ACR.airflow", "builds"),
        EdgeSpec("pipeline", "ADO.build-232", "container", "ACR.airflow", "builds"),
    ]
    with caplog.at_level("WARNING"):
        stitcher.post_edges(edges)
    mock_om.add_lineage.assert_called_once()
    assert any("missing" in r.message or "not found" in r.message for r in caplog.records)


def test_idempotent_add_lineage_called_once_per_edge(mock_om):
    # GIVEN the same edge is posted twice
    stitcher = LineageStitcher(metadata=mock_om)
    edge = EdgeSpec("pipeline", "ADO.build-232", "container", "ACR.airflow", "builds")
    stitcher.post_edge(edge)
    stitcher.post_edge(edge)
    # THEN OM handles idempotency — we call add_lineage both times (OM deduplicates)
    assert mock_om.add_lineage.call_count == 2


def test_posts_aci_to_airflow_edge(mock_om):
    # GIVEN ACI pipeline and Airflow DAG exist
    stitcher = LineageStitcher(metadata=mock_om)
    edge = EdgeSpec(
        from_type="pipeline", from_fqn="ACI-prd.aci-pipeline-prd",
        to_type="pipeline", to_fqn="airflow_pipeline.aci_sf_encrypted_pipeline",
        description="runs",
    )
    stitcher.post_edge(edge)
    mock_om.add_lineage.assert_called_once()


def test_posts_full_cross_dimension_path(mock_om):
    # GIVEN all entities exist
    mock_om.get_by_name.return_value = MagicMock(id="uuid")
    stitcher = LineageStitcher(metadata=mock_om)
    edges = build_edges("ADO", "ACI-prd", "airflow_pipeline", "aci_sf_encrypted_pipeline")
    posted = stitcher.post_edges(edges)
    assert posted == 3
    assert mock_om.add_lineage.call_count == 3
