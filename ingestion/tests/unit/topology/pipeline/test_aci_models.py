"""Tests for ACI dump Pydantic models."""
import json
from pathlib import Path

from metadata.ingestion.source.pipeline.aci.models import AciContainer, AciDump

FIXTURE = Path(__file__).parent.parent.parent / "resources/datasets/aci_dump.json"


def test_aci_dump_parses_container_group():
    # GIVEN valid aci.json content
    data = json.loads(FIXTURE.read_text())
    # WHEN parsed into AciDump
    dump = AciDump.model_validate(data)
    # THEN name and containers are populated
    assert dump.name == "aci-pipeline-prd"
    assert len(dump.containers) == 2


def test_aci_container_extracts_image_tag():
    # GIVEN a container with a full image ref including tag
    container = AciContainer(
        name="airflow",
        image="acrpipelinedevhbwx.azurecr.io/pipeline/airflow:231",
        cpu=1.0,
        memory_gb=2.0,
    )
    # WHEN image_tag is accessed
    # THEN it returns just the tag portion
    assert container.image_tag == "231"


def test_aci_container_extracts_repo():
    # GIVEN a container with a full image ref
    container = AciContainer(
        name="airflow",
        image="acrpipelinedevhbwx.azurecr.io/pipeline/airflow:231",
        cpu=1.0,
        memory_gb=2.0,
    )
    # WHEN repo is accessed
    # THEN it returns the repository name without registry prefix or tag
    assert container.repo == "airflow"
