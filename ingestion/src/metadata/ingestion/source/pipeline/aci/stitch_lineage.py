"""stitch_lineage.py — post cross-dimensional lineage edges to OpenMetadata."""
import importlib
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


def _resolve_symbol(module_path: str, symbol: str):
    """Resolve generated-schema symbols for both real packages and MagicMock stubs."""
    try:
        module = importlib.import_module(module_path)
        return getattr(module, symbol)
    except Exception:
        # test harness may patch `metadata.generated.schema` as MagicMock, which is not a package
        root = importlib.import_module("metadata.generated.schema")
        suffix = module_path.replace("metadata.generated.schema.", "")
        for part in suffix.split("."):
            root = getattr(root, part)
        return getattr(root, symbol)


def _get_entity_cls(entity_type: str):
    if entity_type == "pipeline":
        Pipeline = _resolve_symbol(
            "metadata.generated.schema.entity.data.pipeline", "Pipeline"
        )
        return Pipeline
    if entity_type == "container":
        Container = _resolve_symbol(
            "metadata.generated.schema.entity.data.container", "Container"
        )
        return Container
    logger.warning("Unknown entity type: %s", entity_type)
    return None


@dataclass
class EdgeSpec:
    from_type: str
    from_fqn: str
    to_type: str
    to_fqn: str
    description: str


class LineageStitcher:
    def __init__(self, metadata: Any):
        self.metadata = metadata

    def _resolve(self, entity_type: str, fqn: str) -> Optional[Any]:
        entity_cls = _get_entity_cls(entity_type)
        if entity_cls is None:
            return None
        entity = self.metadata.get_by_name(entity=entity_cls, fqn=fqn)
        if not entity:
            logger.warning("Entity not found in OM, skipping edge: %s", fqn)
            return None
        return entity

    def post_edge(self, edge: EdgeSpec) -> bool:
        from_ref = self._resolve(edge.from_type, edge.from_fqn)
        if not from_ref:
            return False
        to_ref = self._resolve(edge.to_type, edge.to_fqn)
        if not to_ref:
            return False

        AddLineageRequest = _resolve_symbol(
            "metadata.generated.schema.api.lineage.addLineage", "AddLineageRequest"
        )
        EntitiesEdge = _resolve_symbol(
            "metadata.generated.schema.type.entityLineage", "EntitiesEdge"
        )
        LineageDetails = _resolve_symbol(
            "metadata.generated.schema.type.entityLineage", "LineageDetails"
        )
        EntityReference = _resolve_symbol(
            "metadata.generated.schema.type.entityReference", "EntityReference"
        )

        self.metadata.add_lineage(
            AddLineageRequest(
                edge=EntitiesEdge(
                    fromEntity=EntityReference(id=from_ref.id, type=edge.from_type),
                    toEntity=EntityReference(id=to_ref.id, type=edge.to_type),
                    lineageDetails=LineageDetails(description=edge.description),
                )
            )
        )
        logger.info("Posted edge: %s -> %s (%s)", edge.from_fqn, edge.to_fqn, edge.description)
        return True

    def post_edges(self, edges: List[EdgeSpec]) -> int:
        posted = sum(1 for e in edges if self.post_edge(e))
        logger.info("Posted %d/%d lineage edges", posted, len(edges))
        return posted


def build_edges(ado_service: str, aci_service: str, airflow_service: str, dag_name: str) -> List[EdgeSpec]:
    return [
        EdgeSpec(from_type="pipeline", from_fqn=f"{ado_service}.build-232",
                 to_type="container", to_fqn="ACR.airflow", description="builds"),
        EdgeSpec(from_type="pipeline", from_fqn=f"{ado_service}.infra-deploy-233",
                 to_type="pipeline", to_fqn=f"{aci_service}.aci-pipeline-prd", description="deploys"),
        EdgeSpec(from_type="pipeline", from_fqn=f"{aci_service}.aci-pipeline-prd",
                 to_type="pipeline", to_fqn=f"{airflow_service}.{dag_name}", description="runs"),
    ]


def main():
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection, AuthProvider,
    )

    host = os.environ.get("OM_HOST", "http://localhost:8585")
    token = os.environ.get("OM_JWT_TOKEN", "")
    dag_name = os.environ.get("AIRFLOW_DAG_NAME", "aci_sf_encrypted_pipeline")

    server_config = OpenMetadataConnection(
        hostPort=host, authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=token),
    )
    metadata = OpenMetadata(server_config)
    edges = build_edges("ADO", "ACI-prd", "airflow_pipeline", dag_name)
    stitcher = LineageStitcher(metadata=metadata)
    posted = stitcher.post_edges(edges)
    sys.exit(0 if posted > 0 else 1)


if __name__ == "__main__":
    main()
