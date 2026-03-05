#  Copyright 2026 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""ACI connector metadata builder."""

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from metadata.ingestion.source.pipeline.aci.connection import AciConfig
from metadata.ingestion.source.pipeline.aci.models import AciDump

logger = logging.getLogger(__name__)


@dataclass
class AciTask:
    """Simple task representation derived from an ACI container."""

    name: str
    image: str
    cpu: float
    memory_gb: float
    tags: list[str] = field(default_factory=list)


@dataclass
class AciPipelineRequest:
    """Simple pipeline request payload for unit tests."""

    name: str
    tasks: list[AciTask] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)


class AciConnector:
    """Read ACI dumps and build a simple pipeline request object."""

    def __init__(self, config: AciConfig, metadata: Any):
        self.config = config
        self.metadata = metadata

    def build_pipeline_entity(self) -> AciPipelineRequest:
        """Build pipeline-like metadata from aci.json."""
        dump_path = self.config.dumps_dir / "aci.json"
        with dump_path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)

        dump = AciDump.model_validate(raw)
        env = dump.name.split("-")[-1] if dump.name else ""
        image_version = dump.containers[0].image_tag if dump.containers else ""

        tasks = [
            AciTask(
                name=container.name,
                image=container.image,
                cpu=container.cpu,
                memory_gb=container.memory_gb,
                tags=[f"image={container.image}", f"cpu={container.cpu}"],
            )
            for container in dump.containers
        ]
        tags = [f"image_version={image_version}", f"env={env}"]

        return AciPipelineRequest(name=dump.name, tasks=tasks, tags=tags)

    def _create_storage_entities(self) -> None:
        """Create StorageService ACR and containers with latest_tag."""
        self.metadata.get_or_create_service(service_name="ACR", service_type="storage")
        for path in self.config.dumps_dir.glob("acr_tags_*.json"):
            repo = path.stem.replace("acr_tags_", "")
            tags = json.loads(path.read_text(encoding="utf-8"))
            latest = tags[-1] if tags else ""
            self.metadata.create_or_update(
                {"name": repo, "service": "ACR", "tags": [f"latest_tag={latest}"]}
            )

    def run(self) -> int:
        """Create pipeline service, pipeline entity, and storage containers."""
        self.metadata.get_or_create_service(service_name="ACI-prd", service_type="pipeline")

        errors = 0
        try:
            pipeline = self.build_pipeline_entity()
            # Upsert pipeline to refresh tags on re-run; OM handles idempotency
            self.metadata.create_or_update(pipeline)
        except Exception as exc:  # pylint: disable=broad-except
            errors += 1
            logger.error("Failed to parse ACI dump: %s", exc)

        self._create_storage_entities()
        return errors
