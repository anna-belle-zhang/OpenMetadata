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
from dataclasses import dataclass, field
from typing import Any

from metadata.ingestion.source.pipeline.aci.connection import AciConfig
from metadata.ingestion.source.pipeline.aci.models import AciDump


@dataclass
class AciTask:
    """Simple task representation derived from an ACI container."""

    name: str
    image: str
    cpu: float
    memory_gb: float


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
            )
            for container in dump.containers
        ]
        tags = [f"image_version={image_version}", f"env={env}"]

        return AciPipelineRequest(name=dump.name, tasks=tasks, tags=tags)
