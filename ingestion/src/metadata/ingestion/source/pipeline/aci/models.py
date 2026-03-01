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
"""
ACI models.
"""

from typing import Any

from pydantic import BaseModel, Field, model_validator


class AciContainer(BaseModel):
    """ACI container model."""

    name: str
    image: str
    cpu: float
    memory_gb: float

    @model_validator(mode="before")
    @classmethod
    def flatten_resources_requests(cls, values: Any) -> Any:
        """Map nested resources.requests.* keys to top-level fields."""
        if not isinstance(values, dict):
            return values

        requests = (values.get("resources") or {}).get("requests") or {}
        flattened = dict(values)
        if flattened.get("cpu") is None:
            flattened["cpu"] = requests.get("cpu")
        if flattened.get("memory_gb") is None:
            flattened["memory_gb"] = requests.get("memoryInGb")
        return flattened

    @property
    def image_tag(self) -> str:
        image_name = self.image.split("@", 1)[0].rsplit("/", 1)[-1]
        if ":" not in image_name:
            return ""
        return image_name.rsplit(":", 1)[-1]

    @property
    def repo(self) -> str:
        image_name = self.image.split("@", 1)[0].rsplit("/", 1)[-1]
        return image_name.split(":", 1)[0]


class AciDump(BaseModel):
    """ACI container group dump model."""

    name: str
    resource_group: str = Field(alias="resourceGroup")
    location: str
    containers: list[AciContainer] = Field(default_factory=list)
