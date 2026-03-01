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
"""ACI connector connection config."""

from pathlib import Path

from pydantic import BaseModel, Field


class AciConfig(BaseModel):
    """Configuration for the ACI connector."""

    dumps_dir: Path = Field(description="Directory containing aci.json dumps")
    om_host: str = Field(description="OpenMetadata host URL")
    om_user: str = Field(description="OpenMetadata username")
    om_password: str = Field(description="OpenMetadata password")
