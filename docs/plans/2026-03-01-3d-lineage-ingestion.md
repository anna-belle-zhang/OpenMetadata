---
execution-strategy: codex-subagents
created: 2026-03-01
codex-available: true
ralph-available: false
specs-dir: docs/specs/3d-lineage-ingestion/
---

# Three-Dimensional Lineage Ingestion — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ingest Azure infrastructure (ACI/ACR) and ADO pipeline run history into OpenMetadata as native Pipeline/StorageContainer entities with tags, then stitch cross-dimensional lineage edges.

**Architecture:** File-based two-phase approach — dump scripts write JSON files to `ingestion/examples/lineage-dumps/`, standalone Python connectors read those files and call the OM REST API via the `OpenMetadata` Python client. No OM source schema changes, no new registered connector types.

**Tech Stack:** Python 3.10+, Pydantic v2, `metadata-ingestion` package (already installed), `requests` for ADO PAT API, bash for az CLI dump script.

**Specs:** `docs/specs/3d-lineage-ingestion/`

---

## Task 1: Example dump files (test fixtures)

**Files:**
- Create: `ingestion/examples/lineage-dumps/az-dumps/aci.json`
- Create: `ingestion/examples/lineage-dumps/az-dumps/acr_tags_airflow.json`
- Create: `ingestion/examples/lineage-dumps/az-dumps/acr_tags_crypt.json`
- Create: `ingestion/examples/lineage-dumps/az-dumps/resources.json`
- Create: `ingestion/examples/lineage-dumps/az-dumps/blobs.json`
- Create: `ingestion/examples/lineage-dumps/ado-dumps/runs.json`
- Create: `ingestion/examples/lineage-dumps/ado-dumps/approvals.json`
- Create: `ingestion/tests/unit/resources/datasets/aci_dump.json`
- Create: `ingestion/tests/unit/resources/datasets/ado_dump.json`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S-ACI-1 | ACI container group state captured | az-dump-delta.md |
| S-ACI-2 | ACR image tags captured | az-dump-delta.md |
| S-ADO-1 | ADO run history captured | ado-dump-delta.md |

**Step 1: Create example ACI dump**

```bash
mkdir -p ingestion/examples/lineage-dumps/az-dumps
mkdir -p ingestion/examples/lineage-dumps/ado-dumps
```

Write `ingestion/examples/lineage-dumps/az-dumps/aci.json`:

```json
{
  "name": "aci-pipeline-prd",
  "resourceGroup": "rg-airflowdbtdatapipeline-prd",
  "location": "australiaeast",
  "containers": [
    {
      "name": "airflow",
      "image": "acrpipelinedevhbwx.azurecr.io/pipeline/airflow:231",
      "resources": { "requests": { "cpu": 1.0, "memoryInGb": 2.0 } }
    },
    {
      "name": "crypt",
      "image": "acrpipelinedevhbwx.azurecr.io/pipeline/crypt:231",
      "resources": { "requests": { "cpu": 0.5, "memoryInGb": 1.0 } }
    }
  ]
}
```

Write `ingestion/examples/lineage-dumps/az-dumps/acr_tags_airflow.json`:

```json
["229", "230", "231"]
```

Write `ingestion/examples/lineage-dumps/az-dumps/acr_tags_crypt.json`:

```json
["229", "230", "231"]
```

Write `ingestion/examples/lineage-dumps/az-dumps/resources.json`:

```json
[
  {"name": "kv-pipeline-prd-abc", "type": "Microsoft.KeyVault/vaults", "resourceGroup": "rg-airflowdbtdatapipeline-prd"},
  {"name": "stpipelinedata123", "type": "Microsoft.Storage/storageAccounts", "resourceGroup": "rg-airflowdbtdatapipeline-prd"}
]
```

Write `ingestion/examples/lineage-dumps/az-dumps/blobs.json`:

```json
[
  {"name": "encrypted"},
  {"name": "decrypted"},
  {"name": "deployments"}
]
```

Write `ingestion/examples/lineage-dumps/ado-dumps/runs.json`:

```json
[
  {
    "id": 232,
    "name": "build-231",
    "pipeline": {"name": "build-pipeline"},
    "result": "succeeded",
    "startTime": "2026-02-18T01:00:00Z",
    "type": "build",
    "image": "airflow:231"
  },
  {
    "id": 233,
    "name": "infra-deploy-231",
    "pipeline": {"name": "infra-deploy-pipeline"},
    "result": "succeeded",
    "startTime": "2026-02-18T02:00:00Z",
    "type": "deploy",
    "version": "231",
    "env": "prd"
  }
]
```

Write `ingestion/examples/lineage-dumps/ado-dumps/approvals.json`:

```json
[
  {
    "runId": 233,
    "approver": "Abner Zhang",
    "approvedAt": "2026-02-18T01:55:00Z"
  }
]
```

**Step 2: Copy as test datasets**

```bash
cp ingestion/examples/lineage-dumps/az-dumps/aci.json \
   ingestion/tests/unit/resources/datasets/aci_dump.json
cp ingestion/examples/lineage-dumps/ado-dumps/runs.json \
   ingestion/tests/unit/resources/datasets/ado_dump.json
```

**Step 3: Commit**

```bash
git add ingestion/examples/lineage-dumps/ \
        ingestion/tests/unit/resources/datasets/aci_dump.json \
        ingestion/tests/unit/resources/datasets/ado_dump.json
git commit -m "feat: add lineage dump example files and test fixtures"
```

---

## Task 2: ACI connector — Pydantic models

**Files:**
- Create: `ingestion/src/metadata/ingestion/source/pipeline/aci/__init__.py`
- Create: `ingestion/src/metadata/ingestion/source/pipeline/aci/models.py`

**Step 1: Write the failing test**

Create `ingestion/tests/unit/topology/pipeline/test_aci_models.py`:

```python
"""Tests for ACI dump Pydantic models."""
import json
from pathlib import Path

from metadata.ingestion.source.pipeline.aci.models import AciDump, AciContainer

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
    # THEN it returns the repository path without the registry prefix
    assert container.repo == "airflow"
```

**Step 2: Run to verify it fails**

```bash
cd ingestion
python -m pytest tests/unit/topology/pipeline/test_aci_models.py -v
```

Expected: `ModuleNotFoundError: No module named 'metadata.ingestion.source.pipeline.aci'`

**Step 3: Write minimal implementation**

Create `ingestion/src/metadata/ingestion/source/pipeline/aci/__init__.py` (empty):

```python
```

Create `ingestion/src/metadata/ingestion/source/pipeline/aci/models.py`:

```python
"""Pydantic models for ACI and ACR JSON dump files."""
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator


class AciContainerResources(BaseModel):
    cpu: float
    memoryInGb: float = Field(alias="memoryInGb")

    model_config = {"populate_by_name": True}


class AciContainerResourceRequests(BaseModel):
    requests: AciContainerResources


class AciContainerRaw(BaseModel):
    name: str
    image: str
    resources: AciContainerResourceRequests


class AciContainer(BaseModel):
    name: str
    image: str
    cpu: float
    memory_gb: float

    @property
    def image_tag(self) -> str:
        return self.image.rsplit(":", 1)[-1]

    @property
    def repo(self) -> str:
        # acrpipelinedevhbwx.azurecr.io/pipeline/airflow:231 → airflow
        path = self.image.split("/")[-1]
        return path.split(":")[0]

    @classmethod
    def from_raw(cls, raw: AciContainerRaw) -> "AciContainer":
        return cls(
            name=raw.name,
            image=raw.image,
            cpu=raw.resources.requests.cpu,
            memory_gb=raw.resources.requests.memoryInGb,
        )


class AciDump(BaseModel):
    name: str
    resource_group: str = Field(alias="resourceGroup")
    containers: List[AciContainer] = Field(default_factory=list)

    model_config = {"populate_by_name": True}

    @model_validator(mode="before")
    @classmethod
    def parse_containers(cls, data: dict) -> dict:
        raw_containers = data.get("containers", [])
        parsed = [
            AciContainer.from_raw(AciContainerRaw.model_validate(c))
            for c in raw_containers
        ]
        data = dict(data)
        data["containers"] = [c.model_dump() for c in parsed]
        return data
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/unit/topology/pipeline/test_aci_models.py -v
```

Expected: `3 passed`

**Step 5: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/aci/ \
        ingestion/tests/unit/topology/pipeline/test_aci_models.py
git commit -m "feat: add ACI Pydantic models for dump file parsing"
```

---

## Task 3: ACI connector — metadata.py (creates OM entities)

**Files:**
- Create: `ingestion/src/metadata/ingestion/source/pipeline/aci/connection.py`
- Create: `ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`
- Test: `ingestion/tests/unit/topology/pipeline/test_aci_metadata.py`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S-ACI-3 | Create ACI PipelineService | aci-connector-delta.md |
| S-ACI-4 | Create Pipeline with image_version tag | aci-connector-delta.md |
| S-ACI-5 | Create Task per container | aci-connector-delta.md |
| S-ACI-6 | Create ACR StorageService + StorageContainer | aci-connector-delta.md |
| S-ACI-7 | Idempotent re-run | aci-connector-delta.md |
| S-ACI-8 | Skip on malformed dump file, exit non-zero | aci-connector-delta.md |

**Step 1: Write the failing tests**

Create `ingestion/tests/unit/topology/pipeline/test_aci_metadata.py`:

```python
"""Tests for ACI connector metadata.py — uses mocked OpenMetadata client."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from metadata.ingestion.source.pipeline.aci.metadata import AciConnector
from metadata.ingestion.source.pipeline.aci.connection import AciConfig

DUMPS_DIR = Path(__file__).parent.parent.parent.parent.parent / \
    "examples/lineage-dumps"


@pytest.fixture
def config():
    return AciConfig(
        dumps_dir=DUMPS_DIR / "az-dumps",
        om_host="http://localhost:8585",
        om_user="admin",
        om_password="admin",
    )


@pytest.fixture
def mock_om():
    return MagicMock()


def test_creates_pipeline_service(config, mock_om):
    # GIVEN az-dumps/aci.json exists and OM is accessible
    connector = AciConnector(config=config, metadata=mock_om)
    # WHEN run
    connector.run()
    # THEN get_or_create_service is called with service name ACI-prd
    calls = [str(c) for c in mock_om.get_or_create_service.call_args_list]
    assert any("ACI-prd" in c for c in calls)


def test_creates_pipeline_with_image_version_tag(config, mock_om):
    # GIVEN az-dumps/aci.json contains image tag 231
    connector = AciConnector(config=config, metadata=mock_om)
    # WHEN run
    connector.run()
    # THEN create_or_update is called with a Pipeline whose tags include image_version=231
    pipeline_calls = [
        c for c in mock_om.create_or_update.call_args_list
        if "Pipeline" in str(c) or "pipeline" in str(c)
    ]
    tag_values = [str(c) for c in pipeline_calls]
    assert any("231" in v for v in tag_values)


def test_creates_task_per_container(config, mock_om):
    # GIVEN aci.json has 2 containers
    connector = AciConnector(config=config, metadata=mock_om)
    # WHEN run
    entities = connector.build_pipeline_entity()
    # THEN the Pipeline request has 2 tasks
    assert len(entities.tasks) == 2


def test_creates_acr_storage_service(config, mock_om):
    # GIVEN acr_tags_airflow.json exists
    connector = AciConnector(config=config, metadata=mock_om)
    # WHEN run
    connector.run()
    # THEN get_or_create_service called for ACR
    calls = [str(c) for c in mock_om.get_or_create_service.call_args_list]
    assert any("ACR" in c for c in calls)


def test_malformed_aci_json_exits_nonzero(tmp_path, mock_om):
    # GIVEN az-dumps/aci.json contains invalid JSON
    bad_dir = tmp_path / "az-dumps"
    bad_dir.mkdir()
    (bad_dir / "aci.json").write_text("{ not valid json }")
    config = AciConfig(
        dumps_dir=bad_dir,
        om_host="http://localhost:8585",
        om_user="admin",
        om_password="admin",
    )
    connector = AciConnector(config=config, metadata=mock_om)
    # WHEN run
    # THEN a ValueError (or similar) is raised and non-zero exit ensured
    with pytest.raises(Exception):
        connector.run()
```

**Step 2: Run to verify it fails**

```bash
python -m pytest tests/unit/topology/pipeline/test_aci_metadata.py -v
```

Expected: `ModuleNotFoundError` on `connection` and `metadata` imports.

**Step 3: Write minimal implementation**

Create `ingestion/src/metadata/ingestion/source/pipeline/aci/connection.py`:

```python
"""ACI connector configuration."""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AciConfig:
    dumps_dir: Path
    om_host: str
    om_user: str
    om_password: str
```

Create `ingestion/src/metadata/ingestion/source/pipeline/aci/metadata.py`:

```python
"""ACI connector — reads az-dumps/ JSON files and creates OM entities."""
import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.entity.data.pipeline import Task
from metadata.generated.schema.entity.services.pipelineService import (
    PipelineServiceType,
)
from metadata.generated.schema.type.basic import EntityName, FullyQualifiedEntityName
from metadata.generated.schema.type.tagLabel import (
    LabelType,
    State,
    TagLabel,
    TagSource,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.pipeline.aci.connection import AciConfig
from metadata.ingestion.source.pipeline.aci.models import AciDump

logger = logging.getLogger(__name__)

ACI_SERVICE_NAME = "ACI-prd"
ACR_SERVICE_NAME = "ACR"


def _make_tag(key: str, value: str) -> TagLabel:
    return TagLabel(
        tagFQN=FullyQualifiedEntityName(f"custom.{key}.{value}"),
        labelType=LabelType.Automated,
        state=State.Suggested,
        source=TagSource.Classification,
    )


class AciConnector:
    def __init__(self, config: AciConfig, metadata: OpenMetadata):
        self.config = config
        self.metadata = metadata
        self._errors: List[str] = []

    def _load_aci_dump(self) -> AciDump:
        aci_path = self.config.dumps_dir / "aci.json"
        try:
            data = json.loads(aci_path.read_text(encoding="utf-8"))
            return AciDump.model_validate(data)
        except (json.JSONDecodeError, ValueError) as exc:
            raise ValueError(f"Malformed {aci_path}: {exc}") from exc

    def _load_acr_tags(self, repo: str) -> List[str]:
        tag_path = self.config.dumps_dir / f"acr_tags_{repo}.json"
        if not tag_path.exists():
            return []
        try:
            return json.loads(tag_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("Malformed %s — skipping ACR tags for %s", tag_path, repo)
            return []

    def build_pipeline_entity(self) -> CreatePipelineRequest:
        dump = self._load_aci_dump()
        current_tag = dump.containers[0].image_tag if dump.containers else "unknown"
        tasks = [
            Task(
                name=EntityName(c.name),
                displayName=c.image,
                description=f"cpu={c.cpu} memory={c.memory_gb}GB",
            )
            for c in dump.containers
        ]
        return CreatePipelineRequest(
            name=EntityName(dump.name),
            displayName=dump.name,
            service=FullyQualifiedEntityName(ACI_SERVICE_NAME),
            tasks=tasks,
            tags=[
                _make_tag("image_version", current_tag),
                _make_tag("env", "prd"),
            ],
        )

    def run(self) -> int:
        # Create ACI PipelineService
        self.metadata.get_or_create_service(
            entity=__import__(
                "metadata.generated.schema.entity.services.pipelineService",
                fromlist=["PipelineService"],
            ).PipelineService,
            service_name=ACI_SERVICE_NAME,
            service_type=PipelineServiceType.CustomPipeline,
        )

        # Create Pipeline entity
        pipeline_request = self.build_pipeline_entity()
        self.metadata.create_or_update(pipeline_request)
        logger.info("Created pipeline: %s", pipeline_request.name)

        # Create ACR StorageService + StorageContainers
        self.metadata.get_or_create_service(
            entity=__import__(
                "metadata.generated.schema.entity.services.storageService",
                fromlist=["StorageService"],
            ).StorageService,
            service_name=ACR_SERVICE_NAME,
        )

        for repo in ("airflow", "crypt"):
            tags = self._load_acr_tags(repo)
            latest = tags[-1] if tags else "unknown"
            self.metadata.create_or_update(
                __import__(
                    "metadata.generated.schema.api.data.createContainer",
                    fromlist=["CreateContainerRequest"],
                ).CreateContainerRequest(
                    name=EntityName(repo),
                    service=FullyQualifiedEntityName(ACR_SERVICE_NAME),
                    tags=[_make_tag("latest_tag", latest)],
                )
            )
            logger.info("Created ACR container: %s (latest=%s)", repo, latest)

        if self._errors:
            logger.error("Completed with %d errors: %s", len(self._errors), self._errors)
            return 1
        return 0


def main():
    import os
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
        AuthProvider,
    )

    host = os.environ.get("OM_HOST", "http://localhost:8585")
    token = os.environ.get("OM_JWT_TOKEN", "")
    dumps_dir = Path(os.environ.get("AZ_DUMPS_DIR", "ingestion/examples/lineage-dumps/az-dumps"))

    server_config = OpenMetadataConnection(
        hostPort=host,
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=token),
    )
    metadata = OpenMetadata(server_config)

    config = AciConfig(
        dumps_dir=dumps_dir,
        om_host=host,
        om_user="",
        om_password="",
    )
    connector = AciConnector(config=config, metadata=metadata)
    sys.exit(connector.run())


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/unit/topology/pipeline/test_aci_metadata.py -v
```

Expected: `5 passed`

**Step 5: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/aci/ \
        ingestion/tests/unit/topology/pipeline/test_aci_metadata.py
git commit -m "feat: add ACI connector — reads az-dumps and creates OM Pipeline/Container entities"
```

---

## Task 4: ADO connector — Pydantic models

**Files:**
- Create: `ingestion/src/metadata/ingestion/source/pipeline/ado/__init__.py`
- Create: `ingestion/src/metadata/ingestion/source/pipeline/ado/models.py`
- Test: `ingestion/tests/unit/topology/pipeline/test_ado_models.py`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S-ADO-1 | ADO run history with run_id, type, image/version | ado-dump-delta.md |
| S-ADO-2 | Approval records with runId, approver, approvedAt | ado-dump-delta.md |

**Step 1: Write the failing test**

Create `ingestion/tests/unit/topology/pipeline/test_ado_models.py`:

```python
"""Tests for ADO dump Pydantic models."""
import json
from pathlib import Path

from metadata.ingestion.source.pipeline.ado.models import AdoRun, AdoApproval, RunType

FIXTURE = Path(__file__).parent.parent.parent / "resources/datasets/ado_dump.json"


def test_ado_run_parses_build_run():
    data = json.loads(FIXTURE.read_text())
    run = AdoRun.model_validate(data[0])
    assert run.id == 232
    assert run.run_type == RunType.BUILD
    assert run.image == "airflow:231"


def test_ado_run_parses_deploy_run():
    data = json.loads(FIXTURE.read_text())
    run = AdoRun.model_validate(data[1])
    assert run.id == 233
    assert run.run_type == RunType.DEPLOY
    assert run.version == "231"
    assert run.env == "prd"


def test_ado_approval_parses():
    approval = AdoApproval.model_validate({
        "runId": 233,
        "approver": "Abner Zhang",
        "approvedAt": "2026-02-18T01:55:00Z",
    })
    assert approval.run_id == 233
    assert approval.approver == "Abner Zhang"
```

**Step 2: Run to verify it fails**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_models.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Write minimal implementation**

Create `ingestion/src/metadata/ingestion/source/pipeline/ado/__init__.py` (empty).

Create `ingestion/src/metadata/ingestion/source/pipeline/ado/models.py`:

```python
"""Pydantic models for ADO run and approval dump files."""
from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class RunType(str, Enum):
    BUILD = "build"
    DEPLOY = "deploy"


class AdoRun(BaseModel):
    id: int
    name: str
    result: Optional[str] = None
    start_time: Optional[datetime] = Field(None, alias="startTime")
    run_type: RunType = Field(alias="type")
    image: Optional[str] = None       # build runs: image tag built
    version: Optional[str] = None     # deploy runs: version deployed
    env: Optional[str] = None         # deploy runs: target environment

    model_config = {"populate_by_name": True}


class AdoApproval(BaseModel):
    run_id: int = Field(alias="runId")
    approver: str
    approved_at: datetime = Field(alias="approvedAt")

    model_config = {"populate_by_name": True}
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_models.py -v
```

Expected: `3 passed`

**Step 5: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/ado/ \
        ingestion/tests/unit/topology/pipeline/test_ado_models.py
git commit -m "feat: add ADO Pydantic models for run and approval dump parsing"
```

---

## Task 5: ADO connector — metadata.py

**Files:**
- Create: `ingestion/src/metadata/ingestion/source/pipeline/ado/connection.py`
- Create: `ingestion/src/metadata/ingestion/source/pipeline/ado/metadata.py`
- Test: `ingestion/tests/unit/topology/pipeline/test_ado_metadata.py`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S-ADO-3 | Create ADO PipelineService | ado-connector-delta.md |
| S-ADO-4 | Create Pipeline per build run with run_id, image, date | ado-connector-delta.md |
| S-ADO-5 | Create Pipeline per deploy run with version, env | ado-connector-delta.md |
| S-ADO-6 | Attach approved_by to deploy run entity | ado-connector-delta.md |
| S-ADO-7 | Append-only: do not overwrite existing run entities | ado-connector-delta.md |
| S-ADO-8 | Skip run on missing approval (no error raised) | ado-connector-delta.md |
| S-ADO-9 | Skip on malformed runs.json, exit non-zero | ado-connector-delta.md |

**Step 1: Write the failing tests**

Create `ingestion/tests/unit/topology/pipeline/test_ado_metadata.py`:

```python
"""Tests for ADO connector metadata.py."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
from metadata.ingestion.source.pipeline.ado.connection import AdoConfig

DUMPS_DIR = Path(__file__).parent.parent.parent.parent.parent / \
    "examples/lineage-dumps"


@pytest.fixture
def config():
    return AdoConfig(dumps_dir=DUMPS_DIR / "ado-dumps")


@pytest.fixture
def mock_om():
    om = MagicMock()
    om.get_by_name.return_value = None  # entity not found = new entity
    return om


def test_creates_ado_pipeline_service(config, mock_om):
    # GIVEN ado-dumps/runs.json exists
    connector = AdoConnector(config=config, metadata=mock_om)
    connector.run()
    calls = [str(c) for c in mock_om.get_or_create_service.call_args_list]
    assert any("ADO" in c for c in calls)


def test_creates_pipeline_per_build_run(config, mock_om):
    # GIVEN runs.json contains a build run with id=232 and image=airflow:231
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    names = [str(r.name) for r in requests]
    assert any("232" in n or "build" in n for n in names)


def test_build_run_has_image_property(config, mock_om):
    # GIVEN build run with image=airflow:231
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    build_req = next(r for r in requests if "232" in str(r.name) or "build" in str(r.name).lower())
    assert any("231" in str(tag) for tag in (build_req.tags or []))


def test_deploy_run_has_approved_by(config, mock_om):
    # GIVEN deploy run 233 has approval from Abner Zhang in approvals.json
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    deploy_req = next(r for r in requests if "233" in str(r.name) or "deploy" in str(r.name).lower())
    tag_strs = [str(t) for t in (deploy_req.tags or [])]
    assert any("Abner" in t for t in tag_strs)


def test_missing_approval_does_not_raise(config, mock_om):
    # GIVEN a deploy run with no matching approval record
    connector = AdoConnector(config=config, metadata=mock_om)
    connector._approvals = {}  # empty approvals
    requests = connector.build_pipeline_requests()
    # THEN deploy run entity is still created
    assert any("233" in str(r.name) or "deploy" in str(r.name).lower() for r in requests)


def test_existing_entity_not_duplicated(config, mock_om):
    # GIVEN the entity already exists in OM (get_by_name returns a result)
    mock_om.get_by_name.return_value = MagicMock()
    connector = AdoConnector(config=config, metadata=mock_om)
    connector.run()
    # THEN create_or_update is NOT called for existing entities
    mock_om.create_or_update.assert_not_called()


def test_malformed_runs_json_exits_nonzero(tmp_path, mock_om):
    # GIVEN ado-dumps/runs.json contains invalid JSON
    bad_dir = tmp_path
    (bad_dir / "runs.json").write_text("not json")
    config = AdoConfig(dumps_dir=bad_dir)
    connector = AdoConnector(config=config, metadata=mock_om)
    with pytest.raises(Exception):
        connector.run()
```

**Step 2: Run to verify it fails**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_metadata.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Write minimal implementation**

Create `ingestion/src/metadata/ingestion/source/pipeline/ado/connection.py`:

```python
"""ADO connector configuration."""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AdoConfig:
    dumps_dir: Path
```

Create `ingestion/src/metadata/ingestion/source/pipeline/ado/metadata.py`:

```python
"""ADO connector — reads ado-dumps/ JSON files and creates OM Pipeline entities."""
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.entity.data.pipeline import Pipeline
from metadata.generated.schema.entity.services.pipelineService import PipelineServiceType
from metadata.generated.schema.type.basic import EntityName, FullyQualifiedEntityName
from metadata.generated.schema.type.tagLabel import (
    LabelType,
    State,
    TagLabel,
    TagSource,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.pipeline.ado.connection import AdoConfig
from metadata.ingestion.source.pipeline.ado.models import AdoApproval, AdoRun, RunType

logger = logging.getLogger(__name__)

ADO_SERVICE_NAME = "ADO"


def _make_tag(key: str, value: str) -> TagLabel:
    return TagLabel(
        tagFQN=FullyQualifiedEntityName(f"custom.{key}.{value}"),
        labelType=LabelType.Automated,
        state=State.Suggested,
        source=TagSource.Classification,
    )


class AdoConnector:
    def __init__(self, config: AdoConfig, metadata: OpenMetadata):
        self.config = config
        self.metadata = metadata
        self._approvals: Dict[int, AdoApproval] = {}

    def _load_runs(self) -> List[AdoRun]:
        runs_path = self.config.dumps_dir / "runs.json"
        try:
            data = json.loads(runs_path.read_text(encoding="utf-8"))
            return [AdoRun.model_validate(r) for r in data]
        except (json.JSONDecodeError, ValueError) as exc:
            raise ValueError(f"Malformed {runs_path}: {exc}") from exc

    def _load_approvals(self) -> Dict[int, AdoApproval]:
        approvals_path = self.config.dumps_dir / "approvals.json"
        if not approvals_path.exists():
            return {}
        try:
            data = json.loads(approvals_path.read_text(encoding="utf-8"))
            return {a["runId"]: AdoApproval.model_validate(a) for a in data}
        except json.JSONDecodeError:
            logger.warning("Malformed approvals.json — skipping approvals")
            return {}

    def build_pipeline_requests(self) -> List[CreatePipelineRequest]:
        runs = self._load_runs()
        if not self._approvals:
            self._approvals = self._load_approvals()

        requests = []
        for run in runs:
            tags = [
                _make_tag("run_id", str(run.id)),
                _make_tag("date", run.start_time.date().isoformat() if run.start_time else "unknown"),
            ]
            if run.run_type == RunType.BUILD and run.image:
                tags.append(_make_tag("image", run.image))
            if run.run_type == RunType.DEPLOY:
                if run.version:
                    tags.append(_make_tag("version", run.version))
                if run.env:
                    tags.append(_make_tag("env", run.env))
                approval = self._approvals.get(run.id)
                if approval:
                    tags.append(_make_tag("approved_by", approval.approver))
                    tags.append(_make_tag("approved_at", approval.approved_at.isoformat()))

            requests.append(
                CreatePipelineRequest(
                    name=EntityName(run.name),
                    displayName=run.name,
                    service=FullyQualifiedEntityName(ADO_SERVICE_NAME),
                    tags=tags,
                )
            )
        return requests

    def run(self) -> int:
        self.metadata.get_or_create_service(
            entity=__import__(
                "metadata.generated.schema.entity.services.pipelineService",
                fromlist=["PipelineService"],
            ).PipelineService,
            service_name=ADO_SERVICE_NAME,
            service_type=PipelineServiceType.CustomPipeline,
        )

        requests = self.build_pipeline_requests()
        created = 0
        for req in requests:
            existing = self.metadata.get_by_name(entity=Pipeline, fqn=f"{ADO_SERVICE_NAME}.{req.name}")
            if existing:
                logger.info("Skipping existing run entity: %s", req.name)
                continue
            self.metadata.create_or_update(req)
            created += 1
            logger.info("Created ADO pipeline entity: %s", req.name)

        logger.info("Done. Created %d new entities.", created)
        return 0


def main():
    import os
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
        AuthProvider,
    )

    host = os.environ.get("OM_HOST", "http://localhost:8585")
    token = os.environ.get("OM_JWT_TOKEN", "")
    dumps_dir = Path(os.environ.get("ADO_DUMPS_DIR", "ingestion/examples/lineage-dumps/ado-dumps"))

    server_config = OpenMetadataConnection(
        hostPort=host,
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=token),
    )
    metadata = OpenMetadata(server_config)
    config = AdoConfig(dumps_dir=dumps_dir)
    connector = AdoConnector(config=config, metadata=metadata)
    sys.exit(connector.run())


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_metadata.py -v
```

Expected: `7 passed`

**Step 5: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/ado/ \
        ingestion/tests/unit/topology/pipeline/test_ado_metadata.py
git commit -m "feat: add ADO connector — reads ado-dumps and creates OM Pipeline audit entities"
```

---

## Task 6: az_dump.sh

**Files:**
- Create: `ingestion/src/metadata/ingestion/source/pipeline/aci/az_dump.sh`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S-AZDUMP-1 | Writes aci.json with container group name, containers, images | az-dump-delta.md |
| S-AZDUMP-2 | Writes acr_tags_airflow.json and acr_tags_crypt.json | az-dump-delta.md |
| S-AZDUMP-3 | Writes resources.json | az-dump-delta.md |
| S-AZDUMP-4 | Writes blobs.json | az-dump-delta.md |
| S-AZDUMP-5 | Idempotent — re-run overwrites files | az-dump-delta.md |
| S-AZDUMP-6 | Exits non-zero and writes nothing on az error | az-dump-delta.md |

> Note: This is a bash script, not Python — tested manually by running against the real Azure subscription. The example files in `ingestion/examples/lineage-dumps/az-dumps/` serve as the expected output reference.

**Step 1: Write the script**

Create `ingestion/src/metadata/ingestion/source/pipeline/aci/az_dump.sh`:

```bash
#!/usr/bin/env bash
# az_dump.sh — dump Azure resource state to JSON files for lineage ingestion
# Usage: bash az_dump.sh [output_dir]
#
# Prerequisites:
#   - az login completed in the current shell
#   - Env vars: AZURE_RG, AZURE_ACR, AZURE_ACI_NAME, AZURE_STORAGE_ACCOUNT
#
# Defaults match the prd environment.

set -euo pipefail

OUT="${1:-$(pwd)/ingestion/examples/lineage-dumps/az-dumps}"
RG="${AZURE_RG:-rg-airflowdbtdatapipeline-prd}"
ACR="${AZURE_ACR:-acrpipelinedevhbwx}"
ACI_NAME="${AZURE_ACI_NAME:-aci-pipeline-prd}"
STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT:-stpipelinedata}"

mkdir -p "$OUT"

echo "Dumping ACI container group: $ACI_NAME"
az container show \
  --resource-group "$RG" \
  --name "$ACI_NAME" \
  --query "{name:name, resourceGroup:resourceGroup, location:location, containers:containers[].{name:name, image:image, resources:resources}}" \
  -o json > "$OUT/aci.json"

echo "Dumping ACR tags: $ACR/pipeline/airflow"
az acr repository show-tags \
  --name "$ACR" \
  --repository pipeline/airflow \
  -o json > "$OUT/acr_tags_airflow.json"

echo "Dumping ACR tags: $ACR/pipeline/crypt"
az acr repository show-tags \
  --name "$ACR" \
  --repository pipeline/crypt \
  -o json > "$OUT/acr_tags_crypt.json"

echo "Dumping ARM resource list"
az resource list \
  --resource-group "$RG" \
  --query "[].{name:name, type:type, resourceGroup:resourceGroup}" \
  -o json > "$OUT/resources.json"

echo "Dumping Blob containers: $STORAGE_ACCOUNT"
az storage container list \
  --account-name "$STORAGE_ACCOUNT" \
  --auth-mode login \
  --query "[].{name:name}" \
  -o json > "$OUT/blobs.json"

echo "Done. Files written to: $OUT"
ls -lh "$OUT"
```

**Step 2: Make executable and verify syntax**

```bash
chmod +x ingestion/src/metadata/ingestion/source/pipeline/aci/az_dump.sh
bash -n ingestion/src/metadata/ingestion/source/pipeline/aci/az_dump.sh
```

Expected: no output (bash -n only checks syntax)

**Step 3: Manual smoke test (requires az login)**

```bash
# Only run if az login is active
az account show --query name -o tsv  # verify logged in
bash ingestion/src/metadata/ingestion/source/pipeline/aci/az_dump.sh /tmp/az-test-dump
ls /tmp/az-test-dump/
```

Expected: `aci.json  acr_tags_airflow.json  acr_tags_crypt.json  resources.json  blobs.json`

**Step 4: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/aci/az_dump.sh
git commit -m "feat: add az_dump.sh — dumps Azure resource state to JSON files"
```

---

## Task 7: ado_dump.py

**Files:**
- Create: `ingestion/src/metadata/ingestion/source/pipeline/ado/ado_dump.py`
- Test: `ingestion/tests/unit/topology/pipeline/test_ado_dump.py`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S-ADODUMP-1 | Writes runs.json with run_id, type, image/version | ado-dump-delta.md |
| S-ADODUMP-2 | Writes approvals.json with runId, approver, approvedAt | ado-dump-delta.md |
| S-ADODUMP-3 | Append-only: existing records not overwritten | ado-dump-delta.md |
| S-ADODUMP-4 | First run: fetches all available history | ado-dump-delta.md |
| S-ADODUMP-5 | Incremental: only fetches runs newer than last timestamp | ado-dump-delta.md |
| S-ADODUMP-6 | Missing/invalid ADO_PAT: exits non-zero, writes nothing | ado-dump-delta.md |

**Step 1: Write the failing tests**

Create `ingestion/tests/unit/topology/pipeline/test_ado_dump.py`:

```python
"""Tests for ado_dump.py — mocks ADO REST API responses."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from metadata.ingestion.source.pipeline.ado.ado_dump import AdoDumper, AdoDumperConfig

MOCK_RUNS_RESPONSE = {
    "value": [
        {
            "id": 232,
            "name": "build-231",
            "pipeline": {"name": "build-pipeline"},
            "result": "succeeded",
            "createdDate": "2026-02-18T01:00:00Z",
            "templateParameters": {"image_tag": "airflow:231"},
            "variables": {},
        },
        {
            "id": 233,
            "name": "infra-deploy-231",
            "pipeline": {"name": "infra-deploy-pipeline"},
            "result": "succeeded",
            "createdDate": "2026-02-18T02:00:00Z",
            "templateParameters": {},
            "variables": {},
        },
    ]
}

MOCK_APPROVALS_RESPONSE = {
    "value": [
        {
            "steps": [{"assignedApprover": {"displayName": "Abner Zhang"}, "lastModifiedOn": "2026-02-18T01:55:00Z"}],
            "pipeline": {"id": 233},
        }
    ]
}


@pytest.fixture
def config(tmp_path):
    return AdoDumperConfig(
        out_dir=tmp_path,
        org_url="https://dev.azure.com/myorg",
        project="mypipeline",
        pat="fake-pat",
    )


def test_writes_runs_json_on_first_run(config):
    # GIVEN runs.json does not exist AND ADO API returns 2 runs
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = MOCK_RUNS_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()
        dumper = AdoDumper(config)
        dumper.dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    assert len(runs) == 2


def test_appends_new_runs_on_second_run(config):
    # GIVEN runs.json already has run 232
    existing = [{"id": 232, "name": "build-231", "startTime": "2026-02-18T01:00:00Z", "type": "build"}]
    (config.out_dir / "runs.json").write_text(json.dumps(existing))
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = {"value": [
            {"id": 233, "name": "infra-deploy-231", "createdDate": "2026-02-18T02:00:00Z",
             "pipeline": {"name": "infra-deploy"}, "result": "succeeded", "templateParameters": {}, "variables": {}}
        ]}
        mock_get.return_value.raise_for_status = MagicMock()
        dumper = AdoDumper(config)
        dumper.dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    ids = [r["id"] for r in runs]
    assert 232 in ids and 233 in ids
    assert ids.count(232) == 1  # not duplicated


def test_missing_pat_raises(tmp_path):
    # GIVEN ADO_PAT is not set
    config = AdoDumperConfig(
        out_dir=tmp_path, org_url="https://dev.azure.com/myorg",
        project="p", pat="",
    )
    dumper = AdoDumper(config)
    with pytest.raises(ValueError, match="ADO_PAT"):
        dumper.dump()
```

**Step 2: Run to verify it fails**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_dump.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Write minimal implementation**

Create `ingestion/src/metadata/ingestion/source/pipeline/ado/ado_dump.py`:

```python
"""ado_dump.py — fetch ADO pipeline runs and approvals and write to JSON files."""
import base64
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class AdoDumperConfig:
    out_dir: Path
    org_url: str
    project: str
    pat: str


def _auth_header(pat: str) -> Dict[str, str]:
    token = base64.b64encode(f":{pat}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def _api_get(url: str, pat: str, params: Optional[Dict] = None) -> Any:
    resp = requests.get(url, headers=_auth_header(pat), params=params)
    resp.raise_for_status()
    return resp.json()


def _normalize_run(raw: Dict) -> Dict:
    params = raw.get("templateParameters", {}) or {}
    variables = raw.get("variables", {}) or {}
    pipeline_name = raw.get("pipeline", {}).get("name", "")

    run_type = "deploy" if "deploy" in pipeline_name.lower() else "build"
    return {
        "id": raw["id"],
        "name": raw["name"],
        "startTime": raw.get("createdDate"),
        "result": raw.get("result"),
        "type": run_type,
        "image": params.get("image_tag") or variables.get("image_tag", {}).get("value"),
        "version": params.get("version") or variables.get("version", {}).get("value"),
        "env": params.get("env") or variables.get("env", {}).get("value"),
    }


class AdoDumper:
    def __init__(self, config: AdoDumperConfig):
        self.config = config

    def _existing_runs(self) -> List[Dict]:
        path = self.config.out_dir / "runs.json"
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return []

    def _last_timestamp(self, existing: List[Dict]) -> Optional[str]:
        if not existing:
            return None
        times = [r.get("startTime") for r in existing if r.get("startTime")]
        return max(times) if times else None

    def dump(self) -> None:
        if not self.config.pat:
            raise ValueError("ADO_PAT is required but not set")

        existing = self._existing_runs()
        existing_ids = {r["id"] for r in existing}
        since = self._last_timestamp(existing)

        runs_url = (
            f"{self.config.org_url}/{self.config.project}/_apis/pipelines/runs"
            f"?api-version=7.1"
        )
        data = _api_get(runs_url, self.config.pat)
        raw_runs = data.get("value", [])

        new_runs = []
        for raw in raw_runs:
            normalized = _normalize_run(raw)
            if normalized["id"] not in existing_ids:
                if since is None or (normalized["startTime"] or "") > since:
                    new_runs.append(normalized)

        all_runs = existing + new_runs
        self.config.out_dir.mkdir(parents=True, exist_ok=True)
        (self.config.out_dir / "runs.json").write_text(
            json.dumps(all_runs, indent=2, default=str), encoding="utf-8"
        )
        logger.info("Wrote %d runs (%d new) to runs.json", len(all_runs), len(new_runs))

        # Fetch approvals (best effort)
        try:
            approvals_url = (
                f"{self.config.org_url}/{self.config.project}/_apis/pipelines/approvals"
                f"?api-version=7.1-preview.1"
            )
            approvals_data = _api_get(approvals_url, self.config.pat)
            approvals = []
            for item in approvals_data.get("value", []):
                steps = item.get("steps", [])
                pipeline_id = item.get("pipeline", {}).get("id")
                if steps and pipeline_id:
                    step = steps[0]
                    approvals.append({
                        "runId": pipeline_id,
                        "approver": step.get("assignedApprover", {}).get("displayName", ""),
                        "approvedAt": step.get("lastModifiedOn", ""),
                    })
            (self.config.out_dir / "approvals.json").write_text(
                json.dumps(approvals, indent=2, default=str), encoding="utf-8"
            )
            logger.info("Wrote %d approvals to approvals.json", len(approvals))
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Could not fetch approvals: %s", exc)


def main():
    config = AdoDumperConfig(
        out_dir=Path(os.environ.get("ADO_DUMPS_DIR", "ingestion/examples/lineage-dumps/ado-dumps")),
        org_url=os.environ.get("ADO_ORG_URL", ""),
        project=os.environ.get("ADO_PROJECT", ""),
        pat=os.environ.get("ADO_PAT", ""),
    )
    logging.basicConfig(level=logging.INFO)
    try:
        AdoDumper(config).dump()
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_dump.py -v
```

Expected: `4 passed`

**Step 5: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/ado/ado_dump.py \
        ingestion/tests/unit/topology/pipeline/test_ado_dump.py
git commit -m "feat: add ado_dump.py — fetch ADO run history and approvals to JSON files"
```

---

## Task 8: stitch_lineage.py

**Files:**
- Create: `ingestion/src/metadata/ingestion/source/pipeline/aci/stitch_lineage.py`
- Test: `ingestion/tests/unit/topology/pipeline/test_stitch_lineage.py`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S-STITCH-1 | Link ADO build run → ACR StorageContainer | stitch-lineage-delta.md |
| S-STITCH-2 | Link ADO deploy run → ACI Pipeline | stitch-lineage-delta.md |
| S-STITCH-3 | Link ACI Pipeline → Airflow DAG | stitch-lineage-delta.md |
| S-STITCH-4 | Skip edge when entity FQN not found, log warning | stitch-lineage-delta.md |
| S-STITCH-5 | Idempotent re-run — no duplicate edges | stitch-lineage-delta.md |

**Step 1: Write the failing tests**

Create `ingestion/tests/unit/topology/pipeline/test_stitch_lineage.py`:

```python
"""Tests for stitch_lineage.py."""
from unittest.mock import MagicMock, call, patch

import pytest

from metadata.ingestion.source.pipeline.aci.stitch_lineage import LineageStitcher, EdgeSpec


@pytest.fixture
def mock_om():
    om = MagicMock()
    # Default: entity exists
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


def test_skips_edge_when_from_entity_not_found(mock_om):
    # GIVEN from entity does not exist in OM
    mock_om.get_by_name.return_value = None
    stitcher = LineageStitcher(metadata=mock_om)
    edge = EdgeSpec(
        from_type="pipeline", from_fqn="ADO.build-999",
        to_type="container", to_fqn="ACR.airflow",
        description="builds",
    )
    stitcher.post_edge(edge)
    # THEN add_lineage is NOT called
    mock_om.add_lineage.assert_not_called()


def test_other_edges_posted_when_one_skipped(mock_om):
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
    stitcher.post_edges(edges)
    mock_om.add_lineage.assert_called_once()


def test_idempotent_add_lineage_called_once_per_edge(mock_om):
    # GIVEN the same edge is posted twice
    stitcher = LineageStitcher(metadata=mock_om)
    edge = EdgeSpec("pipeline", "ADO.build-232", "container", "ACR.airflow", "builds")
    stitcher.post_edge(edge)
    stitcher.post_edge(edge)
    # THEN OM handles idempotency — we call add_lineage both times (OM deduplicates)
    assert mock_om.add_lineage.call_count == 2
```

**Step 2: Run to verify it fails**

```bash
python -m pytest tests/unit/topology/pipeline/test_stitch_lineage.py -v
```

Expected: `ModuleNotFoundError`

**Step 3: Write minimal implementation**

Create `ingestion/src/metadata/ingestion/source/pipeline/aci/stitch_lineage.py`:

```python
"""stitch_lineage.py — post cross-dimensional lineage edges to OpenMetadata."""
import logging
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.pipeline import Pipeline
from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata

logger = logging.getLogger(__name__)

ENTITY_TYPE_MAP = {
    "pipeline": Pipeline,
    "container": Container,
}


@dataclass
class EdgeSpec:
    from_type: str
    from_fqn: str
    to_type: str
    to_fqn: str
    description: str


class LineageStitcher:
    def __init__(self, metadata: OpenMetadata):
        self.metadata = metadata

    def _resolve(self, entity_type: str, fqn: str) -> Optional[EntityReference]:
        entity_class = ENTITY_TYPE_MAP.get(entity_type)
        if not entity_class:
            logger.warning("Unknown entity type: %s", entity_type)
            return None
        entity = self.metadata.get_by_name(entity=entity_class, fqn=fqn)
        if not entity:
            logger.warning("Entity not found in OM, skipping edge: %s", fqn)
            return None
        return EntityReference(id=entity.id, type=entity_type)

    def post_edge(self, edge: EdgeSpec) -> bool:
        from_ref = self._resolve(edge.from_type, edge.from_fqn)
        if not from_ref:
            return False
        to_ref = self._resolve(edge.to_type, edge.to_fqn)
        if not to_ref:
            return False

        self.metadata.add_lineage(
            AddLineageRequest(
                edge=EntitiesEdge(
                    fromEntity=from_ref,
                    toEntity=to_ref,
                    lineageDetails=LineageDetails(description=edge.description),
                )
            )
        )
        logger.info("Posted edge: %s → %s (%s)", edge.from_fqn, edge.to_fqn, edge.description)
        return True

    def post_edges(self, edges: List[EdgeSpec]) -> int:
        posted = sum(1 for e in edges if self.post_edge(e))
        logger.info("Posted %d/%d lineage edges", posted, len(edges))
        return posted


def build_edges(ado_service: str, aci_service: str, airflow_service: str, dag_name: str) -> List[EdgeSpec]:
    return [
        # DevOps → Infra: build run → ACR image
        EdgeSpec(
            from_type="pipeline", from_fqn=f"{ado_service}.build-232",
            to_type="container", to_fqn=f"ACR.airflow",
            description="builds",
        ),
        # DevOps → Infra: deploy run → ACI container group
        EdgeSpec(
            from_type="pipeline", from_fqn=f"{ado_service}.infra-deploy-233",
            to_type="pipeline", to_fqn=f"{aci_service}.aci-pipeline-prd",
            description="deploys",
        ),
        # Infra → Data: ACI container group → Airflow DAG
        EdgeSpec(
            from_type="pipeline", from_fqn=f"{aci_service}.aci-pipeline-prd",
            to_type="pipeline", to_fqn=f"{airflow_service}.{dag_name}",
            description="runs",
        ),
    ]


def main():
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection, AuthProvider,
    )

    host = os.environ.get("OM_HOST", "http://localhost:8585")
    token = os.environ.get("OM_JWT_TOKEN", "")
    dag_name = os.environ.get("AIRFLOW_DAG_NAME", "aci_sf_encrypted_pipeline")

    server_config = OpenMetadataConnection(
        hostPort=host,
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=token),
    )
    metadata = OpenMetadata(server_config)

    edges = build_edges(
        ado_service="ADO",
        aci_service="ACI-prd",
        airflow_service="airflow_pipeline",
        dag_name=dag_name,
    )
    stitcher = LineageStitcher(metadata=metadata)
    posted = stitcher.post_edges(edges)
    sys.exit(0 if posted > 0 else 1)


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/unit/topology/pipeline/test_stitch_lineage.py -v
```

Expected: `5 passed`

**Step 5: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/aci/stitch_lineage.py \
        ingestion/tests/unit/topology/pipeline/test_stitch_lineage.py
git commit -m "feat: add stitch_lineage.py — post cross-dimensional lineage edges to OM"
```

---

## Task 9: Full test run and smoke test

**Step 1: Run all new unit tests**

```bash
cd ingestion
python -m pytest \
  tests/unit/topology/pipeline/test_aci_models.py \
  tests/unit/topology/pipeline/test_aci_metadata.py \
  tests/unit/topology/pipeline/test_ado_models.py \
  tests/unit/topology/pipeline/test_ado_metadata.py \
  tests/unit/topology/pipeline/test_ado_dump.py \
  tests/unit/topology/pipeline/test_stitch_lineage.py \
  -v
```

Expected: `all tests passed`

**Step 2: Smoke test against local OM (requires OM running at localhost:8585)**

Get a JWT token first:

```bash
export OM_JWT_TOKEN=$(curl -s -X POST http://localhost:8585/api/v1/users/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@open-metadata.org","password":"admin"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['accessToken'])")
```

Run ACI connector:

```bash
cd ingestion
AZ_DUMPS_DIR=examples/lineage-dumps/az-dumps \
OM_HOST=http://localhost:8585 \
OM_JWT_TOKEN=$OM_JWT_TOKEN \
python -m metadata.ingestion.source.pipeline.aci.metadata
```

Run ADO connector:

```bash
ADO_DUMPS_DIR=examples/lineage-dumps/ado-dumps \
OM_HOST=http://localhost:8585 \
OM_JWT_TOKEN=$OM_JWT_TOKEN \
python -m metadata.ingestion.source.pipeline.ado.metadata
```

Run lineage stitcher:

```bash
OM_HOST=http://localhost:8585 \
OM_JWT_TOKEN=$OM_JWT_TOKEN \
python -m metadata.ingestion.source.pipeline.aci.stitch_lineage
```

**Step 3: Verify in OM UI**

Open http://localhost:8585 → Pipelines → filter by service `ACI-prd`
- `aci-pipeline-prd` should be visible with `image_version=231` tag
- Two tasks (airflow, crypt) inside it

Open Pipelines → filter by service `ADO`
- `build-231` with `image=airflow:231` tag
- `infra-deploy-231` with `approved_by=Abner Zhang` tag

Open lineage for `aci-pipeline-prd` — upstream should show ADO deploy run, downstream should show Airflow DAG.

**Step 4: Final commit**

```bash
git add .
git commit -m "feat: 3D lineage ingestion Stage 1 — complete implementation"
```

---

## Running Tests Reference

```bash
cd ingestion

# All new tests
python -m pytest tests/unit/topology/pipeline/test_aci*.py tests/unit/topology/pipeline/test_ado*.py tests/unit/topology/pipeline/test_stitch*.py -v

# Single file
python -m pytest tests/unit/topology/pipeline/test_aci_models.py -v

# With coverage
python -m pytest tests/unit/topology/pipeline/test_aci*.py --cov=metadata.ingestion.source.pipeline.aci --cov-report=term-missing
```

## Environment Variables Reference

| Variable | Default | Used by |
|---|---|---|
| `AZ_DUMPS_DIR` | `ingestion/examples/lineage-dumps/az-dumps` | ACI connector |
| `ADO_DUMPS_DIR` | `ingestion/examples/lineage-dumps/ado-dumps` | ADO connector |
| `OM_HOST` | `http://localhost:8585` | All connectors |
| `OM_JWT_TOKEN` | — | All connectors |
| `ADO_PAT` | — | ado_dump.py |
| `ADO_ORG_URL` | — | ado_dump.py |
| `ADO_PROJECT` | — | ado_dump.py |
| `AZURE_RG` | `rg-airflowdbtdatapipeline-prd` | az_dump.sh |
| `AZURE_ACR` | `acrpipelinedevhbwx` | az_dump.sh |
| `AZURE_ACI_NAME` | `aci-pipeline-prd` | az_dump.sh |
| `AZURE_STORAGE_ACCOUNT` | `stpipelinedata` | az_dump.sh |
| `AIRFLOW_DAG_NAME` | `aci_sf_encrypted_pipeline` | stitch_lineage.py |
