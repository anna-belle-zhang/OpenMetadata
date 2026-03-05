---
execution-strategy: codex-subagents
created: 2026-03-02
codex-available: true
specs-dir: docs/specs/3d-lineage-ingestion/
---

# Deployment Audit Matrix Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extend Stage 1 ADO connectors with `git_sha` + `pipeline_type`, then add a new `dag_enricher.py` that stamps each Airflow DAG entity with the four join keys needed to answer compliance audit questions and locate the correct Delta `AS OF` snapshot.

**Architecture:** Three additive changes — update ADO model + dump + connector (Tasks 1–3), then build the new DAG enricher as a standalone testable script (Task 4). Run order: ado_connector → airflow_connector → stitch_lineage → dag_enricher.

**Tech Stack:** Python 3.10+, Pydantic v2, pytest, unittest.mock. No new packages.

**Specs:** `docs/specs/3d-lineage-ingestion/`

---

## Task 1: Update ADO fixture + models — add `git_sha` and `pipeline_type`

**Files:**
- Modify: `ingestion/tests/unit/resources/datasets/ado_dump.json`
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/ado/models.py`
- Modify: `ingestion/tests/unit/topology/pipeline/test_ado_models.py`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S1 | GIVEN record with `git_sha`+`pipeline_type` WHEN parsed THEN `AdoRun.git_sha` and `.pipeline_type` match | ado-models-delta.md |
| S2 | GIVEN record missing `git_sha` WHEN parsed THEN `ValidationError` raised | ado-models-delta.md |

**Step 1: Update the fixture file**

Add `git_sha` and `pipeline_type` to both records in `ingestion/tests/unit/resources/datasets/ado_dump.json`:

```json
[
  {
    "id": 232,
    "name": "build-231",
    "pipeline": {"name": "build-pipeline"},
    "result": "succeeded",
    "startTime": "2026-02-18T01:00:00Z",
    "type": "build",
    "image": "airflow:231",
    "git_sha": "abc123def456",
    "pipeline_type": "image_build"
  },
  {
    "id": 233,
    "name": "infra-deploy-231",
    "pipeline": {"name": "infra-deploy-pipeline"},
    "result": "succeeded",
    "startTime": "2026-02-18T02:00:00Z",
    "type": "deploy",
    "version": "231",
    "env": "prd",
    "git_sha": "abc123def456",
    "pipeline_type": "infra_deploy"
  }
]
```

**Step 2: Write the failing tests**

Add to `ingestion/tests/unit/topology/pipeline/test_ado_models.py`:

```python
import pytest
from pydantic import ValidationError

def test_ado_run_has_git_sha_and_pipeline_type():
    # S1: GIVEN record with both new fields WHEN parsed THEN fields populated
    data = json.loads(FIXTURE.read_text())
    run = AdoRun.model_validate(data[0])
    assert run.git_sha == "abc123def456"
    assert run.pipeline_type == "image_build"

def test_ado_run_infra_has_pipeline_type_infra_deploy():
    data = json.loads(FIXTURE.read_text())
    run = AdoRun.model_validate(data[1])
    assert run.pipeline_type == "infra_deploy"

def test_ado_run_missing_git_sha_raises():
    # S2: GIVEN record without git_sha WHEN parsed THEN ValidationError
    data = {
        "id": 999, "name": "test", "type": "build",
        "pipeline_type": "image_build",
        # git_sha intentionally omitted
    }
    with pytest.raises(ValidationError):
        AdoRun.model_validate(data)
```

**Step 3: Run tests to verify they fail**

```bash
cd ingestion
python -m pytest tests/unit/topology/pipeline/test_ado_models.py -v
```

Expected: FAIL — `AdoRun` has no `git_sha` or `pipeline_type` field yet.

**Step 4: Update `models.py`**

In `ingestion/src/metadata/ingestion/source/pipeline/ado/models.py`, add two fields to `AdoRun`:

```python
from typing import Literal, Optional
# (Literal is already in typing for Python 3.10+)

class AdoRun(BaseModel):
    id: int
    name: str
    result: Optional[str] = None
    start_time: Optional[datetime] = Field(None, alias="startTime")
    run_type: RunType = Field(alias="type")
    image: Optional[str] = None
    version: Optional[str] = None
    env: Optional[str] = None
    git_sha: str = Field(alias="git_sha")
    pipeline_type: Literal["image_build", "infra_deploy"]

    model_config = {"populate_by_name": True}
```

**Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_models.py -v
```

Expected: all model tests PASS (3 existing + 3 new = 6 total).

**Step 6: Run full test suite to check nothing is broken**

```bash
python -m pytest tests/unit/topology/pipeline/ -v
```

Expected: all existing tests still pass (fixture now includes the new fields, so existing parse tests continue to work).

**Step 7: Commit**

```bash
git add ingestion/tests/unit/resources/datasets/ado_dump.json \
        ingestion/src/metadata/ingestion/source/pipeline/ado/models.py \
        ingestion/tests/unit/topology/pipeline/test_ado_models.py
git commit -m "feat: add git_sha and pipeline_type to AdoRun model"
```

---

## Task 2: Update ADO dump — extract `git_sha`, emit `pipeline_type`

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/ado/ado_dump.py`
- Modify: `ingestion/tests/unit/topology/pipeline/test_ado_dump.py`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S3 | GIVEN ADO API run with `resources.repositories.self.version` WHEN dumped THEN `runs.json` contains `git_sha` | ado-dump-delta.md |
| S4 | GIVEN build pipeline run WHEN dumped THEN `pipeline_type="image_build"` | ado-dump-delta.md |
| S5 | GIVEN infra deploy pipeline run WHEN dumped THEN `pipeline_type="infra_deploy"` | ado-dump-delta.md |

**Step 1: Write the failing tests**

Add to `ingestion/tests/unit/topology/pipeline/test_ado_dump.py`.

First, update `MOCK_RUNS_RESPONSE` to include the `resources` field that ADO returns with the git SHA:

```python
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
            "resources": {"repositories": {"self": {"version": "abc123def456"}}},
        },
        {
            "id": 233,
            "name": "infra-deploy-231",
            "pipeline": {"name": "infra-deploy-pipeline"},
            "result": "succeeded",
            "createdDate": "2026-02-18T02:00:00Z",
            "templateParameters": {},
            "variables": {},
            "resources": {"repositories": {"self": {"version": "abc123def456"}}},
        },
    ]
}
```

Then add the new tests:

```python
def test_runs_json_contains_git_sha(config):
    # S3: GIVEN ADO API returns runs with resources.repositories.self.version
    # WHEN dumped THEN each record in runs.json has git_sha
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = MOCK_RUNS_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()
        AdoDumper(config).dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    assert all("git_sha" in r for r in runs)
    assert runs[0]["git_sha"] == "abc123def456"

def test_runs_json_classifies_pipeline_type_build(config):
    # S4: build pipeline run → pipeline_type="image_build"
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = MOCK_RUNS_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()
        AdoDumper(config).dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    build_run = next(r for r in runs if r["id"] == 232)
    assert build_run["pipeline_type"] == "image_build"

def test_runs_json_classifies_pipeline_type_infra_deploy(config):
    # S5: infra deploy pipeline run → pipeline_type="infra_deploy"
    with patch("metadata.ingestion.source.pipeline.ado.ado_dump.requests.get") as mock_get:
        mock_get.return_value.json.return_value = MOCK_RUNS_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()
        AdoDumper(config).dump()
    runs = json.loads((config.out_dir / "runs.json").read_text())
    deploy_run = next(r for r in runs if r["id"] == 233)
    assert deploy_run["pipeline_type"] == "infra_deploy"
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_dump.py -v
```

Expected: FAIL — `_normalize_run` does not extract `git_sha` or set `pipeline_type` yet.

**Step 3: Update `_normalize_run` in `ado_dump.py`**

```python
def _normalize_run(raw: Dict) -> Dict:
    params = raw.get("templateParameters", {}) or {}
    variables = raw.get("variables", {}) or {}
    pipeline_name = raw.get("pipeline", {}).get("name", "")
    is_deploy = "deploy" in pipeline_name.lower()
    run_type = "deploy" if is_deploy else "build"
    pipeline_type = "infra_deploy" if is_deploy else "image_build"
    git_sha = (
        raw.get("resources", {})
           .get("repositories", {})
           .get("self", {})
           .get("version", "")
    )
    return {
        "id": raw["id"],
        "name": raw["name"],
        "startTime": raw.get("createdDate"),
        "result": raw.get("result"),
        "type": run_type,
        "pipeline_type": pipeline_type,
        "git_sha": git_sha,
        "image": params.get("image_tag") or variables.get("image_tag", {}).get("value"),
        "version": params.get("version") or variables.get("version", {}).get("value"),
        "env": params.get("env") or variables.get("env", {}).get("value"),
    }
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_dump.py -v
```

Expected: all dump tests PASS (3 existing + 3 new = 6 total).

**Step 5: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/ado/ado_dump.py \
        ingestion/tests/unit/topology/pipeline/test_ado_dump.py
git commit -m "feat: extract git_sha and pipeline_type in ado_dump.py"
```

---

## Task 3: Update ADO connector — store `git_sha` + `pipeline_type` as tags

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/pipeline/ado/metadata.py`
- Modify: `ingestion/tests/unit/topology/pipeline/test_ado_metadata.py`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S6 | GIVEN build run with `git_sha` WHEN connector runs THEN Pipeline entity tag includes `git_sha=abc123def456` | ado-connector-delta.md |
| S7 | GIVEN build run WHEN connector runs THEN entity tag includes `pipeline_type=image_build` | ado-connector-delta.md |
| S8 | GIVEN deploy run WHEN connector runs THEN entity tag includes `pipeline_type=infra_deploy` | ado-connector-delta.md |

**Step 1: Write the failing tests**

Add to `ingestion/tests/unit/topology/pipeline/test_ado_metadata.py`:

```python
def test_build_run_has_git_sha_tag(config, mock_om):
    # S6: GIVEN build run with git_sha WHEN connector builds requests
    # THEN tags include git_sha value
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    build_req = next(r for r in requests if "232" in str(r.name))
    tag_strs = [str(t) for t in (build_req.tags or [])]
    assert any("abc123def456" in t for t in tag_strs)

def test_build_run_has_pipeline_type_image_build_tag(config, mock_om):
    # S7: GIVEN build run WHEN connector builds requests
    # THEN tags include pipeline_type=image_build
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    build_req = next(r for r in requests if "232" in str(r.name))
    tag_strs = [str(t) for t in (build_req.tags or [])]
    assert any("image_build" in t for t in tag_strs)

def test_deploy_run_has_pipeline_type_infra_deploy_tag(config, mock_om):
    # S8: GIVEN deploy run WHEN connector builds requests
    # THEN tags include pipeline_type=infra_deploy
    from metadata.ingestion.source.pipeline.ado.metadata import AdoConnector
    connector = AdoConnector(config=config, metadata=mock_om)
    requests = connector.build_pipeline_requests()
    deploy_req = next(r for r in requests if "233" in str(r.name))
    tag_strs = [str(t) for t in (deploy_req.tags or [])]
    assert any("infra_deploy" in t for t in tag_strs)
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_metadata.py -v
```

Expected: FAIL — `_build_one` does not include `git_sha` or `pipeline_type` in tags yet.

**Step 3: Update `_build_one` in `ado/metadata.py`**

```python
def _build_one(self, run: AdoRun) -> AdoPipelineRequest:
    tags = [
        f"run_id={run.id}",
        f"result={run.result or 'unknown'}",
        f"git_sha={run.git_sha}",
        f"pipeline_type={run.pipeline_type}",
    ]

    if run.run_type == RunType.BUILD:
        tags.append(f"image={run.image or 'unknown'}")
    else:
        tags.append(f"version={run.version or 'unknown'}")
        tags.append(f"env={run.env or 'unknown'}")
        approval = self._approvals.get(run.id)
        if approval:
            tags.append(f"approved_by={approval.approver}")
            tags.append(f"approved_at={approval.approved_at}")

    return AdoPipelineRequest(name=str(run.id), tags=tags)
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/unit/topology/pipeline/test_ado_metadata.py -v
```

Expected: all metadata tests PASS (7 existing + 3 new = 10 total).

**Step 5: Run full suite**

```bash
python -m pytest tests/unit/topology/pipeline/ -v
```

Expected: all tests PASS.

**Step 6: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/ado/metadata.py \
        ingestion/tests/unit/topology/pipeline/test_ado_metadata.py
git commit -m "feat: store git_sha and pipeline_type tags on ADO Pipeline entities"
```

---

## Task 4: New `dag_enricher.py` — join deploy refs onto Airflow DAG entities

**Files:**
- Create: `ingestion/src/metadata/ingestion/source/pipeline/dag_enricher.py`
- Create: `ingestion/tests/unit/topology/pipeline/test_dag_enricher.py`

**Scenarios:**
| ID | Scenario | Source |
|----|----------|--------|
| S9  | GIVEN builds + DAG run WHEN enrich THEN `image_build_ref` set to most recent build FQN | dag-enricher-delta.md |
| S10 | GIVEN infras + DAG run WHEN enrich THEN `infra_deploy_ref` set to most recent infra FQN | dag-enricher-delta.md |
| S11 | GIVEN active build has git_sha WHEN enrich THEN DAG entity `git_sha` matches build's | dag-enricher-delta.md |
| S12 | GIVEN DAG has PipelineStatus.endDate WHEN enrich THEN `run_end_timestamp` set to that ISO string | dag-enricher-delta.md |
| S13 | GIVEN builds on 2026-02-01 and 2026-02-18, DAG run 2026-02-20 WHEN enrich THEN picks 2026-02-18 | dag-enricher-delta.md |
| S14 | GIVEN same enrichment run twice WHEN enrich THEN patch_custom_properties called with same values | dag-enricher-delta.md |
| S15 | GIVEN no build before DAG run WHEN enrich THEN DAG skipped, warning logged, others continue | dag-enricher-delta.md |
| S16 | GIVEN DAG has no PipelineStatus WHEN enrich THEN DAG skipped silently | dag-enricher-delta.md |

**Step 1: Write the failing tests**

Create `ingestion/tests/unit/topology/pipeline/test_dag_enricher.py`:

```python
"""Tests for dag_enricher.py."""
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock, call

import pytest


# ---------------------------------------------------------------------------
# Minimal data structures that mirror what the enricher works with
# ---------------------------------------------------------------------------

@dataclass
class AuditEntity:
    """Represents an ADO Pipeline entity (build or infra deploy)."""
    fqn: str
    date: datetime
    git_sha: str = ""


@dataclass
class DagRecord:
    """Represents an Airflow DAG entity with its latest run end time."""
    fqn: str
    run_end: Optional[datetime]


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_om():
    om = MagicMock()
    om.patch_custom_properties = MagicMock()
    return om


@pytest.fixture
def build_feb01():
    return AuditEntity(fqn="ADO.build-230", date=_dt("2026-02-01T00:00:00Z"), git_sha="sha_old")


@pytest.fixture
def build_feb18():
    return AuditEntity(fqn="ADO.build-232", date=_dt("2026-02-18T01:00:00Z"), git_sha="abc123def456")


@pytest.fixture
def infra_feb18():
    return AuditEntity(fqn="ADO.infra-deploy-233", date=_dt("2026-02-18T02:00:00Z"), git_sha="abc123def456")


@pytest.fixture
def dag_feb20():
    return DagRecord(fqn="airflow_pipeline.aci_sf_encrypted_pipeline",
                     run_end=_dt("2026-02-20T14:32:00Z"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_enrich_sets_image_build_ref(mock_om, build_feb18, infra_feb18, dag_feb20):
    # S9: GIVEN builds and a DAG run WHEN enrich THEN image_build_ref set
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    DagEnricher(mock_om).enrich([build_feb18], [infra_feb18], [dag_feb20])
    mock_om.patch_custom_properties.assert_called_once()
    props = mock_om.patch_custom_properties.call_args[0][1]
    assert props["image_build_ref"] == "ADO.build-232"


def test_enrich_sets_infra_deploy_ref(mock_om, build_feb18, infra_feb18, dag_feb20):
    # S10: GIVEN infras and a DAG run WHEN enrich THEN infra_deploy_ref set
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    DagEnricher(mock_om).enrich([build_feb18], [infra_feb18], [dag_feb20])
    props = mock_om.patch_custom_properties.call_args[0][1]
    assert props["infra_deploy_ref"] == "ADO.infra-deploy-233"


def test_enrich_propagates_git_sha(mock_om, build_feb18, infra_feb18, dag_feb20):
    # S11: GIVEN active build has git_sha WHEN enrich THEN DAG entity git_sha matches
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    DagEnricher(mock_om).enrich([build_feb18], [infra_feb18], [dag_feb20])
    props = mock_om.patch_custom_properties.call_args[0][1]
    assert props["git_sha"] == "abc123def456"


def test_enrich_sets_run_end_timestamp(mock_om, build_feb18, infra_feb18, dag_feb20):
    # S12: GIVEN DAG has run_end WHEN enrich THEN run_end_timestamp is ISO string
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    DagEnricher(mock_om).enrich([build_feb18], [infra_feb18], [dag_feb20])
    props = mock_om.patch_custom_properties.call_args[0][1]
    assert props["run_end_timestamp"] == "2026-02-20T14:32:00+00:00"


def test_enrich_picks_most_recent_build_before_dag_run(mock_om, build_feb01, build_feb18, infra_feb18, dag_feb20):
    # S13: GIVEN builds on 2026-02-01 and 2026-02-18, DAG run 2026-02-20
    # THEN picks the 2026-02-18 build (not 2026-02-01)
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    DagEnricher(mock_om).enrich([build_feb01, build_feb18], [infra_feb18], [dag_feb20])
    props = mock_om.patch_custom_properties.call_args[0][1]
    assert props["image_build_ref"] == "ADO.build-232"


def test_enrich_is_idempotent(mock_om, build_feb18, infra_feb18, dag_feb20):
    # S14: GIVEN enricher run twice WHEN enrich THEN same props patched both times
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    enricher = DagEnricher(mock_om)
    enricher.enrich([build_feb18], [infra_feb18], [dag_feb20])
    enricher.enrich([build_feb18], [infra_feb18], [dag_feb20])
    assert mock_om.patch_custom_properties.call_count == 2
    first_props = mock_om.patch_custom_properties.call_args_list[0][0][1]
    second_props = mock_om.patch_custom_properties.call_args_list[1][0][1]
    assert first_props == second_props


def test_enrich_skips_dag_with_no_prior_build(mock_om, build_feb18, infra_feb18, caplog):
    # S15: GIVEN no build before DAG run date WHEN enrich THEN DAG skipped + warning logged
    early_dag = DagRecord(fqn="airflow_pipeline.early_dag",
                          run_end=_dt("2026-01-01T00:00:00Z"))
    late_dag = DagRecord(fqn="airflow_pipeline.late_dag",
                         run_end=_dt("2026-02-20T14:32:00Z"))
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    with caplog.at_level(logging.WARNING):
        DagEnricher(mock_om).enrich([build_feb18], [infra_feb18], [early_dag, late_dag])
    # early_dag skipped (warning), late_dag enriched
    assert mock_om.patch_custom_properties.call_count == 1
    fqn_patched = mock_om.patch_custom_properties.call_args[0][0]
    assert fqn_patched == "airflow_pipeline.late_dag"
    assert any("early_dag" in r.message or "no image build" in r.message.lower()
               for r in caplog.records)


def test_enrich_skips_dag_with_no_run_history(mock_om, build_feb18, infra_feb18):
    # S16: GIVEN DAG with run_end=None WHEN enrich THEN skipped silently
    no_history_dag = DagRecord(fqn="airflow_pipeline.never_ran", run_end=None)
    dag_ran = DagRecord(fqn="airflow_pipeline.did_run",
                        run_end=_dt("2026-02-20T14:32:00Z"))
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    DagEnricher(mock_om).enrich([build_feb18], [infra_feb18], [no_history_dag, dag_ran])
    # Only dag_ran was patched
    assert mock_om.patch_custom_properties.call_count == 1
    fqn_patched = mock_om.patch_custom_properties.call_args[0][0]
    assert fqn_patched == "airflow_pipeline.did_run"
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/unit/topology/pipeline/test_dag_enricher.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'metadata.ingestion.source.pipeline.dag_enricher'`.

**Step 3: Implement `dag_enricher.py`**

Create `ingestion/src/metadata/ingestion/source/pipeline/dag_enricher.py`:

```python
"""dag_enricher.py — stamps Airflow DAG entities with deployment join keys.

For each Airflow DAG entity that has a run history, finds:
  - the most recent ADO image build active on or before the DAG run date
  - the most recent ADO infra deploy active on or before the DAG run date

Then patches four custom properties onto the DAG entity:
  image_build_ref    FQN of the active ADO build Pipeline
  infra_deploy_ref   FQN of the active ADO infra deploy Pipeline (if any)
  git_sha            propagated from the active build entity
  run_end_timestamp  ISO-8601 string; use as Delta AS OF TIMESTAMP value
"""
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class AuditEntity:
    """An ADO Pipeline entity (image build or infra deploy) with its run date."""
    fqn: str
    date: datetime
    git_sha: str = ""


@dataclass
class DagRecord:
    """An Airflow DAG entity with the end timestamp of its most recent run."""
    fqn: str
    run_end: Optional[datetime]


class DagEnricher:
    def __init__(self, metadata: Any):
        self.metadata = metadata

    def enrich(
        self,
        builds: List[AuditEntity],
        infras: List[AuditEntity],
        dags: List[DagRecord],
    ) -> None:
        """Core logic: enriches DAG entities given pre-loaded entity lists."""
        for dag in dags:
            if not dag.run_end:
                continue

            active_build = self._latest_before(builds, dag.run_end)
            if not active_build:
                logger.warning(
                    "No image build found on or before %s for DAG %s — skipping",
                    dag.run_end.isoformat(),
                    dag.fqn,
                )
                continue

            active_infra = self._latest_before(infras, dag.run_end)

            props: dict = {
                "image_build_ref": active_build.fqn,
                "git_sha": active_build.git_sha,
                "run_end_timestamp": dag.run_end.isoformat(),
            }
            if active_infra:
                props["infra_deploy_ref"] = active_infra.fqn

            self.metadata.patch_custom_properties(dag.fqn, props)
            logger.info("Enriched DAG %s with build=%s", dag.fqn, active_build.fqn)

    def _latest_before(
        self, entities: List[AuditEntity], cutoff: datetime
    ) -> Optional[AuditEntity]:
        candidates = [e for e in entities if e.date <= cutoff]
        return max(candidates, key=lambda e: e.date) if candidates else None

    def run(
        self,
        ado_service: str = "ADO",
        airflow_service: str = "airflow_pipeline",
    ) -> None:
        """Entry point when running against a live OM instance."""
        builds, infras = self._load_ado_entities(ado_service)
        dags = self._load_airflow_dags(airflow_service)
        self.enrich(builds, infras, dags)

    def _load_ado_entities(self, service: str):
        """Load ADO Pipeline entities from OM, split into builds and infras."""
        # Placeholder: real implementation queries OM API and filters by pipeline_type tag
        raise NotImplementedError("Implement when integrating with live OM")

    def _load_airflow_dags(self, service: str) -> List[DagRecord]:
        """Load Airflow DAG entities with their latest PipelineStatus.endDate."""
        # Placeholder: real implementation queries OM API
        raise NotImplementedError("Implement when integrating with live OM")


def main() -> None:
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

    server_config = OpenMetadataConnection(
        hostPort=host,
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=token),
    )
    metadata = OpenMetadata(server_config)
    enricher = DagEnricher(metadata)
    try:
        enricher.run()
    except Exception as exc:
        logger.error("dag_enricher failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/unit/topology/pipeline/test_dag_enricher.py -v
```

Expected: all 8 DAG enricher tests PASS.

**Step 5: Run full test suite**

```bash
python -m pytest tests/unit/topology/pipeline/ -v
```

Expected: all tests PASS (existing + new = full suite green).

**Step 6: Commit**

```bash
git add ingestion/src/metadata/ingestion/source/pipeline/dag_enricher.py \
        ingestion/tests/unit/topology/pipeline/test_dag_enricher.py
git commit -m "feat: add dag_enricher.py — stamps Airflow DAG entities with deployment join keys"
```

---

## Final Verification

```bash
python -m pytest tests/unit/topology/pipeline/ -v --tb=short
```

Expected output summary:
```
test_aci_models.py          3 passed
test_aci_metadata.py        4 passed
test_ado_models.py          6 passed   (3 existing + 3 new)
test_ado_metadata.py       10 passed   (7 existing + 3 new)
test_ado_dump.py            6 passed   (3 existing + 3 new)
test_stitch_lineage.py      5 passed
test_dag_enricher.py        8 passed   (all new)
```

Total: 42 tests, all green.

---

## What's Not in This Plan (Intentional)

- `_load_ado_entities` / `_load_airflow_dags` in `dag_enricher.py` are stubs — they raise `NotImplementedError`. Real OM query logic is wired in Stage 2 when the enricher runs inside an Airflow DAG with a live OM connection. The `smoke_ingest.py` pattern from Stage 1 shows how to query entities directly.
- No changes to `service_spec.py` files — enricher runs as a standalone script, not as an OM connector.
- No changes to existing `stitch_lineage.py`.
