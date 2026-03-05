"""Unit tests for ADO → Postgres ingestion helper."""
import datetime as dt
from typing import List, Optional

import pytest

from metadata.ingestion.source.pipeline.ado.postgres_ingestion import (
    AdoPostgresIngestor,
)


class FakeApiClient:
    def __init__(self, runs: List[dict]):
        self._runs = runs
        self.called_since: Optional[dt.datetime] = None

    def fetch_runs(self, since: dt.datetime) -> List[dict]:
        self.called_since = since
        return self._runs


class FakeApprovalsClient:
    def __init__(self, approvals: dict):
        self._approvals = approvals

    def fetch(self) -> dict:
        return self._approvals


class FakeRepo:
    def __init__(self, rows: Optional[List[dict]] = None):
        self.rows: List[dict] = rows or []

    def max_queue_time(self) -> Optional[dt.datetime]:
        if not self.rows:
            return None
        return max(row["queue_time"] for row in self.rows if row.get("queue_time"))

    def upsert(self, row: dict) -> None:
        existing = next(
            (r for r in self.rows if r["run_id"] == row["run_id"] and r["ado_project"] == row["ado_project"]),
            None,
        )
        if existing:
            # Update mutable fields per spec
            for key in ("result", "approved_by", "raw_data", "ingested_at"):
                existing[key] = row.get(key)
        else:
            self.rows.append(row)


def _run_dict(run_id: int, queue_time: str, run_type: str = "build", pipeline_type: str = "image_build") -> dict:
    return {
        "id": run_id,
        "name": f"run-{run_id}",
        "startTime": queue_time,
        "type": run_type,
        "pipeline_type": pipeline_type,
        "git_sha": "abc",
    }


def test_incremental_uses_max_queue_time():
    existing_time = dt.datetime(2026, 2, 18, 2, 0, tzinfo=dt.timezone.utc)
    repo = FakeRepo(
        rows=[
            {
                "run_id": 99,
                "ado_project": "proj",
                "queue_time": existing_time,
                "result": "succeeded",
                "approved_by": None,
                "raw_data": {},
                "ingested_at": existing_time,
            }
        ]
    )
    api = FakeApiClient(
        runs=[
            _run_dict(100, "2026-02-18T02:00:00Z"),
            _run_dict(101, "2026-02-18T02:05:00Z"),
        ]
    )

    ingestor = AdoPostgresIngestor(api_client=api, repo=repo, project="proj")
    ingestor.ingest()

    assert api.called_since == existing_time
    # Only run with queue_time > existing_time ingested
    run_ids = {row["run_id"] for row in repo.rows}
    assert 101 in run_ids and 100 not in run_ids


def test_fallback_uses_lookback_when_table_empty():
    now = dt.datetime(2026, 3, 5, 0, 0, tzinfo=dt.timezone.utc)
    api = FakeApiClient(runs=[_run_dict(1, "2026-03-04T00:00:00Z")])
    repo = FakeRepo()
    ingestor = AdoPostgresIngestor(
        api_client=api,
        repo=repo,
        project="proj",
        lookback_days=7,
        now=lambda: now,
    )
    ingestor.ingest()
    assert api.called_since == now - dt.timedelta(days=7)


def test_mapper_transforms_run_type_and_pipeline_type():
    api = FakeApiClient(runs=[_run_dict(5, "2026-02-18T01:00:00Z", run_type="deploy", pipeline_type="infra_deploy")])
    repo = FakeRepo()
    ingestor = AdoPostgresIngestor(api_client=api, repo=repo, project="proj")
    ingestor.ingest()
    row = repo.rows[0]
    assert row["pipeline_type"] == "deploy"
    assert row["pipeline_subtype"] == "infra_deploy"


def test_malformed_run_is_skipped():
    # First run is malformed (missing id), second is valid
    bad = {"name": "missing-id", "type": "build", "startTime": "2026-02-18T01:00:00Z", "pipeline_type": "image_build", "git_sha": "abc"}
    good = _run_dict(7, "2026-02-18T02:00:00Z")
    api = FakeApiClient(runs=[bad, good])
    repo = FakeRepo()
    ingestor = AdoPostgresIngestor(api_client=api, repo=repo, project="proj")
    ingestor.ingest()
    assert {r["run_id"] for r in repo.rows} == {7}


def test_dry_run_writes_nothing_and_prints_rows(capsys):
    api = FakeApiClient(runs=[_run_dict(8, "2026-02-18T03:00:00Z")])
    repo = FakeRepo()
    ingestor = AdoPostgresIngestor(api_client=api, repo=repo, project="proj", dry_run=True)
    ingestor.ingest()
    assert repo.rows == []
    out = capsys.readouterr().out
    assert "run_id" in out and "pipeline_type" in out


def test_upsert_is_idempotent_and_updates_mutable_fields():
    existing = {
        "run_id": 50,
        "ado_project": "proj",
        "queue_time": dt.datetime(2026, 2, 18, 2, 0, tzinfo=dt.timezone.utc),
        "result": "succeeded",
        "approved_by": None,
        "raw_data": {"old": True},
        "ingested_at": dt.datetime(2026, 2, 18, 2, 1, tzinfo=dt.timezone.utc),
        "pipeline_type": "deploy",
        "pipeline_subtype": "infra_deploy",
    }
    repo = FakeRepo(rows=[existing])
    new_run = _run_dict(50, "2026-02-18T02:00:01Z", run_type="deploy", pipeline_type="infra_deploy")
    new_run["result"] = "failed"
    api = FakeApiClient(runs=[new_run])
    approvals = FakeApprovalsClient({50: {"approver": "Zoe"}})
    ingestor = AdoPostgresIngestor(
        api_client=api,
        repo=repo,
        project="proj",
        approvals_client=approvals,
        now=lambda: dt.datetime(2026, 2, 18, 3, 0, tzinfo=dt.timezone.utc),
    )
    ingestor.ingest()

    assert len(repo.rows) == 1
    row = repo.rows[0]
    assert row["result"] == "failed"
    assert row["approved_by"] == "Zoe"
    assert row["raw_data"]["name"] == "run-50"
    assert row["ingested_at"] == dt.datetime(2026, 2, 18, 3, 0, tzinfo=dt.timezone.utc)
