"""Unit tests for Airflow → Postgres ingestion helper."""
import datetime as dt
import os
from typing import List, Optional

import pytest

from metadata.ingestion.source.pipeline.ado.airflow_postgres_ingestion import (
    AirflowPostgresIngestor,
)


class FakeAirflowApi:
    def __init__(self, runs: List[dict], base_url: Optional[str] = None):
        self._runs = runs
        self.called_since: Optional[dt.datetime] = None
        self.base_url = base_url or "http://localhost:8080/api/v1/dags"

    def fetch_runs(self, since: dt.datetime) -> List[dict]:
        self.called_since = since
        return self._runs


class FakeRepo:
    def __init__(self, rows: Optional[List[dict]] = None):
        self.rows: List[dict] = rows or []

    def max_execution_date(self) -> Optional[dt.datetime]:
        if not self.rows:
            return None
        return max(row["execution_date"] for row in self.rows if row.get("execution_date"))

    def upsert(self, row: dict) -> None:
        existing = next(
            (
                r
                for r in self.rows
                if r["dag_run_id"] == row["dag_run_id"]
                and r["dag_id"] == row["dag_id"]
                and r["airflow_instance"] == row["airflow_instance"]
            ),
            None,
        )
        if existing:
            for key in ("state", "end_date", "duration_seconds", "raw_data", "ingested_at"):
                existing[key] = row.get(key)
        else:
            self.rows.append(row)


def _dag_run(run_id: str, dag_id: str, execution_date: str, state: str = "success") -> dict:
    return {
        "dag_run_id": run_id,
        "dag_id": dag_id,
        "execution_date": execution_date,
        "state": state,
    }


def test_incremental_uses_max_execution_date():
    existing_time = dt.datetime(2026, 2, 18, 2, 0, tzinfo=dt.timezone.utc)
    repo = FakeRepo(
        rows=[
            {
                "dag_run_id": "1",
                "dag_id": "example",
                "airflow_instance": "prod-airflow",
                "execution_date": existing_time,
                "state": "success",
                "raw_data": {},
                "duration_seconds": 10,
                "ingested_at": existing_time,
            }
        ]
    )
    api = FakeAirflowApi(
        runs=[
            _dag_run("2", "example", "2026-02-18T02:00:00+00:00"),
            _dag_run("3", "example", "2026-02-18T02:05:00+00:00"),
        ],
        base_url="http://prod-airflow:8080/api/v1/dags",
    )
    ingestor = AirflowPostgresIngestor(api_client=api, repo=repo, airflow_instance="prod-airflow")
    ingestor.ingest()

    assert api.called_since == existing_time
    run_ids = {row["dag_run_id"] for row in repo.rows}
    assert "3" in run_ids and "2" not in run_ids


def test_fallback_uses_lookback_when_empty():
    now = dt.datetime(2026, 3, 5, 0, 0, tzinfo=dt.timezone.utc)
    api = FakeAirflowApi(runs=[_dag_run("1", "example", "2026-03-04T00:00:00+00:00")])
    repo = FakeRepo()
    ingestor = AirflowPostgresIngestor(
        api_client=api,
        repo=repo,
        airflow_instance="self",
        lookback_days=5,
        now=lambda: now,
    )
    ingestor.ingest()
    assert api.called_since == now - dt.timedelta(days=5)


def test_configurable_airflow_base_url(monkeypatch):
    monkeypatch.setenv("AIRFLOW_BASE_URL", "http://prod-airflow:8080")
    api = FakeAirflowApi(runs=[])
    repo = FakeRepo()
    ingestor = AirflowPostgresIngestor(api_client=api, repo=repo, airflow_instance="prod-airflow")
    assert ingestor.base_url.startswith("http://prod-airflow:8080")
    monkeypatch.delenv("AIRFLOW_BASE_URL")


def test_defaults_to_localhost_when_base_url_not_set():
    os.environ.pop("AIRFLOW_BASE_URL", None)
    api = FakeAirflowApi(runs=[])
    repo = FakeRepo()
    ingestor = AirflowPostgresIngestor(api_client=api, repo=repo, airflow_instance="self")
    assert ingestor.base_url.startswith("http://localhost:8080")


def test_malformed_dag_run_is_skipped():
    bad = {"dag_id": "example", "execution_date": "2026-02-18T02:00:00Z"}  # missing dag_run_id
    good = _dag_run("good", "example", "2026-02-18T02:05:00Z")
    api = FakeAirflowApi(runs=[bad, good])
    repo = FakeRepo()
    ingestor = AirflowPostgresIngestor(api_client=api, repo=repo, airflow_instance="prod")
    ingestor.ingest()
    assert {r["dag_run_id"] for r in repo.rows} == {"good"}


def test_dry_run_prints_rows_and_writes_nothing(capsys):
    api = FakeAirflowApi(runs=[_dag_run("dry", "example", "2026-02-18T02:00:00Z")])
    repo = FakeRepo()
    ingestor = AirflowPostgresIngestor(api_client=api, repo=repo, airflow_instance="prod", dry_run=True)
    ingestor.ingest()
    assert repo.rows == []
    out = capsys.readouterr().out
    assert "dag_run_id" in out and "pipeline_type" not in out  # sanity check output


def test_upsert_idempotent_updates_mutable_fields():
    existing = {
        "dag_run_id": "44",
        "dag_id": "example",
        "airflow_instance": "prod-airflow",
        "execution_date": dt.datetime(2026, 2, 18, 2, 0, tzinfo=dt.timezone.utc),
        "state": "failed",
        "end_date": None,
        "duration_seconds": 10,
        "raw_data": {"old": True},
        "ingested_at": dt.datetime(2026, 2, 18, 2, 1, tzinfo=dt.timezone.utc),
    }
    repo = FakeRepo(rows=[existing])
    new_run = _dag_run("44", "example", "2026-02-18T02:00:01+00:00", state="success")
    new_run["end_date"] = "2026-02-18T02:10:00+00:00"
    new_run["duration"] = 12
    api = FakeAirflowApi(runs=[new_run])
    ingestor = AirflowPostgresIngestor(
        api_client=api,
        repo=repo,
        airflow_instance="prod-airflow",
        now=lambda: dt.datetime(2026, 2, 18, 3, 0, tzinfo=dt.timezone.utc),
    )
    ingestor.ingest()

    assert len(repo.rows) == 1
    row = repo.rows[0]
    assert row["state"] == "success"
    assert row["end_date"].isoformat().startswith("2026-02-18T02:10")
    assert row["duration_seconds"] == 12
    assert row["raw_data"]["dag_run_id"] == "44"
    assert row["ingested_at"] == dt.datetime(2026, 2, 18, 3, 0, tzinfo=dt.timezone.utc)
