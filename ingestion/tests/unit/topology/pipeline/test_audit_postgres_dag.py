"""Tests for audit_postgres_pipeline DAG behaviors."""
import importlib
import os
from pathlib import Path

import pytest

from metadata.ingestion.source.pipeline.ado import audit_postgres_pipeline as dag_mod


class DummyRepo:
    def __init__(self):
        self.rows = []

    def max_queue_time(self):
        return None

    def max_execution_date(self):
        return None

    def upsert(self, row):
        self.rows.append(row)


class DummyApi:
    def __init__(self):
        self.called = False

    def fetch_runs(self, since):
        self.called = True
        return []


class DummyOM:
    def __init__(self):
        self.calls = 0

    def ensure_entities(self):
        self.calls += 1


def test_tasks_run_in_order_and_have_retries():
    importlib.reload(dag_mod)
    dag = dag_mod.dag
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == ["setup_om_entities", "ado_ingest", "airflow_ingest", "refresh_mv", "daily_report"]
    # dependency chain
    edges = [(t.task_id, list(t.downstream_task_ids)) for t in dag.tasks]
    assert edges[0][1] == ["ado_ingest"]
    assert edges[1][1] == ["airflow_ingest"]
    assert edges[2][1] == ["refresh_mv"]
    assert edges[3][1] == ["daily_report"]
    for task in dag.tasks:
        assert task.retries == 2
        assert int(task.retry_delay.total_seconds()) == 300


def test_dry_run_propagates(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DRY_RUN", "true")
    repo = DummyRepo()
    api = DummyApi()
    dag_mod.run_ado_ingestion(api_client=api, repo=repo, project="proj")
    dag_mod.run_airflow_ingestion(api_client=api, repo=repo, airflow_instance="self")
    assert repo.rows == []
    assert api.called  # API still invoked in dry-run

    dag_mod.generate_daily_report(output_dir=tmp_path)
    assert not list(tmp_path.glob("audit-*.md"))
    monkeypatch.delenv("DRY_RUN")


def test_setup_om_entities_is_idempotent():
    om = DummyOM()
    dag_mod.setup_om_entities(om_client=om)
    dag_mod.setup_om_entities(om_client=om)
    assert om.calls == 1
