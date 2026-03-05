"""Tests for dag_enricher.py."""
import logging
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from metadata.ingestion.source.pipeline.dag_enricher import AuditEntity, DagRecord


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
    # S9: GIVEN builds and a DAG run WHEN enrich THEN image_build_ref set to build FQN
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
    # S11: GIVEN active build has git_sha WHEN enrich THEN DAG git_sha matches build's
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
    # S14: GIVEN enricher run twice with same data WHEN enrich THEN same props both times
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    enricher = DagEnricher(mock_om)
    enricher.enrich([build_feb18], [infra_feb18], [dag_feb20])
    enricher.enrich([build_feb18], [infra_feb18], [dag_feb20])
    assert mock_om.patch_custom_properties.call_count == 2
    first_props = mock_om.patch_custom_properties.call_args_list[0][0][1]
    second_props = mock_om.patch_custom_properties.call_args_list[1][0][1]
    assert first_props == second_props


def test_enrich_skips_dag_with_no_prior_build(mock_om, build_feb18, infra_feb18, caplog):
    # S15: GIVEN no build before DAG run date WHEN enrich THEN DAG skipped + warning + others continue
    early_dag = DagRecord(fqn="airflow_pipeline.early_dag",
                          run_end=_dt("2026-01-01T00:00:00Z"))
    late_dag = DagRecord(fqn="airflow_pipeline.late_dag",
                         run_end=_dt("2026-02-20T14:32:00Z"))
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    with caplog.at_level(logging.WARNING):
        DagEnricher(mock_om).enrich([build_feb18], [infra_feb18], [early_dag, late_dag])
    assert mock_om.patch_custom_properties.call_count == 1
    fqn_patched = mock_om.patch_custom_properties.call_args[0][0]
    assert fqn_patched == "airflow_pipeline.late_dag"
    assert len(caplog.records) >= 1


def test_enrich_skips_dag_with_no_run_history(mock_om, build_feb18, infra_feb18):
    # S16: GIVEN DAG with run_end=None WHEN enrich THEN skipped silently
    no_history_dag = DagRecord(fqn="airflow_pipeline.never_ran", run_end=None)
    dag_ran = DagRecord(fqn="airflow_pipeline.did_run",
                        run_end=_dt("2026-02-20T14:32:00Z"))
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher
    DagEnricher(mock_om).enrich([build_feb18], [infra_feb18], [no_history_dag, dag_ran])
    assert mock_om.patch_custom_properties.call_count == 1
    fqn_patched = mock_om.patch_custom_properties.call_args[0][0]
    assert fqn_patched == "airflow_pipeline.did_run"


# ---------------------------------------------------------------------------
# Audit log tests (A1-A3) — Task 1
# ---------------------------------------------------------------------------

import json


def _read_jsonl(path) -> list:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_enrich_appends_audit_row(tmp_path, mock_om, build_feb18, infra_feb18, dag_feb20):
    # A1: GIVEN DAG enriched WHEN enricher runs
    # THEN audit_log.jsonl has one row with all required fields
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher

    audit_path = tmp_path / "audit_log.jsonl"
    DagEnricher(mock_om, audit_log_path=audit_path).enrich(
        [build_feb18], [infra_feb18], [dag_feb20]
    )

    rows = _read_jsonl(audit_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["audit_date"] == "2026-02-20"
    assert row["dag_fqn"] == "airflow_pipeline.aci_sf_encrypted_pipeline"
    assert row["image_build_ref"] == "ADO.build-232"
    assert row["git_sha"] == "abc123def456"
    assert row["infra_deploy_ref"] == "ADO.infra-deploy-233"
    assert "run_end_timestamp" in row


def test_enrich_no_duplicate_row_on_rerun(tmp_path, mock_om, build_feb18, infra_feb18, dag_feb20):
    # A2: GIVEN (audit_date, dag_fqn) already in jsonl
    # WHEN enricher runs again THEN still only one row (no duplicate)
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher

    audit_path = tmp_path / "audit_log.jsonl"
    enricher = DagEnricher(mock_om, audit_log_path=audit_path)
    enricher.enrich([build_feb18], [infra_feb18], [dag_feb20])
    enricher.enrich([build_feb18], [infra_feb18], [dag_feb20])

    rows = _read_jsonl(audit_path)
    assert len(rows) == 1


def test_enrich_two_dags_two_rows(tmp_path, mock_om, build_feb18, infra_feb18):
    # A3: GIVEN two DAGs run on the same date
    # WHEN enricher processes both THEN two rows in jsonl
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher, DagRecord

    audit_path = tmp_path / "audit_log.jsonl"
    dag_a = DagRecord(fqn="airflow_pipeline.dag_a", run_end=_dt("2026-02-20T14:00:00Z"))
    dag_b = DagRecord(fqn="airflow_pipeline.dag_b", run_end=_dt("2026-02-20T15:00:00Z"))

    DagEnricher(mock_om, audit_log_path=audit_path).enrich(
        [build_feb18], [infra_feb18], [dag_a, dag_b]
    )

    rows = _read_jsonl(audit_path)
    assert len(rows) == 2
    fqns = {r["dag_fqn"] for r in rows}
    assert fqns == {"airflow_pipeline.dag_a", "airflow_pipeline.dag_b"}


# ---------------------------------------------------------------------------
# approved_by / env wiring tests (C1-C3) — Task 3
# ---------------------------------------------------------------------------

def test_audit_row_contains_approved_by(tmp_path, mock_om, build_feb18, dag_feb20):
    # C1: GIVEN infra entity has approved_by WHEN appended THEN row has approved_by
    from metadata.ingestion.source.pipeline.dag_enricher import AuditEntity, DagEnricher

    infra_with_approver = AuditEntity(
        fqn="ADO.infra-deploy-233",
        date=_dt("2026-02-18T02:00:00Z"),
        git_sha="abc123def456",
        approved_by="Abner Zhang",
        env="prd",
    )
    audit_path = tmp_path / "audit_log.jsonl"
    DagEnricher(mock_om, audit_log_path=audit_path).enrich(
        [build_feb18], [infra_with_approver], [dag_feb20]
    )
    rows = _read_jsonl(audit_path)
    assert rows[0]["approved_by"] == "Abner Zhang"


def test_audit_row_contains_env(tmp_path, mock_om, build_feb18, dag_feb20):
    # C2: GIVEN infra entity has env WHEN appended THEN row has env
    from metadata.ingestion.source.pipeline.dag_enricher import AuditEntity, DagEnricher

    infra_with_env = AuditEntity(
        fqn="ADO.infra-deploy-233",
        date=_dt("2026-02-18T02:00:00Z"),
        git_sha="abc123def456",
        approved_by="Abner Zhang",
        env="prd",
    )
    audit_path = tmp_path / "audit_log.jsonl"
    DagEnricher(mock_om, audit_log_path=audit_path).enrich(
        [build_feb18], [infra_with_env], [dag_feb20]
    )
    rows = _read_jsonl(audit_path)
    assert rows[0]["env"] == "prd"


def test_audit_row_no_infra_approved_by_null(tmp_path, mock_om, build_feb18, dag_feb20):
    # C3: GIVEN no infra WHEN appended THEN approved_by and env are null
    from metadata.ingestion.source.pipeline.dag_enricher import DagEnricher

    audit_path = tmp_path / "audit_log.jsonl"
    DagEnricher(mock_om, audit_log_path=audit_path).enrich(
        [build_feb18], [], [dag_feb20]
    )
    rows = _read_jsonl(audit_path)
    assert rows[0]["approved_by"] is None
    assert rows[0]["env"] is None
