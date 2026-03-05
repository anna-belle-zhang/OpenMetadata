"""Tests for audit_log_pusher.py."""
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

AUDIT_LOG_CONTENT = "\n".join([
    json.dumps({
        "audit_date": "2026-02-17",
        "dag_fqn": "airflow_pipeline.aci_sf_encrypted_pipeline",
        "run_end_timestamp": "2026-02-17T14:28:00Z",
        "image_build_ref": "ADO.build-230",
        "git_sha": "abc122",
        "infra_deploy_ref": "ADO.infra-deploy-233",
        "approved_by": "Abner Zhang",
        "env": "prd",
    }),
    json.dumps({
        "audit_date": "2026-02-18",
        "dag_fqn": "airflow_pipeline.aci_sf_encrypted_pipeline",
        "run_end_timestamp": "2026-02-18T14:32:00Z",
        "image_build_ref": "ADO.build-232",
        "git_sha": "abc123def456",
        "infra_deploy_ref": "ADO.infra-deploy-233",
        "approved_by": "Abner Zhang",
        "env": "prd",
    }),
])

GOLD_TABLE_FQNS = ["gold_db.gold.dim_sf_members_scd2"]


@pytest.fixture
def audit_file(tmp_path) -> Path:
    path = tmp_path / "audit_log.jsonl"
    path.write_text(AUDIT_LOG_CONTENT)
    return path


@pytest.fixture
def mock_om():
    om = MagicMock()
    om.get_or_create_table = MagicMock(return_value=MagicMock(id="table-uuid-123"))
    om.push_table_data = MagicMock()
    om.add_lineage = MagicMock()
    om.update_table_profile = MagicMock()
    return om


def test_pusher_creates_table_entity(audit_file, mock_om):
    # B1: GIVEN rows in jsonl WHEN pusher runs THEN table entity created with correct FQN
    from metadata.ingestion.source.pipeline.audit_log_pusher import AuditLogPusher

    AuditLogPusher(
        mock_om,
        audit_log_path=audit_file,
        gold_table_fqns=GOLD_TABLE_FQNS,
        owner="DataPlatform",
        tier="Tier.Gold",
        env_label="env:dev",
    ).run()

    mock_om.get_or_create_table.assert_called_once()
    call_kwargs = mock_om.get_or_create_table.call_args
    table_fqn = call_kwargs[1].get("fqn") or call_kwargs[0][0]
    assert "daily_deployment_log" in table_fqn
    assert "audit" in table_fqn
    assert call_kwargs[1]["owner"] == "DataPlatform"
    assert "Tier.Gold" in call_kwargs[1]["tags"]
    assert "env:dev" in call_kwargs[1]["tags"]
    assert call_kwargs[1]["last_updated"]


def test_pusher_pushes_all_rows_as_sample_data(audit_file, mock_om):
    # B2: GIVEN 2 rows in jsonl WHEN pusher runs THEN push_table_data called with 2 rows
    from metadata.ingestion.source.pipeline.audit_log_pusher import AuditLogPusher

    AuditLogPusher(
        mock_om,
        audit_log_path=audit_file,
        gold_table_fqns=GOLD_TABLE_FQNS,
        owner="DataPlatform",
        tier="Tier.Gold",
        env_label="env:dev",
    ).run()

    mock_om.push_table_data.assert_called_once()
    rows_pushed = mock_om.push_table_data.call_args[0][1]  # positional: (table_id, rows)
    assert len(rows_pushed) == 2


def test_pusher_posts_lineage_to_gold_tables(audit_file, mock_om):
    # B3: GIVEN one gold_table_fqn WHEN pusher runs THEN add_lineage called once
    from metadata.ingestion.source.pipeline.audit_log_pusher import AuditLogPusher

    AuditLogPusher(
        mock_om,
        audit_log_path=audit_file,
        gold_table_fqns=GOLD_TABLE_FQNS,
        owner="DataPlatform",
        tier="Tier.Gold",
        env_label="env:dev",
    ).run()

    assert mock_om.add_lineage.call_count == len(GOLD_TABLE_FQNS)
    lineage_call = mock_om.add_lineage.call_args[0][0]
    assert "daily_deployment_log" in str(lineage_call)
    assert "dim_sf_members_scd2" in str(lineage_call)


def test_pusher_is_idempotent(audit_file, mock_om):
    # B4: GIVEN pusher already ran WHEN it runs again THEN same calls, no errors
    from metadata.ingestion.source.pipeline.audit_log_pusher import AuditLogPusher

    pusher = AuditLogPusher(mock_om, audit_log_path=audit_file, gold_table_fqns=GOLD_TABLE_FQNS)
    pusher._owner = "DataPlatform"
    pusher._tier = "Tier.Gold"
    pusher._env_label = "env:dev"
    pusher.run()
    pusher.run()

    assert mock_om.get_or_create_table.call_count == 2
    assert mock_om.push_table_data.call_count == 2
    first_rows = mock_om.push_table_data.call_args_list[0][0][1]
    second_rows = mock_om.push_table_data.call_args_list[1][0][1]
    assert first_rows == second_rows


def test_pusher_passes_schema_and_row_count(audit_file, mock_om):
    from metadata.ingestion.source.pipeline.audit_log_pusher import AuditLogPusher

    pusher = AuditLogPusher(
        mock_om,
        audit_log_path=audit_file,
        gold_table_fqns=GOLD_TABLE_FQNS,
        owner="DataPlatform",
        tier="Tier.Gold",
        env_label="env:dev",
    )
    pusher.run()

    columns = mock_om.get_or_create_table.call_args[1]["columns"]
    column_names = [c["name"] for c in columns]
    assert "ingested_at" in column_names
    assert set(column_names) >= {
        "audit_date",
        "dag_fqn",
        "run_end_timestamp",
        "image_build_ref",
        "git_sha",
        "infra_deploy_ref",
        "approved_by",
        "env",
    }
    mock_om.update_table_profile.assert_called_once()
    _, kwargs = mock_om.update_table_profile.call_args
    assert kwargs["row_count"] == 2


def test_pusher_raises_on_missing_metadata(audit_file, mock_om):
    from metadata.ingestion.source.pipeline.audit_log_pusher import AuditLogPusher

    with pytest.raises(ValueError):
        AuditLogPusher(mock_om, audit_log_path=audit_file).run()


def test_pusher_surfaces_failures(audit_file, mock_om):
    from metadata.ingestion.source.pipeline.audit_log_pusher import AuditLogPusher

    mock_om.push_table_data.side_effect = RuntimeError("boom")
    pusher = AuditLogPusher(
        mock_om,
        audit_log_path=audit_file,
        gold_table_fqns=GOLD_TABLE_FQNS,
        owner="DataPlatform",
        tier="Tier.Gold",
        env_label="env:dev",
    )

    with pytest.raises(RuntimeError):
        pusher.run()
