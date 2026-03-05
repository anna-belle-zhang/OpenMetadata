"""Delta spec tests for OM registration of audit entities."""
from metadata.ingestion.source.pipeline.ado import audit_postgres_pipeline as dag_mod


class DummyOMClient:
    def __init__(self):
        self.services = []
        self.tables = []
        self.views = []

    def create_or_update_service(self, name: str, parent_service: str):
        self.services.append((name, parent_service))

    def create_or_update_table(self, name: str, schema: str):
        self.tables.append((name, schema))

    def create_or_update_view(self, name: str, schema: str, *, owner: str = None, tier: str = None):
        self.views.append({"name": name, "schema": schema, "owner": owner, "tier": tier})


def setup_fresh_client():
    dag_mod._om_created = False
    return DummyOMClient()


def test_audit_db_service_registered_under_postgres_pipeline():
    client = setup_fresh_client()
    dag_mod.setup_om_entities(om_client=client)
    assert ("audit_db", "postgres_pipeline") in client.services


def test_tables_and_view_registered():
    client = setup_fresh_client()
    dag_mod.setup_om_entities(om_client=client)
    assert ("ado_pipeline_runs", "audit_db.audit") in client.tables
    assert ("airflow_dag_executions", "audit_db.audit") in client.tables
    assert any(v["name"] == "daily_audit_summary" and v["schema"] == "audit_db.audit" for v in client.views)


def test_daily_audit_summary_has_owner_and_tier():
    client = setup_fresh_client()
    dag_mod.setup_om_entities(om_client=client)
    view = next(v for v in client.views if v["name"] == "daily_audit_summary")
    assert view["owner"] == "DataPlatform"
    assert view["tier"] == "Gold"


def test_registration_idempotent():
    client = setup_fresh_client()
    dag_mod.setup_om_entities(om_client=client)
    dag_mod.setup_om_entities(om_client=client)
    assert len(client.services) == 1
    assert len(client.tables) == 2
    assert len(client.views) == 1
