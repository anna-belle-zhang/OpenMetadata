"""Incremental Airflow → Postgres ingestion helper."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ValidationError

DEFAULT_INGESTED_BY = "airflow_postgres_ingestion"


class AirflowDagRun(BaseModel):
    dag_run_id: str
    dag_id: str
    execution_date: datetime
    state: Optional[str] = None
    end_date: Optional[datetime] = None
    duration: Optional[float] = None

    model_config = {"populate_by_name": True}


def map_dag_run_to_row(
    run: AirflowDagRun,
    airflow_instance: str,
    raw: Dict[str, Any],
    *,
    ingested_at: datetime,
    ingested_by: str = DEFAULT_INGESTED_BY,
) -> Dict[str, Any]:
    duration_seconds = run.duration
    if duration_seconds is None and run.end_date:
        duration_seconds = (run.end_date - run.execution_date).total_seconds()

    return {
        "dag_run_id": run.dag_run_id,
        "dag_id": run.dag_id,
        "airflow_instance": airflow_instance,
        "execution_date": run.execution_date,
        "state": run.state,
        "end_date": run.end_date,
        "duration_seconds": duration_seconds,
        "raw_data": raw,
        "ingested_at": ingested_at,
        "ingested_by": ingested_by,
    }


class AirflowPostgresIngestor:
    """Ingests Airflow DAG runs into Postgres with incremental upsert."""

    def __init__(
        self,
        api_client: Any,
        repo: Any,
        airflow_instance: str,
        *,
        base_url: Optional[str] = None,
        lookback_days: int = 30,
        dry_run: bool = False,
        ingested_by: str = DEFAULT_INGESTED_BY,
        now: Optional[callable] = None,
    ):
        self.api_client = api_client
        self.repo = repo
        self.airflow_instance = airflow_instance
        self.lookback_days = lookback_days
        self.dry_run = dry_run
        self.ingested_by = ingested_by
        self._now = now or (lambda: datetime.now(timezone.utc))
        base = base_url or os.environ.get("AIRFLOW_BASE_URL") or "http://localhost:8080"
        self.base_url = f"{base.rstrip('/')}/api/v1/dags"

    def _since_timestamp(self) -> datetime:
        existing_max = self.repo.max_execution_date()
        if existing_max:
            return existing_max
        return self._now() - timedelta(days=self.lookback_days)

    def ingest(self) -> int:
        since = self._since_timestamp()
        raw_runs: List[Dict[str, Any]] = self.api_client.fetch_runs(since)
        rows: List[Dict[str, Any]] = []

        for raw in raw_runs:
            try:
                run = AirflowDagRun.model_validate(raw)
            except ValidationError:
                continue

            if run.execution_date and run.execution_date <= since:
                continue

            rows.append(
                map_dag_run_to_row(
                    run=run,
                    airflow_instance=self.airflow_instance,
                    raw=raw,
                    ingested_at=self._now(),
                    ingested_by=self.ingested_by,
                )
            )

        if self.dry_run:
            for row in rows:
                print(row)
            return 0

        for row in rows:
            self.repo.upsert(row)
        return len(rows)
