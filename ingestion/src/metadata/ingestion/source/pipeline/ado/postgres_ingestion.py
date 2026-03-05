"""Incremental ADO → Postgres ingestion helper."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from metadata.ingestion.source.pipeline.ado.models import AdoRun

logger = logging.getLogger(__name__)

DEFAULT_INGESTED_BY = "ado_postgres_ingestion"


def map_run_to_row(
    run: AdoRun,
    project: str,
    raw: Dict[str, Any],
    *,
    ingested_at: datetime,
    ingested_by: str = DEFAULT_INGESTED_BY,
    approved_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Map an AdoRun into the ado_pipeline_runs row shape."""
    return {
        "run_id": run.id,
        "ado_project": project,
        "pipeline_type": run.run_type.value,
        "pipeline_subtype": run.pipeline_type,
        "queue_time": run.start_time,
        "start_time": run.start_time,
        "result": run.result,
        "env": run.env,
        "version": run.version,
        "image_tag": run.image,
        "git_sha": run.git_sha,
        "approved_by": approved_by,
        "raw_data": raw,
        "ingested_at": ingested_at,
        "ingested_by": ingested_by,
    }


class AdoPostgresIngestor:
    """Ingests ADO pipeline runs into Postgres with incremental upsert."""

    def __init__(
        self,
        api_client: Any,
        repo: Any,
        project: str,
        *,
        approvals_client: Optional[Any] = None,
        lookback_days: int = 30,
        dry_run: bool = False,
        ingested_by: str = DEFAULT_INGESTED_BY,
        now: Optional[callable] = None,
    ):
        self.api_client = api_client
        self.repo = repo
        self.project = project
        self.approvals_client = approvals_client
        self.lookback_days = lookback_days
        self.dry_run = dry_run
        self.ingested_by = ingested_by
        self._now = now or (lambda: datetime.now(timezone.utc))

    def _since_timestamp(self) -> datetime:
        existing_max = self.repo.max_queue_time()
        if existing_max:
            return existing_max
        return self._now() - timedelta(days=self.lookback_days)

    def ingest(self) -> int:
        since = self._since_timestamp()
        approvals: Dict[int, Dict[str, Any]] = {}
        if self.approvals_client:
            try:
                approvals = self.approvals_client.fetch()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Could not fetch approvals: %s", exc)

        raw_runs: List[Dict[str, Any]] = self.api_client.fetch_runs(since)
        rows: List[Dict[str, Any]] = []

        for raw in raw_runs:
            try:
                run = AdoRun.model_validate(raw)
            except ValidationError:
                logger.warning("Skipping malformed ADO run: %s", raw)
                continue

            # Incremental filter: only queue_time/start_time newer than since
            if run.start_time and run.start_time <= since:
                continue

            approval = approvals.get(run.id) if approvals else None
            approved_by = None
            if approval:
                approved_by = approval.get("approver") or approval.get("approved_by")

            rows.append(
                map_run_to_row(
                    run=run,
                    project=self.project,
                    raw=raw,
                    ingested_at=self._now(),
                    ingested_by=self.ingested_by,
                    approved_by=approved_by,
                )
            )

        if self.dry_run:
            for row in rows:
                print(row)
            return 0

        for row in rows:
            self.repo.upsert(row)
        return len(rows)
