#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""audit_log_pusher.py — publishes the daily audit log into OpenMetadata.

Reads audit_log.jsonl (written by dag_enricher.py) and:
  1. Creates or updates a Table entity "audit_db.public.daily_deployment_log" in OM.
  2. Pushes all rows as native TableData (visible in OM's "Sample Data" tab).
  3. Posts lineage edges from the audit table to each configured gold table FQN.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

AUDIT_TABLE_FQN = "audit_db.public.daily_deployment_log"
AUDIT_COLUMNS = [
    "audit_date",
    "dag_fqn",
    "run_end_timestamp",
    "image_build_ref",
    "git_sha",
    "infra_deploy_ref",
    "approved_by",
    "env",
]


class AuditLogPusher:
    def __init__(
        self,
        metadata: Any,
        audit_log_path: Optional[Path] = None,
        gold_table_fqns: Optional[List[str]] = None,
        table_fqn: str = AUDIT_TABLE_FQN,
        owner: Optional[str] = None,
        tier: Optional[str] = None,
        env_label: Optional[str] = None,
    ):
        self.metadata = metadata
        self._audit_log_path = audit_log_path or Path("audit_log.jsonl")
        self._gold_table_fqns = gold_table_fqns or []
        self._table_fqn = table_fqn
        self._owner = owner
        self._tier = tier
        self._env_label = env_label

    def run(self) -> None:
        try:
            self._assert_required_metadata()
            rows = self._read_rows()
            if not rows:
                logger.info("audit_log.jsonl is empty — nothing to push")
                return

            columns = self._infer_columns(rows)
            run_completed_at = datetime.utcnow()

            table = self.metadata.get_or_create_table(
                fqn=self._table_fqn,
                columns=columns,
                owner=self._owner,
                tags=[self._tier, self._env_label],
                last_updated=run_completed_at,
            )

            self.metadata.push_table_data(table.id, rows)
            logger.info("Pushed %d audit rows to OM table %s", len(rows), self._table_fqn)

            self._update_row_count(table, len(rows))

            for gold_fqn in self._gold_table_fqns:
                self._post_lineage(gold_fqn)

        except Exception as exc:  # surface failures clearly to schedulers
            logger.exception("audit_log_pusher failed: %s", exc)
            raise

    def _read_rows(self) -> List[dict]:
        if not self._audit_log_path.exists():
            return []
        rows = []
        for line in self._audit_log_path.read_text().splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def _infer_columns(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Infer OM column definitions from JSON rows and append ingested_at.
        Each column dict: {"name": str, "dataType": str, "nullable": bool}
        """
        types: Dict[str, set] = {}
        nullable: Dict[str, bool] = {}

        for row in rows:
            for key, value in row.items():
                nullable.setdefault(key, False)
                types.setdefault(key, set())
                if value is None:
                    nullable[key] = True
                    continue
                types[key].add(self._detect_type(key, value))

        columns: List[Dict[str, Any]] = []
        for key in sorted(types.keys()):
            inferred_type = self._choose_type(key, types[key])
            columns.append(
                {"name": key, "dataType": inferred_type, "nullable": nullable.get(key, True)}
            )

        # ensure ingested_at is present
        columns.append({"name": "ingested_at", "dataType": "timestamp", "nullable": True})
        return columns

    @staticmethod
    def _detect_type(key: str, value: Any) -> str:
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int) and not isinstance(value, bool):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, (dict, list)):
            return "json"
        # Heuristics by key name
        lowered = key.lower()
        if "timestamp" in lowered:
            return "timestamp"
        if lowered.endswith("date") or lowered.startswith("date_") or lowered == "audit_date":
            return "date"
        return "string"

    @staticmethod
    def _choose_type(key: str, detected: set) -> str:
        if not detected:
            return "string"
        if "json" in detected:
            return "json"
        if "float" in detected and "int" in detected:
            return "float"
        # prefer timestamp/date when present
        if "timestamp" in detected:
            return "timestamp"
        if "date" in detected:
            return "date"
        if "float" in detected:
            return "float"
        if "int" in detected:
            return "int"
        if "boolean" in detected:
            return "boolean"
        return "string"

    def _update_row_count(self, table: Any, row_count: int) -> None:
        """
        Best-effort table profile row count update.
        Tries explicit update_table_profile, then ingest_profile_data, otherwise logs.
        """
        if hasattr(self.metadata, "update_table_profile"):
            self.metadata.update_table_profile(table.id, row_count=row_count)
            return

        if hasattr(self.metadata, "ingest_profile_data"):
            try:
                from metadata.generated.schema.api.data.createTableProfile import (
                    CreateTableProfileRequest,
                )
                from metadata.generated.schema.entity.data.table import TableProfile

                profile_request = CreateTableProfileRequest(
                    tableProfile=[TableProfile(rowCount=row_count)]
                )
                self.metadata.ingest_profile_data(table, profile_request)
                return
            except Exception as exc:  # pragma: no cover - only when library available
                logger.warning("Could not ingest table profile: %s", exc)
                return

        logger.info("metadata client missing table profile API; skipping row count update")

    def _assert_required_metadata(self) -> None:
        if not self._owner or not self._tier or not self._env_label:
            raise ValueError("owner, tier, and env_label are required for audit table publishing")

    def _post_lineage(self, gold_fqn: str) -> None:
        edge = {
            "edge": {
                "fromEntity": {"type": "table", "fqn": self._table_fqn},
                "toEntity":   {"type": "table", "fqn": gold_fqn},
            }
        }
        self.metadata.add_lineage(edge)
        logger.info("Lineage: %s \u2192 %s", self._table_fqn, gold_fqn)
