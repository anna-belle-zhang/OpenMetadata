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
"""Utilities to enrich DAG entities with build and infra audit context."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class AuditEntity:
    fqn: str
    date: datetime
    git_sha: str = ""
    approved_by: Optional[str] = None
    env: Optional[str] = None


@dataclass
class DagRecord:
    fqn: str
    run_end: Optional[datetime]


class DagEnricher:
    def __init__(self, metadata: Any, audit_log_path: Optional[Path] = None):
        self.metadata = metadata
        self._audit_log_path = audit_log_path or Path("audit_log.jsonl")

    def enrich(
        self,
        builds: List[AuditEntity],
        infras: List[AuditEntity],
        dags: List[DagRecord],
    ) -> None:
        for dag in dags:
            if dag.run_end is None:
                continue

            active_build = self._latest_before(builds, dag.run_end)
            if active_build is None:
                logger.warning("No build found before DAG run for dag %s", dag.fqn)
                continue

            active_infra = self._latest_before(infras, dag.run_end)
            props = {
                "image_build_ref": active_build.fqn,
                "git_sha": active_build.git_sha,
                "run_end_timestamp": dag.run_end.isoformat(),
            }
            if active_infra is not None:
                props["infra_deploy_ref"] = active_infra.fqn

            self.metadata.patch_custom_properties(dag.fqn, props)
            self._append_audit_row(dag=dag, build=active_build, infra=active_infra)

    def _latest_before(
        self, entities: List[AuditEntity], cutoff: datetime
    ) -> Optional[AuditEntity]:
        eligible = [entity for entity in entities if entity.date <= cutoff]
        if not eligible:
            return None
        return max(eligible, key=lambda entity: entity.date)

    def _append_audit_row(
        self,
        dag: DagRecord,
        build: AuditEntity,
        infra: Optional[AuditEntity],
    ) -> None:
        audit_date = dag.run_end.date().isoformat()
        # Avoid duplicates for the same dag on the same audit date.
        if self._audit_log_path.exists():
            with self._audit_log_path.open("r", encoding="utf-8") as existing:
                for line in existing:
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if entry.get("audit_date") == audit_date and entry.get("dag_fqn") == dag.fqn:
                        return

        row = {
            "audit_date": audit_date,
            "dag_fqn": dag.fqn,
            "run_end_timestamp": dag.run_end.isoformat(),
            "image_build_ref": build.fqn,
            "git_sha": build.git_sha,
            "infra_deploy_ref": infra.fqn if infra else None,
            "approved_by": infra.approved_by if infra else None,
            "env": infra.env if infra else None,
        }
        with self._audit_log_path.open("a", encoding="utf-8") as audit_file:
            audit_file.write(json.dumps(row))
            audit_file.write("\n")
