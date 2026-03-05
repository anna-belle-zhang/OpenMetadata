"""ADO connector - reads ado-dumps/ JSON files and creates Pipeline entities."""
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List

from metadata.ingestion.source.pipeline.ado.connection import AdoConfig
from metadata.ingestion.source.pipeline.ado.models import AdoApproval, AdoRun, RunType

logger = logging.getLogger(__name__)

ADO_SERVICE_NAME = "ADO"


@dataclass
class AdoPipelineRequest:
    name: str
    tags: List[str] = field(default_factory=list)


class AdoConnector:
    def __init__(self, config: AdoConfig, metadata: Any):
        self.config = config
        self.metadata = metadata
        self._approvals: Dict[int, AdoApproval] = self._load_approvals()

    def _load_runs(self) -> List[AdoRun]:
        runs_path = self.config.dumps_dir / "runs.json"
        try:
            data = json.loads(runs_path.read_text(encoding="utf-8"))
            return [AdoRun.model_validate(run) for run in data]
        except (json.JSONDecodeError, ValueError) as exc:
            raise ValueError(f"Malformed {runs_path}: {exc}") from exc

    def _load_approvals(self) -> Dict[int, AdoApproval]:
        approvals_path = self.config.dumps_dir / "approvals.json"
        if not approvals_path.exists():
            return {}

        try:
            data = json.loads(approvals_path.read_text(encoding="utf-8"))
            return {
                approval["runId"]: AdoApproval.model_validate(approval)
                for approval in data
            }
        except json.JSONDecodeError:
            logger.warning("Malformed approvals.json - skipping")
            return {}

    def build_pipeline_requests(self) -> List[AdoPipelineRequest]:
        requests: List[AdoPipelineRequest] = []
        for run in self._load_runs():
            requests.append(self._build_one(run))
        return requests

    def run(self) -> int:
        from importlib import import_module

        try:
            Pipeline = import_module(
                "metadata.generated.schema.entity.data.pipeline"
            ).Pipeline
        except Exception:
            from metadata.generated import schema as generated_schema

            Pipeline = generated_schema.entity.data.pipeline.Pipeline

        self.metadata.get_or_create_service(service_name=ADO_SERVICE_NAME)
        for run in self._load_runs():
            existing = self.metadata.get_by_name(
                entity=Pipeline, fqn=f"{ADO_SERVICE_NAME}.{run.id}"
            )
            if existing is not None:
                continue
            self.metadata.create_or_update(self._build_one(run))
        return 0

    def _build_one(self, run: AdoRun) -> AdoPipelineRequest:
        tags = [
            f"run_id={run.id}",
            f"result={run.result or 'unknown'}",
            f"git_sha={run.git_sha}",
            f"pipeline_type={run.pipeline_type}",
        ]

        if run.run_type == RunType.BUILD:
            tags.append(f"image={run.image or 'unknown'}")
        else:
            tags.append(f"version={run.version or 'unknown'}")
            tags.append(f"env={run.env or 'unknown'}")
            approval = self._approvals.get(run.id)
            if approval:
                tags.append(f"approved_by={approval.approver}")
                tags.append(f"approved_at={approval.approved_at}")

        return AdoPipelineRequest(name=str(run.id), tags=tags)
