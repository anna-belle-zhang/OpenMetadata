"""ado_dump.py - fetch ADO pipeline runs and approvals and write to JSON files."""

import base64
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class AdoDumperConfig:
    out_dir: Path
    org_url: str
    project: str
    pat: str


def _auth_header(pat: str) -> Dict[str, str]:
    token = base64.b64encode(f":{pat}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}


def _api_get(url: str, pat: str, params: Optional[Dict] = None) -> Any:
    resp = requests.get(url, headers=_auth_header(pat), params=params)
    resp.raise_for_status()
    return resp.json()


def _normalize_run(raw: Dict) -> Dict:
    params = raw.get("templateParameters", {}) or {}
    variables = raw.get("variables", {}) or {}
    pipeline_name = raw.get("pipeline", {}).get("name", "")
    is_deploy = "deploy" in pipeline_name.lower()
    run_type = "deploy" if is_deploy else "build"
    pipeline_type = "infra_deploy" if is_deploy else "image_build"
    git_sha = (
        raw.get("resources", {})
        .get("repositories", {})
        .get("self", {})
        .get("version", "")
    )
    return {
        "id": raw["id"],
        "name": raw["name"],
        "startTime": raw.get("createdDate"),
        "result": raw.get("result"),
        "type": run_type,
        "pipeline_type": pipeline_type,
        "git_sha": git_sha,
        "image": params.get("image_tag") or variables.get("image_tag", {}).get("value"),
        "version": params.get("version") or variables.get("version", {}).get("value"),
        "env": params.get("env") or variables.get("env", {}).get("value"),
    }


class AdoDumper:
    def __init__(self, config: AdoDumperConfig):
        self.config = config

    def _existing_runs(self) -> List[Dict]:
        path = self.config.out_dir / "runs.json"
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return []

    def _last_timestamp(self, existing: List[Dict]) -> Optional[str]:
        if not existing:
            return None
        times = [run.get("startTime") for run in existing if run.get("startTime")]
        return max(times) if times else None

    def dump(self) -> None:
        if not self.config.pat:
            raise ValueError("ADO_PAT is required but not set")

        existing = self._existing_runs()
        existing_ids = {run["id"] for run in existing}

        runs_url = f"{self.config.org_url}/{self.config.project}/_apis/pipelines/runs?api-version=7.1"
        params: Dict[str, str] = {}
        last_ts = self._last_timestamp(existing)
        if last_ts:
            params["createdAfter"] = last_ts

        data = _api_get(runs_url, self.config.pat, params=params or None)
        raw_runs = data.get("value", [])

        new_runs: List[Dict] = []
        for raw in raw_runs:
            normalized = _normalize_run(raw)
            if normalized["id"] not in existing_ids:
                new_runs.append(normalized)

        all_runs = existing + new_runs
        self.config.out_dir.mkdir(parents=True, exist_ok=True)
        (self.config.out_dir / "runs.json").write_text(
            json.dumps(all_runs, indent=2, default=str),
            encoding="utf-8",
        )
        logger.info("Wrote %d runs (%d new) to runs.json", len(all_runs), len(new_runs))

        # Best-effort approvals dump.
        try:
            approvals_url = (
                f"{self.config.org_url}/{self.config.project}"
                "/_apis/pipelines/approvals?api-version=7.1-preview.1"
            )
            approvals_data = _api_get(approvals_url, self.config.pat)
            approvals: List[Dict] = []
            for item in approvals_data.get("value", []):
                steps = item.get("steps", [])
                pipeline_id = item.get("run", {}).get("id") or item.get("pipeline", {}).get("id")
                if steps and pipeline_id:
                    step = steps[0]
                    approvals.append(
                        {
                            "runId": pipeline_id,
                            "approver": step.get("assignedApprover", {}).get("displayName", ""),
                            "approvedAt": step.get("lastModifiedOn", ""),
                        }
                    )
            (self.config.out_dir / "approvals.json").write_text(
                json.dumps(approvals, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Could not fetch approvals: %s", exc)


def main() -> None:
    config = AdoDumperConfig(
        out_dir=Path(
            os.environ.get("ADO_DUMPS_DIR", "ingestion/examples/lineage-dumps/ado-dumps")
        ),
        org_url=os.environ.get("ADO_ORG_URL", ""),
        project=os.environ.get("ADO_PROJECT", ""),
        pat=os.environ.get("ADO_PAT", ""),
    )
    logging.basicConfig(level=logging.INFO)
    try:
        AdoDumper(config).dump()
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
