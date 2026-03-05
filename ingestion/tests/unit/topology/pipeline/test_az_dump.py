"""Tests for az_dump.sh using a mocked az CLI."""
import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest


def _write_mock_az(tmp_bin: Path, scenario: str = "happy") -> Path:
    """Create a fake `az` executable that returns canned JSON based on the subcommand."""
    script = tmp_bin / "az"
    script.write_text(
        """#!/usr/bin/env python3
import json, os, sys

scenario = os.environ.get("AZ_SCENARIO", "happy")
cmd = " ".join(sys.argv[1:])

def out(obj):
    sys.stdout.write(json.dumps(obj))

if scenario == "fail":
    sys.exit(1)

if "container show" in cmd:
    tag = os.environ.get("AZ_TAG", "231")
    out({"name": "aci-pipeline-prd", "resourceGroup": "rg-airflowdbtdatapipeline-prd",
         "location": "australiaeast",
         "containers": [
             {"name": "airflow", "image": f"repo/airflow:{tag}", "resources": {"requests": {"cpu": 1.0, "memoryInGb": 2.0}}},
             {"name": "crypt", "image": f"repo/crypt:{tag}", "resources": {"requests": {"cpu": 0.5, "memoryInGb": 1.0}}},
         ]})
elif "acr repository show-tags" in cmd and "airflow" in cmd:
    tag = os.environ.get("AZ_TAG", "231")
    out(["229", "230", tag])
elif "acr repository show-tags" in cmd and "crypt" in cmd:
    tag = os.environ.get("AZ_TAG", "231")
    out(["229", "230", tag])
elif "resource list" in cmd:
    out([{"name": "res1", "type": "t", "resourceGroup": "rg-airflowdbtdatapipeline-prd"}])
elif "storage container list" in cmd:
    out([{"name": "blob1"}, {"name": "blob2"}])
else:
    out({})
""",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    os.environ["AZ_SCENARIO"] = scenario
    return script


def _run_dump(tmp_path: Path, env: dict) -> subprocess.CompletedProcess:
    out_dir = tmp_path / "out"
    cmd = ["bash", "ingestion/src/metadata/ingestion/source/pipeline/aci/az_dump.sh", str(out_dir)]
    return subprocess.run(cmd, env=env, capture_output=True, text=True)


@pytest.fixture
def base_env(monkeypatch, tmp_path):
    tmp_bin = tmp_path / "bin"
    tmp_bin.mkdir()
    _write_mock_az(tmp_bin, scenario="happy")
    env = os.environ.copy()
    env["PATH"] = f"{tmp_bin}:{env['PATH']}"
    env["AZURE_RG"] = "rg-airflowdbtdatapipeline-prd"
    env["AZURE_ACR"] = "acrpipelinedevhbwx"
    env["AZURE_ACI_NAME"] = "aci-pipeline-prd"
    env["AZURE_STORAGE_ACCOUNT"] = "stpipelinedata"
    return env


def test_dumps_all_expected_files(base_env, tmp_path):
    result = _run_dump(tmp_path, base_env)
    assert result.returncode == 0
    out_dir = tmp_path / "out"
    files = {p.name for p in out_dir.iterdir()}
    assert {"aci.json", "acr_tags_airflow.json", "acr_tags_crypt.json", "resources.json", "blobs.json"} <= files
    aci = json.loads((out_dir / "aci.json").read_text())
    assert aci["name"] == "aci-pipeline-prd"
    assert any(c["name"] == "airflow" for c in aci["containers"])
    # assert image tags and cpu are present in aci.json output
    for container in aci["containers"]:
        assert "image" in container
        assert ":" in container["image"]  # has tag
        assert container["resources"]["requests"]["cpu"] > 0
    airflow_tags = json.loads((out_dir / "acr_tags_airflow.json").read_text())
    assert airflow_tags[-1] == "231"
    blobs = json.loads((out_dir / "blobs.json").read_text())
    assert {"name": "blob1"} in blobs


def test_idempotent_overwrites_outputs(base_env, tmp_path):
    # First run with tag 231
    result1 = _run_dump(tmp_path, {**base_env, "AZ_TAG": "231"})
    assert result1.returncode == 0
    out_dir = tmp_path / "out"
    first = json.loads((out_dir / "acr_tags_airflow.json").read_text())
    assert first[-1] == "231"
    # Second run with tag 232 should overwrite, not append
    result2 = _run_dump(tmp_path, {**base_env, "AZ_TAG": "232"})
    assert result2.returncode == 0
    second = json.loads((out_dir / "acr_tags_airflow.json").read_text())
    assert second[-1] == "232"
    assert second.count("232") == 1  # no duplicate append


def test_fail_fast_on_az_error(tmp_path):
    tmp_bin = tmp_path / "bin"
    tmp_bin.mkdir()
    _write_mock_az(tmp_bin, scenario="fail")
    env = os.environ.copy()
    env["PATH"] = f"{tmp_bin}:{env['PATH']}"
    result = _run_dump(tmp_path, env)
    assert result.returncode != 0
    out_dir = tmp_path / "out"
    if out_dir.exists():
        files = list(out_dir.iterdir())
        assert all(f.stat().st_size == 0 for f in files)
