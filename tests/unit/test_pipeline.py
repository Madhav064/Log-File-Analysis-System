import importlib.util
import subprocess
import sys

import pytest

from loganalysis.common.config import REPO_ROOT

spec = importlib.util.spec_from_file_location("pipeline", REPO_ROOT / "scripts" / "pipeline.py")
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)


def test_analytics_runs_each_job_as_a_module_or_only_the_selected():
    every = pipeline.commands("analytics", "local", [])
    assert [c[2] for c in every] == [f"loganalysis.analytics.{m}" for m in pipeline.ANALYTICS]
    assert pipeline.commands("analytics", "cluster", ["errors"]) == [
        [sys.executable, "-m", "loganalysis.analytics.errors", "--env", "cluster"]]


def test_ingest_uses_the_hdfs_loader_only_on_the_cluster():
    assert pipeline.commands("ingest", "cluster", [])[0][2] == "loganalysis.ingestion.hdfs_loader"
    assert pipeline.commands("ingest", "local", [])[0][2] == "loganalysis.ingestion.download"


def test_missing_module_is_detected_including_missing_parent_package():
    assert pipeline.missing_module([sys.executable, "-m", "loganalysis.nope.nothing"]) == "loganalysis.nope.nothing"
    assert pipeline.missing_module([sys.executable, "-m", "loganalysis.parsing.parser"]) is None
    assert pipeline.missing_module([sys.executable, "script.py"]) is None


def test_stage_fails_fast_with_the_owning_task_when_a_job_is_not_built_yet():
    r = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "pipeline.py"), "etl", "--env", "local"],
                       capture_output=True, text=True)
    if r.returncode == 0:
        pytest.skip("the ETL job exists now")
    assert "does not exist yet (task B4)" in r.stderr


def test_unknown_only_value_is_rejected():
    r = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "pipeline.py"), "analytics", "--only", "bogus"],
                       capture_output=True, text=True)
    assert r.returncode == 2 and "unknown job" in r.stderr
