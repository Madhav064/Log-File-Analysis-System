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
    if pipeline.missing_module(pipeline.commands("export", "local", [])[0]) is None:
        pytest.skip("the export job exists now")  # never run a real stage from a unit test
    r = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "pipeline.py"), "export", "--env", "local"],
                       capture_output=True, text=True)
    assert "does not exist yet (task C7)" in r.stderr


def test_job_name_is_the_module_or_the_repo_relative_script():
    assert pipeline.job_name(pipeline.commands("etl", "local", [])[0]) == "loganalysis.etl.parse_logs"
    assert pipeline.job_name(pipeline.commands("compare", "local", [])[0]) == "scripts/compare_engines.py"
    assert pipeline.job_name(pipeline.commands("mapreduce", "local", [])[0]) == "mapreduce/run.py"


T0 = 1_762_934_400  # a real 2025 epoch; Windows cannot convert timestamps near 0 to local time


def test_record_run_keeps_the_latest_run_of_each_job_and_fits_the_contract():
    from loganalysis.common.reports import to_rows

    runs = pipeline.record_run([], "etl", "a", "cluster", T0, 150.24, "ok")
    runs = pipeline.record_run(runs, "analytics", "b", "cluster", T0 + 10, 60, "ok")
    runs = pipeline.record_run(runs, "etl", "a", "cluster", T0 + 20, 140, "failed")
    assert [(r["stage"], r["duration_sec"], r["status"]) for r in runs] == [("analytics", 60, "ok"),
                                                                             ("etl", 140, "failed")]
    to_rows("pipeline_runs", runs)  # raises if a column is missing or extra


def test_unknown_only_value_is_rejected():
    r = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "pipeline.py"), "analytics", "--only", "bogus"],
                       capture_output=True, text=True)
    assert r.returncode == 2 and "unknown job" in r.stderr
