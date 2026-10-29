"""The MapReduce mappers and reducers, run exactly as Hadoop would run them (map -> sort -> reduce)
but locally on the bundled sample, must agree with a straight Python count over the same lines."""
import importlib.util
from collections import Counter
from pathlib import Path

import pytest

from loganalysis.common.config import REPO_ROOT, load_config
from loganalysis.parsing.parser import Rejected, parse_line

spec = importlib.util.spec_from_file_location("mr_run", REPO_ROOT / "mapreduce" / "run.py")
mr_run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mr_run)

SAMPLE = REPO_ROOT / "data" / "sample" / "nasa_sample.log"
RECORDS = [r for r in (parse_line(line) for line in SAMPLE.read_text("utf-8").splitlines())
           if not isinstance(r, Rejected)]


@pytest.fixture
def cfg(tmp_path):
    cfg = load_config("local")
    cfg["paths"]["mapreduce"] = str(tmp_path)
    return cfg


def read_output(path):
    return [tuple(line.split("\t")) for line in (Path(path) / "part-00000").read_text().splitlines()]


def test_sample_has_the_odd_lines_we_rely_on():
    assert len(RECORDS) >= 2000
    assert any(r.bytes_missing for r in RECORDS) and any(r.status_class == "5xx" for r in RECORDS)


def test_status_count(cfg):
    rows = read_output(mr_run.run_local(cfg, "status_count", 20))
    assert {k: int(v) for k, v in rows} == {str(k): v for k, v in Counter(r.status for r in RECORDS).items()}


def test_hourly_traffic(cfg):
    rows = read_output(mr_run.run_local(cfg, "hourly_traffic", 20))
    assert {k: int(v) for k, v in rows} == dict(Counter(f"{r.ts:%Y-%m-%dT%H}" for r in RECORDS))
    assert [k for k, _ in rows] == sorted(k for k, _ in rows)


def test_top_resources_two_stages(cfg):
    n = 5
    rows = [(k, int(v)) for k, v in read_output(mr_run.run_local(cfg, "top_resources", n))]
    expected = sorted(Counter(r.resource for r in RECORDS).items(), key=lambda kv: (-kv[1], kv[0]))[:n]
    assert rows == expected
