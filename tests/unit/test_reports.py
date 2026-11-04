import json

import pytest

from loganalysis.common.reports import read_report, report_path, write_report
from loganalysis.contracts import column_names

RUN = {"stage": "etl", "job": "loganalysis.etl.parse_logs", "env": "cluster",
       "started_at": "2026-11-12T14:00:00+05:30", "duration_sec": 150.2, "status": "ok"}


@pytest.fixture
def cfg(tmp_path):
    return {"paths": {"reports": str(tmp_path / "reports")}}


def test_write_then_read_round_trips_in_contract_order(cfg):
    path = write_report(cfg, "pipeline_runs", [RUN])
    data = json.loads(path.read_text())
    assert data["columns"] == list(column_names("pipeline_runs"))
    assert data["rows"][0][0] == "etl"
    assert read_report(cfg, "pipeline_runs") == [RUN]


def test_missing_report_reads_as_none(cfg):
    assert read_report(cfg, "ingestion_report") is None


def test_a_record_that_breaks_the_contract_is_refused_and_nothing_is_written(cfg):
    with pytest.raises(ValueError, match="missing"):
        write_report(cfg, "pipeline_runs", [{**RUN, "extra": 1}])
    assert not report_path(cfg, "pipeline_runs").exists()


def test_unknown_report_name_is_refused(cfg):
    with pytest.raises(KeyError):
        write_report(cfg, "status_counts", [])
