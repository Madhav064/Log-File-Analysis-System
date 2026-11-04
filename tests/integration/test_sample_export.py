"""ETL -> analytics -> export on the bundled sample (real NASA lines). Checks the serving JSON the dashboard reads:
one file per table in contract order, timestamps with the server's offset, and a complete manifest."""
import json
import re

import pytest

pytest.importorskip("pyspark")

from loganalysis.analytics import anomalies, errors, top_n, traffic  # noqa: E402
from loganalysis.common.reports import write_report  # noqa: E402
from loganalysis.contracts import REPORTS, TABLES, column_names  # noqa: E402
from loganalysis.etl import parse_logs  # noqa: E402
from loganalysis.export import to_serving  # noqa: E402
from loganalysis.export.serving import MANIFEST  # noqa: E402

ISO_WITH_OFFSET = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d-04:00$")


@pytest.fixture(scope="module")
def serving(spark, tmp_path_factory):
    from loganalysis.common.config import load_config

    cfg = load_config("local")
    out = tmp_path_factory.mktemp("export")
    for key in ("parsed", "rejected", "quality", "analytics", "mapreduce", "serving", "reports"):
        cfg["paths"][key] = str(out / key)
    parse_logs.run(spark, cfg)
    for job in (errors, traffic, top_n, anomalies):
        job.run(spark, cfg)
    write_report(cfg, "pipeline_runs", [{"stage": "etl", "job": "loganalysis.etl.parse_logs", "env": "local",
                                         "started_at": "2026-11-12T10:00:00-05:00", "duration_sec": 9.5,
                                         "status": "ok"}])
    to_serving.run(spark, cfg)
    return out / "serving"


def load(serving, name):
    return json.loads((serving / f"{name}.json").read_text())


def test_manifest_lists_every_table_and_report(serving):
    m = json.loads((serving / MANIFEST).read_text())
    assert set(m["tables"]) == set(TABLES) | set(REPORTS)
    assert m["tables"]["engine_benchmark"]["missing"] is True       # compare_engines.py has not run here
    assert m["tables"]["ingestion_report"]["missing"] is True       # only the cluster loader writes it
    for name in set(TABLES) - {"engine_benchmark"}:
        entry = m["tables"][name]
        assert not entry["missing"] and entry["rows"] == len(load(serving, name)["rows"]), name


@pytest.mark.parametrize("name", [n for n in TABLES if n != "engine_benchmark"])
def test_every_file_has_the_contract_columns_and_iso_timestamps(serving, name):
    data = load(serving, name)
    assert data["columns"] == list(column_names(name))
    for i, (col, typ) in enumerate(TABLES[name].columns):
        values = [row[i] for row in data["rows"] if row[i] is not None]
        if typ == "timestamp":
            assert all(ISO_WITH_OFFSET.match(v) for v in values), (name, col, values[:3])


def test_hour_buckets_keep_server_local_time(serving):
    first = load(serving, "traffic_hourly")["rows"][0][0]
    assert first == "1995-07-01T00:00:00-04:00"   # the sample starts at 01/Jul/1995:00:00:01 -0400


def test_quality_report_map_becomes_a_json_object(serving):
    q = load(serving, "quality_report")
    row = dict(zip(q["columns"], q["rows"][0]))
    assert isinstance(row["rejected_by_reason"], dict)
    assert row["parsed"] + row["rejected"] == row["total_lines"]


def test_pipeline_runs_gets_the_exports_own_runtime(serving):
    runs = load(serving, "pipeline_runs")
    stages = [dict(zip(runs["columns"], r))["stage"] for r in runs["rows"]]
    assert stages == ["etl", "export"]


def test_a_table_over_the_row_limit_is_refused(spark):
    df = spark.range(5).selectExpr("CAST(id AS STRING) AS host", "id AS errors")
    with pytest.raises(SystemExit, match="more than 3"):
        to_serving.collect_rows(df, "top_error_hosts", 3)
