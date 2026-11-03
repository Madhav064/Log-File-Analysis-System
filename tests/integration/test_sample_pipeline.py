"""ETL -> analytics on the bundled sample, exactly as `pipeline.py etl/analytics --env local` runs them (outputs go
to a temp dir). Checks every table against its contract and the numbers against an independent Python count and
against the MapReduce jobs (run locally), i.e. what scripts/compare_engines.py checks on the cluster."""
import importlib.util
from collections import Counter

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

from loganalysis.analytics import anomalies, errors, top_n, traffic  # noqa: E402
from loganalysis.common.config import REPO_ROOT  # noqa: E402
from loganalysis.common.io import read_parsed, read_table  # noqa: E402
from loganalysis.contracts import TABLES  # noqa: E402
from loganalysis.etl import parse_logs  # noqa: E402
from loganalysis.parsing.parser import Rejected, parse_line  # noqa: E402
from loganalysis.parsing.schema import REJECTED_COLUMNS, parsed_struct  # noqa: E402


def load_script(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def file_lines(path):
    """Lines as Spark's text reader sees them: split on newlines only (str.splitlines also splits on \\f, \\v...)."""
    lines = path.read_text("utf-8").split("\n")
    return lines[:-1] if lines and lines[-1] == "" else lines


SAMPLE_LINES = file_lines(REPO_ROOT / "data" / "sample" / "nasa_sample.log")
RESULTS = [parse_line(line) for line in SAMPLE_LINES]
RECORDS = [r for r in RESULTS if not isinstance(r, Rejected)]
SPARK_TABLES = [name for name, t in TABLES.items() if name != "engine_benchmark"]


@pytest.fixture(scope="module")
def pipeline_output(spark, tmp_path_factory):
    from loganalysis.common.config import load_config

    cfg = load_config("local")
    out = tmp_path_factory.mktemp("pipeline")
    for key in ("parsed", "rejected", "quality", "analytics", "mapreduce"):
        cfg["paths"][key] = str(out / key)
    parse_logs.run(spark, cfg)
    for job in (errors, traffic, top_n, anomalies):
        job.run(spark, cfg)
    return cfg


def test_quality_report_accounts_for_every_line(spark, pipeline_output):
    q = read_table(spark, pipeline_output, "quality_report").collect()
    assert len(q) == 1
    q = q[0]
    reasons = Counter(r.reason for r in RESULTS if isinstance(r, Rejected))
    assert (q.total_lines, q.parsed, q.rejected) == (len(SAMPLE_LINES), len(RECORDS), sum(reasons.values()))
    assert dict(q.rejected_by_reason) == dict(reasons)
    assert q.unique_hosts == len({r.host for r in RECORDS})


def test_parsed_and_rejected_datasets(spark, pipeline_output):
    parsed = read_parsed(spark, pipeline_output)
    # same columns and types; order differs because Spark appends the partition column (log_date) last
    expected = {(f.name, f.dataType) for f in parsed_struct().fields}
    assert {(f.name, f.dataType) for f in parsed.schema.fields} == expected
    assert parsed.count() == len(RECORDS)
    rejected = spark.read.parquet(pipeline_output["paths"]["rejected"])
    assert tuple(rejected.columns) == REJECTED_COLUMNS
    assert {r.source_file for r in rejected.collect()} == {"nasa_sample.log"}


@pytest.mark.parametrize("name", SPARK_TABLES)
def test_every_table_matches_its_contract(spark, pipeline_output, name):
    df = read_table(spark, pipeline_output, name)
    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == list(TABLES[name].columns)


def test_counts_agree_with_an_independent_python_count(spark, pipeline_output):
    status = {r.status: r.requests for r in read_table(spark, pipeline_output, "status_counts").collect()}
    assert status == dict(Counter(r.status for r in RECORDS))
    hourly = read_table(spark, pipeline_output, "traffic_hourly").agg(
        F.sum("requests").alias("requests"), F.sum("bytes").alias("bytes")).first()
    assert (hourly.requests, hourly.bytes) == (len(RECORDS), sum(r.bytes for r in RECORDS))
    top = read_table(spark, pipeline_output, "top_resources_by_hits").collect()
    expected = sorted(Counter(r.resource for r in RECORDS).items(), key=lambda kv: (-kv[1], kv[0]))
    assert sorted(((r.resource, r.requests) for r in top), key=lambda kv: (-kv[1], kv[0])) == \
        expected[:pipeline_output["top_n"]]


def test_spark_matches_mapreduce_on_the_sample(spark, pipeline_output):
    compare = load_script("compare_engines", REPO_ROOT / "scripts" / "compare_engines.py")
    mr_run = load_script("mr_run", REPO_ROOT / "mapreduce" / "run.py")
    for job, spec in compare.JOBS.items():
        mr_run.run_local(pipeline_output, job, pipeline_output["top_n"])
        diff = compare.compare(compare.read_spark(spark, pipeline_output, spec),
                               compare.parse_mapreduce(compare.read_mapreduce(pipeline_output, job)), spec["ranked"])
        assert diff["match"], (job, diff)


def test_rerunning_a_job_replaces_its_output(spark, pipeline_output):
    errors.run(spark, pipeline_output)
    status = read_table(spark, pipeline_output, "status_counts")
    assert status.count() == len({r.status for r in RECORDS})
