"""Shared fixtures. Spark fixtures skip (not fail) where pyspark is not installed, so the pure-Python tests
still run anywhere; in the client container and in CI pyspark is present and everything runs."""
import os
import time

import pytest

from loganalysis.common.config import load_config

# PySpark turns collected timestamps into naive datetimes in the *Python process's* time zone, not the Spark
# session's. Pin the process to the server's zone so `row.hour_ts.hour` means the same as in the jobs.
os.environ["TZ"] = load_config("local")["timezone"]
if hasattr(time, "tzset"):  # not on Windows, where the Spark tests are skipped anyway
    time.tzset()

ANOMALY_TEST_CFG = {"z_threshold": 3, "baseline_days": 7, "min_baseline_points": 3, "min_requests": 5}


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from loganalysis.common.spark import get_spark

    if time.localtime(804571201).tm_hour != 0:  # 1 Jul 1995 00:00:01 at UTC-04:00
        pytest.fail(f"Python is not in {os.environ['TZ']}, so collected timestamps would be wrong; "
                    f"install the OS time-zone database (apt package tzdata)")

    session = get_spark("tests", load_config("local"))
    yield session
    session.stop()


@pytest.fixture
def local_cfg(tmp_path):
    """The local config with every output under tmp_path; input stays the bundled sample."""
    cfg = load_config("local")
    for key in ("parsed", "rejected", "quality", "analytics", "mapreduce", "serving"):
        cfg["paths"][key] = str(tmp_path / key)
    return cfg


def log_line(ts="01/Jul/1995:00:00:01", status=200, resource="/index.html", host="h1", nbytes=100, method="GET"):
    """A Common Log Format line with sensible defaults; nbytes=None writes '-'."""
    size = "-" if nbytes is None else nbytes
    return f'{host} - - [{ts} -0400] "{method} {resource} HTTP/1.0" {status} {size}'


@pytest.fixture
def make_parsed(spark):
    """Lines -> a parsed DataFrame (§6.2), built with the Python parser so Spark transforms can be tested on
    tiny hand-written inputs. Every line must parse."""
    from loganalysis.parsing.parser import Rejected, parse_line
    from loganalysis.parsing.schema import parsed_struct

    def build(lines):
        records = [parse_line(line) for line in lines]
        bad = [line for line, r in zip(lines, records) if isinstance(r, Rejected)]
        assert not bad, f"test lines must parse: {bad}"
        return spark.createDataFrame(records, parsed_struct())

    return build
