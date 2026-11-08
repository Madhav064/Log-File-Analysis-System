"""The API against tests/data/: the real pipeline's export of the real NASA sample (make_test_data.py).
Numbers are checked against an independent count of the same sample with the pure-Python parser."""
from collections import Counter
from pathlib import Path

import pytest
from app import create_app

from loganalysis.common.config import REPO_ROOT
from loganalysis.parsing.parser import Rejected, parse_line

DATA = Path(__file__).resolve().parent / "data"
pytestmark = pytest.mark.skipif(not (DATA / "manifest.json").exists(),
                                reason="run dashboard/tests/make_test_data.py to build the test export")

LINES = (REPO_ROOT / "data" / "sample" / "nasa_sample.log").read_text("utf-8", errors="replace").split("\n")
LINES = LINES[:-1] if LINES[-1] == "" else LINES
RESULTS = [parse_line(line) for line in LINES]
RECORDS = [r for r in RESULTS if not isinstance(r, Rejected)]


@pytest.fixture(scope="module")
def client():
    return create_app({"DATA_DIR": str(DATA), "TESTING": True}).test_client()


def get(client, url, status=200):
    r = client.get(url)
    assert r.status_code == status, (url, r.get_json())
    return r.get_json()


def test_summary_matches_an_independent_count_of_the_sample(client):
    s = get(client, "/api/summary")["data"]
    assert s["total_lines"] == len(LINES)
    assert s["parsed"] == s["total_requests"] == len(RECORDS)
    assert s["total_bytes"] == sum(r.bytes for r in RECORDS)
    assert s["unique_hosts"] == len({r.host for r in RECORDS})
    errors = sum(r.is_error for r in RECORDS)
    assert s["error_rate"] == pytest.approx(errors / len(RECORDS))
    assert s["days_with_traffic"] == len({r.log_date for r in RECORDS})
    assert s["hours_with_traffic"] == len({(r.log_date, r.log_hour) for r in RECORDS})


def test_status_counts_and_classes(client):
    body = get(client, "/api/errors/status")
    assert {r["status"]: r["requests"] for r in body["data"]} == dict(Counter(r.status for r in RECORDS))
    assert sum(c["requests"] for c in body["classes"]) == len(RECORDS)
    assert body["source"] == ["status_counts"]


@pytest.mark.parametrize("granularity", ["hour", "day"])
def test_timelines_fill_every_period_and_keep_the_total(client, granularity):
    rows = get(client, f"/api/traffic/timeline?granularity={granularity}")["data"]
    assert sum(r["requests"] or 0 for r in rows) == len(RECORDS)
    key = "hour_ts" if granularity == "hour" else "log_date"
    assert [r[key] for r in rows] == sorted(r[key] for r in rows)
    if granularity == "hour":
        assert rows[0]["hour_ts"].endswith("T00:00:00-04:00") and len(rows) % 24 == 0
    errs = get(client, f"/api/errors/timeline?granularity={granularity}")["data"]
    assert sum((r["errors_4xx"] or 0) + (r["errors_5xx"] or 0) for r in errs) == sum(r.is_error for r in RECORDS)


def test_date_filter_and_its_validation(client):
    first = get(client, "/api/pipeline/quality")["data"]["min_ts"][:10]
    rows = get(client, f"/api/traffic/timeline?granularity=day&from={first}&to={first}")["data"]
    assert [r["log_date"] for r in rows] == [first]
    for bad in ("granularity=week", "from=1995-06-01", "from=13-07-1995", "to=1995-01-01"):
        assert get(client, f"/api/traffic/timeline?{bad}", 400)["code"] == 400


def test_top_resources_by_hits_match_the_count(client):
    rows = get(client, "/api/top/resources?by=hits&n=5")["data"]
    expected = sorted(Counter(r.resource for r in RECORDS).items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    assert [(r["resource"], r["requests"]) for r in rows] == expected
    assert len(get(client, "/api/top/resources?by=bytes&n=3")["data"]) == 3
    get(client, "/api/top/resources?n=0", 400)
    get(client, "/api/top/resources?by=size", 400)


@pytest.mark.parametrize("url", ["/api/errors/top-resources?n=3", "/api/errors/top-hosts?n=3", "/api/top/hosts?n=3",
                                 "/api/top/resource-types", "/api/traffic/profile?by=hour",
                                 "/api/traffic/profile?by=weekday", "/api/traffic/peaks?granularity=day",
                                 "/api/anomalies", "/api/pipeline/quality", "/api/pipeline/runs", "/api/health"])
def test_every_other_endpoint_answers(client, url):
    get(client, url)


def test_profiles_have_all_hours_and_weekdays(client):
    assert [r["hour"] for r in get(client, "/api/traffic/profile?by=hour")["data"]] == list(range(24))
    assert [r["day_of_week"] for r in get(client, "/api/traffic/profile?by=weekday")["data"]] == list(range(1, 8))


def test_tables_missing_from_this_export_answer_503(client):
    # the sample run has no benchmark (compare_engines.py) and no HDFS loader
    assert "pipeline.py export" in get(client, "/api/pipeline/benchmark", 503)["error"]
    get(client, "/api/pipeline/ingestion", 503)
    health = get(client, "/api/health")
    assert set(health["missing"]) == {"engine_benchmark", "ingestion_report"}


def test_bad_enum_params_are_400(client):
    for url in ("/api/anomalies?kind=weird", "/api/anomalies?metric=bytes", "/api/traffic/profile?by=month",
                "/api/traffic/peaks?granularity=week", "/api/errors/top-resources?status=abc"):
        assert get(client, url, 400)["code"] == 400


def test_every_exported_file_matches_the_contract(client):
    from loganalysis.contracts import column_names

    for name, entry in get(client, "/api/health")["tables"].items():
        assert entry["columns"] == list(column_names(name)), name
