import pytest

pytest.importorskip("pyspark")

from conftest import log_line  # noqa: E402

from loganalysis.analytics import errors  # noqa: E402

LINES = [
    log_line("01/Jul/1995:00:10:00", 200, "/a"),
    log_line("01/Jul/1995:00:20:00", 404, "/missing", host="bad"),
    log_line("01/Jul/1995:00:30:00", 404, "/missing", host="bad"),
    log_line("01/Jul/1995:00:40:00", 404, "/gone", host="other"),
    log_line("01/Jul/1995:01:05:00", 500, "/cgi", host="bad"),
    log_line("01/Jul/1995:01:15:00", 200, "/a"),
    log_line("01/Jul/1995:01:25:00", 304, "/a"),
    log_line("01/Jul/1995:01:35:00", 403, "/secret", host="other"),
]


@pytest.fixture
def parsed(make_parsed):
    return make_parsed(LINES)


def test_status_counts_with_share_of_total(parsed):
    rows = {r.status: r for r in errors.status_counts(parsed).collect()}
    assert {s: r.requests for s, r in rows.items()} == {200: 2, 304: 1, 403: 1, 404: 3, 500: 1}
    assert rows[404].status_class == "4xx" and rows[404].pct == pytest.approx(37.5)
    assert sum(r.pct for r in rows.values()) == pytest.approx(100)


def test_error_timeline_splits_4xx_and_5xx_per_hour(parsed):
    rows = sorted(errors.error_timeline_hourly(parsed).collect(), key=lambda r: r.hour_ts)
    assert [(r.hour_ts.hour, r.requests, r.errors_4xx, r.errors_5xx) for r in rows] == [(0, 4, 3, 0), (1, 4, 1, 1)]
    assert [r.error_rate for r in rows] == pytest.approx([0.75, 0.5])


def test_top_error_resources_is_per_status_with_key_tiebreak(parsed):
    rows = errors.top_error_resources(parsed, n=1).collect()
    assert sorted((r.status, r.resource, r.requests) for r in rows) == [
        (403, "/secret", 1), (404, "/missing", 2), (500, "/cgi", 1)]
    both_404 = errors.top_error_resources(parsed, n=5).where("status = 404").collect()
    assert [(r.resource, r.requests) for r in sorted(both_404, key=lambda r: -r.requests)] == [
        ("/missing", 2), ("/gone", 1)]


def test_top_error_hosts(parsed):
    rows = errors.top_error_hosts(parsed, n=1).collect()
    assert [(r.host, r.errors) for r in rows] == [("bad", 3)]


def test_compute_matches_the_contract_tables(parsed):
    from loganalysis.common.io import conform

    out = errors.compute(parsed, {"top_n": 3})
    for name, df in out.items():
        conform(df, name)  # raises on any missing or extra column
