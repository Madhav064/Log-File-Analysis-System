import pytest

pytest.importorskip("pyspark")

from conftest import log_line  # noqa: E402

from loganalysis.analytics import traffic  # noqa: E402
from loganalysis.common.io import conform  # noqa: E402

# 1 July 1995 was a Saturday (ISO 6); 3 July a Monday (ISO 1). 2 July has no traffic at all.
LINES = [
    log_line("01/Jul/1995:10:00:05", host="a", nbytes=100),
    log_line("01/Jul/1995:10:00:40", host="a", nbytes=None),
    log_line("01/Jul/1995:10:59:59", host="b", nbytes=50),
    log_line("01/Jul/1995:11:30:00", host="c", nbytes=10),
    log_line("03/Jul/1995:10:15:00", host="a", nbytes=1),
    log_line("03/Jul/1995:10:15:30", host="d", nbytes=2),
]


@pytest.fixture
def parsed(make_parsed):
    return make_parsed(LINES)


def test_hourly_volume_and_exact_unique_hosts(parsed):
    rows = sorted(traffic.traffic_hourly(parsed).collect(), key=lambda r: r.hour_ts)
    assert [(r.hour_ts.day, r.hour_ts.hour, r.requests, r.bytes, r.unique_hosts) for r in rows] == [
        (1, 10, 3, 150, 2), (1, 11, 1, 10, 1), (3, 10, 2, 3, 2)]


def test_daily_volume_with_iso_day_of_week(parsed):
    rows = sorted(traffic.traffic_daily(parsed).collect(), key=lambda r: r.log_date)
    assert [(str(r.log_date), r.day_of_week, r.requests, r.unique_hosts) for r in rows] == [
        ("1995-07-01", 6, 4, 3), ("1995-07-03", 1, 2, 2)]


def test_hour_of_day_profile_has_24_hours_averaged_over_days_with_traffic(parsed):
    rows = {r.hour: r for r in traffic.hour_of_day_profile(parsed).collect()}
    assert sorted(rows) == list(range(24))
    assert (rows[10].total_requests, rows[10].avg_requests_per_day) == (5, 2.5)   # 2 days with traffic, not 3
    assert (rows[0].total_requests, rows[0].avg_requests_per_day) == (0, 0.0)


def test_day_of_week_profile_has_7_days(parsed):
    rows = {r.day_of_week: r for r in traffic.day_of_week_profile(traffic.traffic_daily(parsed)).collect()}
    assert sorted(rows) == list(range(1, 8))
    assert (rows[6].total_requests, rows[6].avg_requests) == (4, 4.0)
    assert (rows[7].total_requests, rows[7].avg_requests) == (0, 0.0)


def test_peaks_rank_each_granularity_and_break_ties_by_earliest(parsed):
    hourly, daily = traffic.traffic_hourly(parsed), traffic.traffic_daily(parsed)
    rows = traffic.traffic_peaks(parsed, hourly, daily, k=2).collect()
    by = {g: sorted((r for r in rows if r.granularity == g), key=lambda r: r.rank) for g in ("minute", "hour", "day")}
    assert [(r.period_start.hour, r.period_start.minute, r.requests) for r in by["minute"]] == [(10, 0, 2), (10, 15, 2)]
    assert [(r.period_start.day, r.period_start.hour, r.requests) for r in by["hour"]] == [(1, 10, 3), (3, 10, 2)]
    assert [(r.period_start.day, r.period_start.hour, r.requests, r.rank) for r in by["day"]] == [
        (1, 0, 4, 1), (3, 0, 2, 2)]


def test_compute_matches_the_contract_tables(parsed):
    for name, df in traffic.compute(parsed, {"peaks_top_k": 3}).items():
        conform(df, name)
