"""Ten days of synthetic hourly traffic with three planted events: a 3-hour gap, a traffic spike and an
error-rate spike. Normal hours vary a little (10-12 requests, 0-1 errors) so the baseline std is not 0."""
import pytest

pytest.importorskip("pyspark")

from conftest import ANOMALY_TEST_CFG, log_line  # noqa: E402

from loganalysis.analytics import anomalies  # noqa: E402
from loganalysis.common.io import conform  # noqa: E402

CFG = {"anomaly": ANOMALY_TEST_CFG}
GAP = {(5, 3), (5, 4), (5, 5)}          # (day of July, hour): no requests at all
TRAFFIC_SPIKE = (9, 12)                 # 100 requests instead of ~11
ERROR_SPIKE = (9, 15)                   # every request is a 404


def synthetic_lines():
    lines = []
    for day in range(1, 11):
        for hour in range(24):
            if (day, hour) in GAP:
                continue
            n = 100 if (day, hour) == TRAFFIC_SPIKE else 10 + day % 3
            errors = n if (day, hour) == ERROR_SPIKE else day % 2
            for i in range(n):
                ts = f"{day:02d}/Jul/1995:{hour:02d}:{i % 60:02d}:{i // 60:02d}"
                lines.append(log_line(ts, status=404 if i < errors else 200, resource=f"/r{i}"))
    return lines


@pytest.fixture(scope="module")
def parsed(spark):
    from loganalysis.parsing.parser import parse_line
    from loganalysis.parsing.schema import parsed_struct

    return spark.createDataFrame([parse_line(line) for line in synthetic_lines()], parsed_struct()).cache()


def key(row):
    return (row.hour_ts.day, row.hour_ts.hour)


def test_calendar_is_complete_with_zero_hours(parsed):
    cal = anomalies.hourly_calendar(parsed).collect()
    assert len(cal) == 10 * 24
    assert {key(r) for r in cal if r.requests == 0} == GAP


def test_gaps_spikes_and_nothing_else(parsed):
    rows = anomalies.detect(parsed, CFG).collect()
    found = {(r.metric, r.kind, key(r)) for r in rows}
    assert found == ({("requests", "gap", h) for h in GAP}
                     | {("requests", "spike", TRAFFIC_SPIKE), ("error_rate", "spike", ERROR_SPIKE)})


def test_gap_rows_carry_no_baseline(parsed):
    gaps = [r for r in anomalies.detect(parsed, CFG).collect() if r.kind == "gap"]
    assert all(r.value == 0.0 and r.baseline_mean is None and r.z_score is None for r in gaps)


def test_gap_hours_are_left_out_of_the_following_baseline(parsed):
    # 6 July 03:00: the previous days at 03:00 are 1-4 July (11, 12, 10, 11 requests) and the 5 July gap.
    scored = anomalies.score(anomalies.hourly_calendar(parsed), CFG)
    r = scored.where("metric = 'requests'").collect()
    row = next(x for x in r if key(x) == (6, 3))
    assert (row.baseline_points, row.baseline_mean) == (4, pytest.approx(11.0))


def test_first_days_are_not_scored_without_enough_baseline(parsed):
    scored = anomalies.score(anomalies.hourly_calendar(parsed), CFG).where("metric = 'requests'").collect()
    early = [r for r in scored if r.hour_ts.day <= 3]          # at most 2 previous days < min_baseline_points
    assert early and all(r.z_score is None for r in early)


def test_detect_matches_the_contract_table(parsed):
    conform(anomalies.detect(parsed, CFG), "anomalies")
