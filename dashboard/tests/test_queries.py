"""Pure query helpers. Gap runs use the two real holes of the NASA data (hours from the Track B handoff run)."""
from datetime import date

from app.services.queries import fill_days, fill_hours, gap_runs, resource_types


def hours(start_day, start_h, end_day, end_h, month=7):
    out = []
    for d in range(start_day, end_day + 1):
        for h in range(24):
            if (d, h) < (start_day, start_h) or (d, h) > (end_day, end_h):
                continue
            out.append({"hour_ts": f"1995-{month:02d}-{d:02d}T{h:02d}:00:00-04:00", "kind": "gap"})
    return out


def test_the_two_real_gap_runs_are_merged_and_labelled():
    runs = gap_runs(hours(28, 14, 31, 23, 7) + hours(1, 15, 3, 3, 8))
    assert [(r["start"][:13], r["end"][:13], r["hours"], r["cause"]) for r in runs] == [
        ("1995-07-28T14", "1995-07-31T23", 82, "missing_data"),
        ("1995-08-01T15", "1995-08-03T03", 37, "outage")]


def test_fill_hours_and_days_insert_empty_periods():
    rows = [{"hour_ts": "1995-07-01T02:00:00-04:00", "requests": 5}]
    filled = fill_hours(rows, "hour_ts", date(1995, 7, 1), date(1995, 7, 1), ("requests",))
    assert len(filled) == 24 and filled[2]["requests"] == 5 and filled[3]["requests"] is None
    days = fill_days([{"log_date": "1995-07-02", "requests": 1}], "log_date", date(1995, 7, 1), date(1995, 7, 3),
                     ("requests",))
    assert [d["requests"] for d in days] == [None, 1, None]


def test_resource_types_group_the_tail_into_other():
    rows = [{"resource_type": f"t{i}", "requests": 100 - i, "bytes": 0, "pct": 1.0} for i in range(12)]
    every, chart = resource_types(rows)
    assert len(every) == 12 and len(chart) == 9 and chart[-1]["resource_type"] == "other (4 types)"
    assert sum(r["requests"] for r in chart) == sum(r["requests"] for r in rows)
