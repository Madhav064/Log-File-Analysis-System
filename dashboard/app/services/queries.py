"""Shape the exported tables into what each API endpoint returns. Pure functions over Repository rows.

Timestamps in the serving files are ISO strings in server time (1995-07-13T09:00:00-04:00), so the first ten
characters are the server-local date and date filtering is a string comparison.
"""
from datetime import date, datetime, timedelta

RESOURCE_TYPES_SHOWN = 8   # resource_type_breakdown has dozens of rare extensions; the rest become "other"


def local_date(ts):
    return date.fromisoformat(ts[:10])


def data_range(repo):
    """(first, last) server-local dates covered by the data, from the quality report."""
    q = repo.rows("quality_report")[0]
    return local_date(q["min_ts"]), local_date(q["max_ts"])


def in_range(rows, key, start, end):
    return [r for r in rows if start <= local_date(r[key]) <= end]


def _offset(ts):
    return ts[19:]   # "-04:00"


def fill_hours(rows, key, start, end, fields):
    """Every hour from start 00:00 to end 23:00, with None where the table has no row (hours without traffic
    are missing from the hourly tables, so a chart would otherwise draw straight across a gap)."""
    if not rows:
        return []
    by_ts = {r[key]: r for r in rows}
    offset = _offset(rows[0][key])
    out, t = [], datetime.combine(start, datetime.min.time())
    while t.date() <= end:
        ts = t.strftime("%Y-%m-%dT%H:%M:%S") + offset
        out.append(by_ts.get(ts) or {key: ts, **{f: None for f in fields}})
        t += timedelta(hours=1)
    return out


def fill_days(rows, key, start, end, fields):
    by_day = {r[key]: r for r in rows}
    out, d = [], start
    while d <= end:
        out.append(by_day.get(d.isoformat()) or {key: d.isoformat(), **{f: None for f in fields}})
        d += timedelta(days=1)
    return out


def traffic_timeline(repo, granularity, start, end):
    if granularity == "hour":
        rows = in_range(repo.rows("traffic_hourly"), "hour_ts", start, end)
        return "traffic_hourly", fill_hours(rows, "hour_ts", start, end, ("requests", "bytes", "unique_hosts"))
    rows = [r for r in repo.rows("traffic_daily") if start <= date.fromisoformat(r["log_date"]) <= end]
    return "traffic_daily", fill_days(rows, "log_date", start, end,
                                      ("day_of_week", "requests", "bytes", "unique_hosts"))


def error_timeline(repo, granularity, start, end):
    rows = in_range(repo.rows("error_timeline_hourly"), "hour_ts", start, end)
    fields = ("requests", "errors_4xx", "errors_5xx", "error_rate")
    if granularity == "hour":
        return fill_hours(rows, "hour_ts", start, end, fields)
    days = {}
    for r in rows:
        d = days.setdefault(r["hour_ts"][:10], {"log_date": r["hour_ts"][:10], "requests": 0, "errors_4xx": 0,
                                                "errors_5xx": 0})
        for f in ("requests", "errors_4xx", "errors_5xx"):
            d[f] += r[f]
    for d in days.values():
        d["error_rate"] = (d["errors_4xx"] + d["errors_5xx"]) / d["requests"] if d["requests"] else None
    return fill_days(sorted(days.values(), key=lambda d: d["log_date"]), "log_date", start, end, fields)


def status_classes(status_rows):
    """Requests per status class (2xx ... 5xx) with their codes, for the doughnut and its table."""
    total = sum(r["requests"] for r in status_rows) or 1
    classes = {}
    for r in sorted(status_rows, key=lambda r: r["status"]):
        c = classes.setdefault(r["status_class"], {"status_class": r["status_class"], "requests": 0, "codes": []})
        c["requests"] += r["requests"]
        c["codes"].append(r["status"])
    for c in classes.values():
        c["pct"] = 100 * c["requests"] / total
    return sorted(classes.values(), key=lambda c: c["status_class"])


def error_rate(status_rows):
    total = sum(r["requests"] for r in status_rows)
    errors = sum(r["requests"] for r in status_rows if r["status"] >= 400)
    return errors / total if total else None


def peaks(repo, granularity=None):
    rows = repo.rows("traffic_peaks")
    if granularity:
        rows = [r for r in rows if r["granularity"] == granularity]
    return sorted(rows, key=lambda r: (r["granularity"], r["rank"]))


def summary(repo):
    """The Overview KPIs. Unique hosts come from quality_report: distinct counts cannot be summed from the
    hourly or daily tables."""
    q = repo.rows("quality_report")[0]
    daily = repo.rows("traffic_daily")
    status = repo.rows("status_counts")
    first, last = local_date(q["min_ts"]), local_date(q["max_ts"])
    top = {r["granularity"]: r for r in peaks(repo) if r["rank"] == 1}
    out = {
        "total_lines": q["total_lines"], "parsed": q["parsed"], "rejected": q["rejected"],
        "reject_pct": q["reject_pct"], "total_requests": sum(r["requests"] for r in daily),
        "total_bytes": sum(r["bytes"] for r in daily), "unique_hosts": q["unique_hosts"],
        "error_rate": error_rate(status), "min_ts": q["min_ts"], "max_ts": q["max_ts"],
        "calendar_days": (last - first).days + 1, "days_with_traffic": len(daily),
        "hours_with_traffic": len(repo.rows("traffic_hourly")),
        "peak_minute": top.get("minute"), "peak_hour": top.get("hour"), "peak_day": top.get("day"),
        "busiest_hour_of_day": max(repo.rows("hour_of_day_profile"), key=lambda r: r["total_requests"])["hour"],
        "status_classes": status_classes(status),
    }
    if repo.available("anomalies"):
        out["anomaly_counts"] = anomaly_counts(repo.rows("anomalies"))
    return out


def anomaly_counts(rows):
    counts = {}
    for r in rows:
        key = f"{r['metric']}:{r['kind']}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def gap_runs(anomaly_rows):
    """Consecutive gap hours merged into runs: [{start, end, hours}]. The real data has two: the cut-off July
    file (28 Jul 14:00 to 31 Jul 23:00) and the hurricane outage (1 Aug 15:00 to 3 Aug 03:00)."""
    hours = sorted(r["hour_ts"] for r in anomaly_rows if r["kind"] == "gap")
    runs = []
    for ts in hours:
        t = datetime.fromisoformat(ts)
        if runs and t - datetime.fromisoformat(runs[-1]["end"]) == timedelta(hours=1):
            runs[-1]["end"], runs[-1]["hours"] = ts, runs[-1]["hours"] + 1
        else:
            runs.append({"start": ts, "end": ts, "hours": 1})
    for run in runs:
        run["cause"] = gap_cause(run)
    return runs


def gap_cause(run):
    """'missing_data' when the run lasts until the end of a month: the logs come as one file per month, so the
    hours were never in any file (the July file stops on 28 Jul). Otherwise 'outage': inside a file, the
    server itself logged nothing (the hurricane shutdown of 1-3 Aug)."""
    after = datetime.fromisoformat(run["end"]) + timedelta(hours=1)
    return "missing_data" if after.month != datetime.fromisoformat(run["end"]).month else "outage"


def top_n(rows, key, n):
    return sorted(rows, key=lambda r: -r[key])[:n]


def resource_types(rows):
    """All types sorted by requests, plus a chart series with the long tail grouped into 'other'."""
    rows = sorted(rows, key=lambda r: -r["requests"])
    head, tail = rows[:RESOURCE_TYPES_SHOWN], rows[RESOURCE_TYPES_SHOWN:]
    chart = [{"resource_type": r["resource_type"], "requests": r["requests"], "pct": r["pct"]} for r in head]
    if tail:
        chart.append({"resource_type": f"other ({len(tail)} types)", "requests": sum(r["requests"] for r in tail),
                      "pct": sum(r["pct"] for r in tail)})
    return rows, chart
