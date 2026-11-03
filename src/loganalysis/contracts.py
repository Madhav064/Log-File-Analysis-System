"""The output tables of the Spark jobs (plan §6.4): name, storage layer, and columns with their Spark types.

This is the code-level form of the contract. Every job writes through `common.io.write_table`, which conforms
the DataFrame to these columns (order and types) and fails on any mismatch; the tests check the same thing;
the export (C7) and the dashboard's mock data can import TABLES instead of re-typing the column lists.
Pure Python, no Spark import.

Units used across tables:
- `pct` columns are percentages, 0-100.  `error_rate` is a ratio, 0-1.
- Timestamps are instants; the jobs bucket them in the server's time zone (plan P4).
- `day_of_week` is ISO: 1 = Monday ... 7 = Sunday.
"""
from collections import namedtuple

Table = namedtuple("Table", "layer columns built_by")

# layer "analytics" -> <paths.analytics>/<name>;  layer "quality" -> <paths.quality> (plan §6.1)
TABLES = {
    "quality_report": Table("quality", (
        ("total_lines", "bigint"), ("parsed", "bigint"), ("rejected", "bigint"), ("reject_pct", "double"),
        ("rejected_by_reason", "map<string,bigint>"), ("min_ts", "timestamp"), ("max_ts", "timestamp"),
        ("unique_hosts", "bigint"), ("run_at", "timestamp"),
    ), "loganalysis.etl.parse_logs"),

    # M5a errors
    "status_counts": Table("analytics", (
        ("status", "int"), ("status_class", "string"), ("requests", "bigint"), ("pct", "double"),
    ), "loganalysis.analytics.errors"),
    "error_timeline_hourly": Table("analytics", (
        ("hour_ts", "timestamp"), ("requests", "bigint"), ("errors_4xx", "bigint"), ("errors_5xx", "bigint"),
        ("error_rate", "double"),
    ), "loganalysis.analytics.errors"),
    "top_error_resources": Table("analytics", (       # top N per error status
        ("resource", "string"), ("status", "int"), ("requests", "bigint"),
    ), "loganalysis.analytics.errors"),
    "top_error_hosts": Table("analytics", (
        ("host", "string"), ("errors", "bigint"),
    ), "loganalysis.analytics.errors"),

    # M5b traffic
    "traffic_hourly": Table("analytics", (
        ("hour_ts", "timestamp"), ("requests", "bigint"), ("bytes", "bigint"), ("unique_hosts", "bigint"),
    ), "loganalysis.analytics.traffic"),
    "traffic_daily": Table("analytics", (
        ("log_date", "date"), ("day_of_week", "int"), ("requests", "bigint"), ("bytes", "bigint"),
        ("unique_hosts", "bigint"),
    ), "loganalysis.analytics.traffic"),
    "hour_of_day_profile": Table("analytics", (       # averages divide by the days that have traffic
        ("hour", "int"), ("total_requests", "bigint"), ("avg_requests_per_day", "double"),
    ), "loganalysis.analytics.traffic"),
    "day_of_week_profile": Table("analytics", (       # averages divide by the days that have traffic
        ("day_of_week", "int"), ("total_requests", "bigint"), ("avg_requests", "double"),
    ), "loganalysis.analytics.traffic"),
    "traffic_peaks": Table("analytics", (             # top K (config peaks_top_k) per granularity
        ("granularity", "string"), ("period_start", "timestamp"), ("requests", "bigint"), ("rank", "int"),
    ), "loganalysis.analytics.traffic"),

    # M5c top-N
    "top_resources_by_hits": Table("analytics", (
        ("resource", "string"), ("requests", "bigint"),
    ), "loganalysis.analytics.top_n"),
    "top_resources_by_bytes": Table("analytics", (
        ("resource", "string"), ("bytes", "bigint"),
    ), "loganalysis.analytics.top_n"),
    "top_hosts": Table("analytics", (
        ("host", "string"), ("requests", "bigint"), ("bytes", "bigint"),
    ), "loganalysis.analytics.top_n"),
    "resource_type_breakdown": Table("analytics", (
        ("resource_type", "string"), ("requests", "bigint"), ("bytes", "bigint"), ("pct", "double"),
    ), "loganalysis.analytics.top_n"),

    # M5d anomalies: flagged hours only (spike / drop / gap)
    "anomalies": Table("analytics", (
        ("metric", "string"), ("hour_ts", "timestamp"), ("value", "double"), ("baseline_mean", "double"),
        ("baseline_std", "double"), ("z_score", "double"), ("kind", "string"),
    ), "loganalysis.analytics.anomalies"),

    # M9, written by scripts/compare_engines.py
    "engine_benchmark": Table("analytics", (
        ("job", "string"), ("engine", "string"), ("runtime_sec", "double"), ("output_rows", "bigint"),
        ("results_match", "boolean"),
    ), "scripts/compare_engines.py"),
}

GRANULARITIES = ("minute", "hour", "day")          # traffic_peaks.granularity
METRICS = ("requests", "error_rate")               # anomalies.metric
ANOMALY_KINDS = ("spike", "drop", "gap")           # anomalies.kind


def ddl(name):
    """'status int, status_class string, ...' for spark.createDataFrame(rows, ddl(name))."""
    return ", ".join(f"{col} {typ}" for col, typ in TABLES[name].columns)


def column_names(name):
    return tuple(col for col, _ in TABLES[name].columns)
