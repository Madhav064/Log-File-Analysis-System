"""Anomaly detection (M5d, plan P8): hours whose traffic or error rate is far from normal, and hours with no data.

    python -m loganalysis.analytics.anomalies --env local|cluster

Reads   paths.parsed
Writes  anomalies (flagged hours only)

Method, per metric (`requests`, `error_rate`):
1. A complete hourly calendar from the first to the last hour of data, so an hour without requests is a row
   with 0 instead of a missing row. Such an hour is a `gap` (missing data or an outage).
2. Baseline = mean and sample std of the same hour of day over the previous `baseline_days` days. Because the
   calendar is complete, "the previous N rows of this hour of day" is exactly "the previous N days".
   Gap hours, and for error_rate hours with fewer than `min_requests` requests, are left out of the baseline,
   so an outage's zeros do not distort the following week.
3. z = (value - mean) / std. z > z_threshold is a `spike`, z < -z_threshold a `drop`. An hour is not scored
   when its baseline has fewer than `min_baseline_points` values, when std is 0, or (error_rate) when it has
   fewer than `min_requests` requests.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from loganalysis.analytics.common import hour_bucket
from loganalysis.common.io import read_parsed, write_table
from loganalysis.common.job import run_job


def hourly_calendar(parsed: DataFrame) -> DataFrame:
    """Every hour from the first to the last hour with data: hour_ts, requests, errors (0 when there was none)."""
    hourly = parsed.groupBy(hour_bucket(F.col("ts")).alias("hour_ts")).agg(
        F.count(F.lit(1)).alias("requests"), F.count(F.when(F.col("is_error"), 1)).alias("errors"))
    hours = hourly.agg(F.sequence(F.min("hour_ts"), F.max("hour_ts"), F.expr("INTERVAL 1 HOUR")).alias("h")) \
        .select(F.explode("h").alias("hour_ts"))
    return hours.join(hourly, "hour_ts", "left").fillna(0, subset=["requests", "errors"])


def _metric_rows(calendar: DataFrame, min_requests: int) -> DataFrame:
    """Long format: one row per (hour, metric) with its value and whether it may be scored / used in a baseline."""
    has_traffic = F.col("requests") > 0
    requests = calendar.select(
        "hour_ts", F.lit("requests").alias("metric"), F.col("requests").cast("double").alias("value"),
        has_traffic.alias("eligible"))
    error_rate = calendar.select(
        "hour_ts", F.lit("error_rate").alias("metric"),
        F.when(has_traffic, F.col("errors") / F.col("requests")).alias("value"),
        (has_traffic & (F.col("requests") >= min_requests)).alias("eligible"))
    return requests.unionByName(error_rate)


def score(calendar: DataFrame, cfg) -> DataFrame:
    """Every (hour, metric) with its baseline, z-score and kind ('spike', 'drop' or null)."""
    a = cfg["anomaly"]
    rows = _metric_rows(calendar, a["min_requests"]).withColumn("hour_of_day", F.hour("hour_ts"))
    in_baseline = F.when(F.col("eligible"), F.col("value"))
    w = Window.partitionBy("metric", "hour_of_day").orderBy("hour_ts").rowsBetween(-a["baseline_days"], -1)
    scored = rows.select(
        "*",
        F.avg(in_baseline).over(w).alias("baseline_mean"),
        F.stddev_samp(in_baseline).over(w).alias("baseline_std"),
        F.count(in_baseline).over(w).alias("baseline_points"),
    )
    scoreable = F.col("eligible") & (F.col("baseline_points") >= a["min_baseline_points"]) & (F.col("baseline_std") > 0)
    z = F.when(scoreable, (F.col("value") - F.col("baseline_mean")) / F.col("baseline_std"))
    threshold = float(a["z_threshold"])
    return scored.withColumn("z_score", z).withColumn(
        "kind", F.when(F.col("z_score") > threshold, "spike").when(F.col("z_score") < -threshold, "drop"))


def detect(parsed: DataFrame, cfg) -> DataFrame:
    """The anomalies table: gap hours plus every spike and drop."""
    calendar = hourly_calendar(parsed)
    gaps = calendar.where(F.col("requests") == 0).select(
        F.lit("requests").alias("metric"), "hour_ts", F.lit(0.0).alias("value"),
        F.lit(None).cast("double").alias("baseline_mean"), F.lit(None).cast("double").alias("baseline_std"),
        F.lit(None).cast("double").alias("z_score"), F.lit("gap").alias("kind"))
    flagged = score(calendar, cfg).where(F.col("kind").isNotNull()).select(
        "metric", "hour_ts", "value", "baseline_mean", "baseline_std", "z_score", "kind")
    return gaps.unionByName(flagged)


def run(spark: SparkSession, cfg):
    parsed = read_parsed(spark, cfg).select("ts", "is_error")
    write_table(detect(parsed, cfg), cfg, "anomalies")


if __name__ == "__main__":
    run_job("anomalies", run, __doc__)
