"""Traffic analytics (M5b): volume over time, daily and weekly rhythm, and the busiest periods.

    python -m loganalysis.analytics.traffic --env local|cluster

Reads   paths.parsed
Writes  traffic_hourly, traffic_daily, hour_of_day_profile, day_of_week_profile, traffic_peaks
Profile averages divide by the days that have traffic, so the data holes (the cut-off July file, the August
outage) do not drag "a normal day" down. Unique hosts are exact (countDistinct).
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loganalysis.analytics.common import hour_bucket, top_k
from loganalysis.common.io import read_parsed, write_table
from loganalysis.common.job import run_job


def iso_day_of_week(date_col):
    """1 = Monday ... 7 = Sunday (Spark's weekday() is 0 = Monday)."""
    return F.expr(f"weekday({date_col}) + 1")


def _volume(df: DataFrame, *keys) -> DataFrame:
    return df.groupBy(*keys).agg(F.count(F.lit(1)).alias("requests"), F.sum("bytes").alias("bytes"),
                                 F.countDistinct("host").alias("unique_hosts"))


def traffic_hourly(parsed: DataFrame) -> DataFrame:
    """Requests, bytes and unique hosts per hour with traffic."""
    return _volume(parsed, hour_bucket(F.col("ts")).alias("hour_ts"))


def traffic_daily(parsed: DataFrame) -> DataFrame:
    """Requests, bytes and unique hosts per day with traffic, with its ISO day of week."""
    return _volume(parsed, "log_date").withColumn("day_of_week", iso_day_of_week("log_date"))


def hour_of_day_profile(parsed: DataFrame) -> DataFrame:
    """All 24 hours: total requests, and the average per day over the days that have traffic."""
    days = parsed.agg(F.countDistinct("log_date").alias("days"))
    totals = parsed.groupBy(F.col("log_hour").alias("hour")).agg(F.count(F.lit(1)).alias("total_requests"))
    hours = parsed.sparkSession.range(24).select(F.col("id").cast("int").alias("hour"))
    return (hours.join(totals, "hour", "left").crossJoin(days)
            .select("hour", F.coalesce("total_requests", F.lit(0)).alias("total_requests"),
                    (F.coalesce("total_requests", F.lit(0)) / F.col("days")).alias("avg_requests_per_day")))


def day_of_week_profile(daily: DataFrame) -> DataFrame:
    """All 7 ISO weekdays: total requests, and the average over the days of that weekday that have traffic."""
    stats = daily.groupBy("day_of_week").agg(F.sum("requests").alias("total_requests"),
                                             F.count(F.lit(1)).alias("days"))
    weekdays = daily.sparkSession.range(1, 8).select(F.col("id").cast("int").alias("day_of_week"))
    return weekdays.join(stats, "day_of_week", "left").select(
        "day_of_week", F.coalesce("total_requests", F.lit(0)).alias("total_requests"),
        F.coalesce(F.col("total_requests") / F.col("days"), F.lit(0.0)).alias("avg_requests"))


def traffic_peaks(parsed: DataFrame, hourly: DataFrame, daily: DataFrame, k: int) -> DataFrame:
    """The k busiest minutes, hours and days, ranked 1..k (ties: earliest period first)."""
    minutes = (parsed.groupBy(F.date_trunc("minute", "ts").alias("period_start"))
               .agg(F.count(F.lit(1)).alias("requests")))
    periods = (minutes.withColumn("granularity", F.lit("minute"))
               .unionByName(hourly.select(F.col("hour_ts").alias("period_start"), "requests",
                                          F.lit("hour").alias("granularity")))
               .unionByName(daily.select(F.to_timestamp("log_date").alias("period_start"), "requests",
                                         F.lit("day").alias("granularity"))))
    return top_k(periods, "requests", "period_start", k, partition_by=["granularity"], rank_col="rank")


def compute(parsed: DataFrame, cfg) -> dict:
    hourly, daily = traffic_hourly(parsed), traffic_daily(parsed)
    return {
        "traffic_hourly": hourly,
        "traffic_daily": daily,
        "hour_of_day_profile": hour_of_day_profile(parsed),
        "day_of_week_profile": day_of_week_profile(daily),
        "traffic_peaks": traffic_peaks(parsed, hourly, daily, cfg["peaks_top_k"]),
    }


def run(spark: SparkSession, cfg):
    parsed = read_parsed(spark, cfg).select("ts", "log_date", "log_hour", "host", "bytes").cache()
    try:
        for name, df in compute(parsed, cfg).items():
            write_table(df, cfg, name)
    finally:
        parsed.unpersist()


if __name__ == "__main__":
    run_job("traffic", run, __doc__)
