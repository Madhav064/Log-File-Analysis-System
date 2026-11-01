"""Spark ETL (M4): raw log text -> parsed Parquet (partitioned by log_date) + rejected lines + quality report.

    python -m loganalysis.etl.parse_logs --env local|cluster

Reads   paths.raw_glob
Writes  paths.parsed    (§6.2, partitioned by log_date)
        paths.rejected  (§6.3)
        paths.quality   (quality_report, one row for this run)
"""
from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loganalysis.common.io import write_dataset, write_table
from loganalysis.common.job import run_job
from loganalysis.common.log import get_logger
from loganalysis.parsing.spark_parser import annotate, parsed_records, rejected_records

log = get_logger("etl")


def read_raw(spark: SparkSession, raw_glob: str) -> DataFrame:
    """One row per line: `value` (decoded as UTF-8, invalid bytes become U+FFFD) and the file's base name."""
    return spark.read.text(raw_glob).select(
        "value", F.regexp_extract(F.input_file_name(), r"([^/\\]+)$", 1).alias("source_file"))


def quality_report(annotated: DataFrame, run_at: datetime) -> DataFrame:
    """One row: line counts, rejections by reason, the time range and unique hosts of the parsed lines."""
    ok = F.col("reason").isNull()
    totals = annotated.agg(
        F.count(F.lit(1)).alias("total_lines"),
        F.count(F.when(ok, 1)).alias("parsed"),
        F.count(F.when(~ok, 1)).alias("rejected"),
        F.min(F.when(ok, F.col("ts"))).alias("min_ts"),
        F.max(F.when(ok, F.col("ts"))).alias("max_ts"),
        F.countDistinct(F.when(ok, F.col("host"))).alias("unique_hosts"),
    )
    reasons = (annotated.where(~ok).groupBy("reason").count()
               .agg(F.map_from_entries(F.collect_list(F.struct("reason", "count"))).alias("rejected_by_reason")))
    return totals.crossJoin(reasons).select(
        "total_lines", "parsed", "rejected",
        F.when(F.col("total_lines") > 0, F.col("rejected") * 100.0 / F.col("total_lines"))
         .otherwise(F.lit(0.0)).alias("reject_pct"),
        "rejected_by_reason", "min_ts", "max_ts", "unique_hosts",
        F.lit(run_at).cast("timestamp").alias("run_at"),
    )


def run(spark: SparkSession, cfg):
    paths = cfg["paths"]
    annotated = annotate(read_raw(spark, paths["raw_glob"])).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        # One file per day instead of one per (input split x day).
        write_dataset(parsed_records(annotated).repartition("log_date"), paths["parsed"], partition_by=["log_date"])
        write_dataset(rejected_records(annotated).coalesce(1), paths["rejected"])
        report = quality_report(annotated, datetime.now(timezone.utc))
        write_table(report, cfg, "quality_report")
    finally:
        annotated.unpersist()

    # timestamps formatted by Spark, i.e. in the session (server) time zone, not the driver process's zone
    fmt = "yyyy-MM-dd HH:mm:ss XXX"
    r = spark.read.parquet(paths["quality"]).select(
        "*", F.date_format("min_ts", fmt).alias("first"), F.date_format("max_ts", fmt).alias("last")).first()
    log.info("lines %d = parsed %d + rejected %d (%.4f%%) %s; %d unique hosts; %s .. %s",
             r.total_lines, r.parsed, r.rejected, r.reject_pct, dict(r.rejected_by_reason or {}),
             r.unique_hosts, r.first, r.last)
    if r.parsed + r.rejected != r.total_lines:
        raise RuntimeError("parsed + rejected != total lines")


if __name__ == "__main__":
    run_job("etl", run, __doc__)
