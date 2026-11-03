"""Error analytics (M5a): how often requests fail, when, on which resources and from which hosts.

    python -m loganalysis.analytics.errors --env local|cluster

Reads   paths.parsed
Writes  status_counts, error_timeline_hourly, top_error_resources (top N per error status), top_error_hosts
An error is any status >= 400 (plan P5); 4xx are client errors, 5xx server errors.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loganalysis.analytics.common import hour_bucket, pct_of_total, top_k
from loganalysis.common.io import read_parsed, write_table
from loganalysis.common.job import run_job
from loganalysis.parsing.schema import ERROR_STATUS, SERVER_ERROR_STATUS


def status_counts(parsed: DataFrame) -> DataFrame:
    """Requests per HTTP status, with its class and share of all requests."""
    return (parsed.groupBy("status", "status_class").agg(F.count(F.lit(1)).alias("requests"))
            .withColumn("pct", pct_of_total("requests")))


def error_timeline_hourly(parsed: DataFrame) -> DataFrame:
    """Per hour with traffic: requests, 4xx, 5xx and the error rate (errors / requests, 0-1)."""
    client = F.col("status").between(ERROR_STATUS, SERVER_ERROR_STATUS - 1)
    server = F.col("status") >= SERVER_ERROR_STATUS
    return (parsed.groupBy(hour_bucket(F.col("ts")).alias("hour_ts"))
            .agg(F.count(F.lit(1)).alias("requests"),
                 F.count(F.when(client, 1)).alias("errors_4xx"),
                 F.count(F.when(server, 1)).alias("errors_5xx"))
            .withColumn("error_rate", (F.col("errors_4xx") + F.col("errors_5xx")) / F.col("requests")))


def top_error_resources(parsed: DataFrame, n: int) -> DataFrame:
    """For every error status, the n resources that returned it most often (404 = broken links)."""
    counts = parsed.where(F.col("is_error")).groupBy("status", "resource").agg(F.count(F.lit(1)).alias("requests"))
    return top_k(counts, "requests", "resource", n, partition_by=["status"])


def top_error_hosts(parsed: DataFrame, n: int) -> DataFrame:
    """The n hosts that received the most error responses."""
    counts = parsed.where(F.col("is_error")).groupBy("host").agg(F.count(F.lit(1)).alias("errors"))
    return top_k(counts, "errors", "host", n)


def compute(parsed: DataFrame, cfg) -> dict:
    return {
        "status_counts": status_counts(parsed),
        "error_timeline_hourly": error_timeline_hourly(parsed),
        "top_error_resources": top_error_resources(parsed, cfg["top_n"]),
        "top_error_hosts": top_error_hosts(parsed, cfg["top_n"]),
    }


def run(spark: SparkSession, cfg):
    for name, df in compute(read_parsed(spark, cfg), cfg).items():
        write_table(df, cfg, name)


if __name__ == "__main__":
    run_job("errors", run, __doc__)
