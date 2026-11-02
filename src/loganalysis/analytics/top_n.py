"""Top-N analytics (M5c): the most requested resources and the most active hosts.

    python -m loganalysis.analytics.top_n --env local|cluster

Reads   paths.parsed
Writes  top_resources_by_hits, top_resources_by_bytes, top_hosts, resource_type_breakdown
N is config top_n. Ties break on the key ascending, exactly like the MapReduce top_resources job, so
scripts/compare_engines.py can compare the two lists row for row.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loganalysis.analytics.common import pct_of_total, top_k
from loganalysis.common.io import read_parsed, write_table
from loganalysis.common.job import run_job


def top_resources_by_hits(parsed: DataFrame, n: int) -> DataFrame:
    counts = parsed.groupBy("resource").agg(F.count(F.lit(1)).alias("requests"))
    return top_k(counts, "requests", "resource", n)


def top_resources_by_bytes(parsed: DataFrame, n: int) -> DataFrame:
    totals = parsed.groupBy("resource").agg(F.sum("bytes").alias("bytes"))
    return top_k(totals, "bytes", "resource", n)


def top_hosts(parsed: DataFrame, n: int) -> DataFrame:
    """The n hosts with the most requests, with the bytes they downloaded."""
    totals = parsed.groupBy("host").agg(F.count(F.lit(1)).alias("requests"), F.sum("bytes").alias("bytes"))
    return top_k(totals, "requests", "host", n)


def resource_type_breakdown(parsed: DataFrame) -> DataFrame:
    """Every resource type (file extension, 'directory' or 'none'): requests, bytes and share of requests."""
    return (parsed.groupBy("resource_type").agg(F.count(F.lit(1)).alias("requests"), F.sum("bytes").alias("bytes"))
            .withColumn("pct", pct_of_total("requests")))


def compute(parsed: DataFrame, cfg) -> dict:
    n = cfg["top_n"]
    return {
        "top_resources_by_hits": top_resources_by_hits(parsed, n),
        "top_resources_by_bytes": top_resources_by_bytes(parsed, n),
        "top_hosts": top_hosts(parsed, n),
        "resource_type_breakdown": resource_type_breakdown(parsed),
    }


def run(spark: SparkSession, cfg):
    parsed = read_parsed(spark, cfg).select("resource", "host", "bytes", "resource_type").cache()
    try:
        for name, df in compute(parsed, cfg).items():
            write_table(df, cfg, name)
    finally:
        parsed.unpersist()


if __name__ == "__main__":
    run_job("top_n", run, __doc__)
