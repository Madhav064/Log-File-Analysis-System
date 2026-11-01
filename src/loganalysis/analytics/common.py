"""Building blocks shared by the analytics jobs. Pure DataFrame functions, no I/O."""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def hour_bucket(ts: Column) -> Column:
    """Start of the hour, in the session (server) time zone."""
    return F.date_trunc("hour", ts)


def top_k(df: DataFrame, value: str, key: str, k: int, partition_by=None, rank_col=None) -> DataFrame:
    """The k rows with the largest `value` (per `partition_by` group). Ties break on `key` ascending, the same
    rule the MapReduce top_n.py uses, so both engines produce exactly the same list. Adds `rank_col` if given."""
    order = [F.col(value).desc(), F.col(key).asc()]
    if not partition_by and not rank_col:
        return df.orderBy(*order).limit(k)  # a take-ordered: no single-partition window over every row
    w = Window.orderBy(*order)
    if partition_by:
        w = w.partitionBy(*partition_by)
    ranked = df.withColumn("_rank", F.row_number().over(w)).where(F.col("_rank") <= k)
    return ranked.withColumnRenamed("_rank", rank_col) if rank_col else ranked.drop("_rank")


def pct_of_total(value: str) -> Column:
    """value as a percentage (0-100) of its column total. For small, already-aggregated frames only: the
    window has no partition, so Spark moves every row to one task."""
    everything = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    return F.col(value) * 100.0 / F.sum(value).over(everything)
