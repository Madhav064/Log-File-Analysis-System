"""The Spark twin of parser.parse_line: native regexp_extract/rlike over the same patterns, no Python UDF.

    annotated = annotate(lines)          # every input line, with parsed columns + `reason` (null when parsed)
    parsed    = parsed_records(annotated)
    rejected  = rejected_records(annotated)

The checks run in the same order as parse_line, so both engines give every line the same verdict
(tests/unit/test_parser_parity.py). log_date and log_hour are taken in the Spark session time zone, which
`common.spark.get_spark` sets to the server's zone (plan P4); for this dataset that equals the line's own offset.
Needs `spark.sql.legacy.timeParserPolicy=CORRECTED` (also set by get_spark) so an impossible date such as
32/Jul becomes null instead of raising SparkUpgradeException.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .log_pattern import LOG_PATTERN, REQUEST_PATTERN, SPARK_TIMESTAMP_FORMAT, TIMESTAMP_PATTERN
from .schema import (
    BAD_REQUEST,
    BAD_STATUS,
    BAD_TIMESTAMP,
    ERROR_STATUS,
    MAX_STATUS,
    MIN_STATUS,
    NO_MATCH,
    PARSED_COLUMNS,
    REJECTED_COLUMNS,
    parsed_struct,
)

_LOG_GROUPS = ("host", "ident", "user", "raw_ts", "request", "raw_status", "raw_bytes")


def _group(col: Column, pattern: str, index: int) -> Column:
    return F.regexp_extract(col, pattern, index)


def resource_type_col(resource: Column) -> Column:
    """Same rules as parser.resource_type."""
    name = F.substring_index(resource, "/", -1)
    return (F.when(resource.endswith("/"), F.lit("directory"))
            .when(name.contains("."), F.lower(F.substring_index(name, ".", -1)))
            .otherwise(F.lit("none")))


def annotate(lines: DataFrame, line_col: str = "value", source_col: str | None = "source_file",
             keep: tuple = ()) -> DataFrame:
    """One row per input line: the PARSED_COLUMNS, `reason` (null when the line parsed), `raw_line`,
    `source_file`, and any `keep` columns carried through unchanged (e.g. a line id).

    raw_line is kept only on rejected rows, so a cached `annotated` frame stays small.
    """
    line = F.regexp_replace(F.col(line_col), r"[\r\n]+$", "")
    source = F.col(source_col) if source_col else F.lit(None).cast("string")
    df = lines.select(line.alias("_line"), source.alias("source_file"), *keep)

    df = df.select("*", *[_group(F.col("_line"), LOG_PATTERN, i).alias(n) for i, n in enumerate(_LOG_GROUPS, 1)])
    ts = F.when(F.col("raw_ts").rlike(TIMESTAMP_PATTERN), F.to_timestamp("raw_ts", SPARK_TIMESTAMP_FORMAT))
    status = F.col("raw_status").cast("int")
    df = df.select("*", ts.alias("ts"), status.alias("status"),
                   *[_group(F.col("request"), REQUEST_PATTERN, i).alias(n)
                     for i, n in enumerate(("method", "_target", "_protocol"), 1)])

    reason = (F.when(~F.col("_line").rlike(LOG_PATTERN), F.lit(NO_MATCH))
              .when(F.col("ts").isNull(), F.lit(BAD_TIMESTAMP))
              .when(~F.col("status").between(MIN_STATUS, MAX_STATUS), F.lit(BAD_STATUS))
              .when(~F.col("request").rlike(REQUEST_PATTERN), F.lit(BAD_REQUEST)))
    resource = F.substring_index(F.col("_target"), "?", 1)
    bytes_missing = F.col("raw_bytes") == "-"

    return df.select(
        F.col("host"),
        F.col("ts"),
        F.to_date("ts").alias("log_date"),
        F.hour("ts").alias("log_hour"),
        F.col("method"),
        resource.alias("resource"),
        F.when(F.col("_protocol") != "", F.col("_protocol")).alias("protocol"),
        F.col("status"),
        F.concat(F.floor(F.col("status") / 100).cast("string"), F.lit("xx")).alias("status_class"),
        F.when(bytes_missing, F.lit(0)).otherwise(F.col("raw_bytes").cast("long")).cast("long").alias("bytes"),
        bytes_missing.alias("bytes_missing"),
        (F.col("status") >= ERROR_STATUS).alias("is_error"),
        resource_type_col(resource).alias("resource_type"),
        reason.alias("reason"),
        F.when(reason.isNotNull(), F.col("_line")).alias("raw_line"),
        F.col("source_file"),
        *keep,
    )


def parsed_records(annotated: DataFrame) -> DataFrame:
    """The lines that parsed, with exactly the §6.2 columns and types."""
    struct = parsed_struct()
    return annotated.where(F.col("reason").isNull()).select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in struct.fields])


def rejected_records(annotated: DataFrame) -> DataFrame:
    """The lines that did not parse: raw_line, reason, source_file (§6.3)."""
    return annotated.where(F.col("reason").isNotNull()).select(*REJECTED_COLUMNS)


__all__ = ["PARSED_COLUMNS", "annotate", "parsed_records", "rejected_records", "resource_type_col"]
