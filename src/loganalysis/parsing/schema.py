"""Column names, rejection reasons and Spark schemas for parsed and rejected records (plan §6.2, §6.3).

Pure Python at import time: pyspark is imported only inside the *_struct() functions, because this package
is zipped and shipped to the Hadoop Streaming nodes, where pyspark is not on the path.
"""

# Rejection reasons, in the order the checks run. The first failing check names the reason.
NO_MATCH = "NO_MATCH"            # the line is not Common Log Format at all
BAD_TIMESTAMP = "BAD_TIMESTAMP"  # the [timestamp] is not a real date/time
BAD_STATUS = "BAD_STATUS"        # the status is three digits but outside 100-599
BAD_REQUEST = "BAD_REQUEST"      # the quoted request has no method + resource (e.g. "-")
REJECT_REASONS = (NO_MATCH, BAD_TIMESTAMP, BAD_STATUS, BAD_REQUEST)

MIN_STATUS, MAX_STATUS = 100, 599
ERROR_STATUS = 400               # plan P5: an error is any status >= 400
SERVER_ERROR_STATUS = 500

PARSED_COLUMNS = (
    "host", "ts", "log_date", "log_hour", "method", "resource", "protocol", "status", "status_class",
    "bytes", "bytes_missing", "is_error", "resource_type",
)
REJECTED_COLUMNS = ("raw_line", "reason", "source_file")


def status_class(status):
    """200 -> '2xx'."""
    return f"{status // 100}xx"


def parsed_struct():
    """Spark StructType of a parsed record, in PARSED_COLUMNS order."""
    from pyspark.sql import types as T

    return T.StructType([
        T.StructField("host", T.StringType(), False),
        T.StructField("ts", T.TimestampType(), False),
        T.StructField("log_date", T.DateType(), False),
        T.StructField("log_hour", T.IntegerType(), False),
        T.StructField("method", T.StringType(), False),
        T.StructField("resource", T.StringType(), False),
        T.StructField("protocol", T.StringType(), True),
        T.StructField("status", T.IntegerType(), False),
        T.StructField("status_class", T.StringType(), False),
        T.StructField("bytes", T.LongType(), False),
        T.StructField("bytes_missing", T.BooleanType(), False),
        T.StructField("is_error", T.BooleanType(), False),
        T.StructField("resource_type", T.StringType(), False),
    ])


def rejected_struct():
    """Spark StructType of a rejected record, in REJECTED_COLUMNS order."""
    from pyspark.sql import types as T

    return T.StructType([
        T.StructField("raw_line", T.StringType(), False),
        T.StructField("reason", T.StringType(), False),
        T.StructField("source_file", T.StringType(), True),
    ])
