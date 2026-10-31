"""The one place a SparkSession is built, so every job runs with the same settings.

--env cluster : master, memory and HDFS come from infra/spark/spark-defaults.conf (client container).
--env local   : an in-process local[*] Spark on the local filesystem. This also works inside the client
                container, whose spark-defaults.conf would otherwise send plain paths to HDFS and the driver
                to the "client" hostname.
"""
from pyspark.sql import SparkSession

# Settings every job needs, whatever the environment.
COMMON_CONF = {
    # An impossible date (32/Jul, 25:00) must parse to null. Spark 3's default policy (EXCEPTION) raises
    # SparkUpgradeException instead whenever the old lenient parser would have accepted the string.
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    # Overwriting a partitioned table replaces it entirely, so a re-run never leaves stale partitions behind.
    "spark.sql.sources.partitionOverwriteMode": "static",
}

LOCAL_CONF = {
    "spark.hadoop.fs.defaultFS": "file:///",
    "spark.driver.host": "127.0.0.1",
    "spark.driver.bindAddress": "127.0.0.1",
    "spark.sql.shuffle.partitions": "4",    # the sample is tiny; 200 partitions would only add overhead
    "spark.ui.enabled": "false",
    "spark.ui.showConsoleProgress": "false",
}


def get_spark(app_name, cfg):
    """Build (or reuse) the SparkSession for `cfg["env"]`, in the server's time zone (plan P4)."""
    builder = SparkSession.builder.appName(f"loganalysis-{app_name}")
    conf = {**COMMON_CONF, "spark.sql.session.timeZone": cfg["timezone"]}
    if cfg["env"] == "local":
        builder = builder.master("local[*]")
        conf.update(LOCAL_CONF)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
