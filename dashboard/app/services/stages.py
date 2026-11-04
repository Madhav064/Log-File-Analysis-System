"""The ten dashboard stages, in pipeline order: what each one shows and the technical facts in its
"Stage details" panel. Paths come from config/pipeline.yaml (cluster env) and schemas from loganalysis.contracts,
so nothing here can drift from the pipeline itself."""
from loganalysis.common.config import load_config
from loganalysis.contracts import REPORTS, TABLES

ANALYTICS = "analytics"


def _stages(paths):
    analytics = paths["analytics"]
    return [
        dict(slug="overview", number="01", group="Overview", title="Overview",
             summary="The headline numbers of the whole analysis and the state of every pipeline stage.",
             engine="All stages", jobs=[], input="data/serving/*.json", output="(this page)",
             tables=["quality_report", "traffic_daily", "status_counts", "traffic_peaks"], step=None),
        dict(slug="architecture", number="02", group="Overview", title="Architecture",
             summary="How the data flows from the raw NASA logs through HDFS, Spark and MapReduce to this dashboard.",
             engine="Docker Compose (lite profile)", jobs=[], input="infra/docker-compose.yml",
             output="(this page)", tables=[], step=None),
        dict(slug="ingestion", number="03", group="Data pipeline", title="Ingestion (HDFS)",
             summary="Download the two log files, verify their checksums, and store them in HDFS.",
             engine="HDFS (Hadoop 3.4.1)", jobs=[("ingest", "loganalysis.ingestion.hdfs_loader")],
             input="NASA_access_log_Jul95.gz, NASA_access_log_Aug95.gz", output=paths["raw"],
             tables=["ingestion_report"], step="ingest"),
        dict(slug="etl", number="04", group="Data pipeline", title="Parsing & ETL",
             summary="Turn each raw text line into typed fields, set aside lines that cannot be read, and report "
                     "data quality.",
             engine="Spark 3.5.3 (PySpark)", jobs=[("etl", "loganalysis.etl.parse_logs")],
             input=paths["raw_glob"], output=f"{paths['parsed']} (Parquet, by log_date), {paths['rejected']}, "
                                             f"{paths['quality']}",
             tables=["quality_report"], step="etl"),
        dict(slug="errors", number="05", group="Analytics", title="Errors",
             summary="How often requests failed (HTTP status 400 or higher), when, and for which pages and hosts.",
             engine="Spark 3.5.3 (PySpark)", jobs=[(ANALYTICS, "loganalysis.analytics.errors")],
             input=paths["parsed"], output=f"{analytics}/<table>",
             tables=["status_counts", "error_timeline_hourly", "top_error_resources", "top_error_hosts"],
             step="analytics"),
        dict(slug="traffic", number="06", group="Analytics", title="Traffic",
             summary="When the site was busy: requests per hour and day, daily and weekly rhythm, busiest periods.",
             engine="Spark 3.5.3 (PySpark)", jobs=[(ANALYTICS, "loganalysis.analytics.traffic")],
             input=paths["parsed"], output=f"{analytics}/<table>",
             tables=["traffic_hourly", "traffic_daily", "hour_of_day_profile", "day_of_week_profile",
                     "traffic_peaks"], step="analytics"),
        dict(slug="top-n", number="07", group="Analytics", title="Top N",
             summary="The most requested pages and files, the most active hosts, and the mix of file types.",
             engine="Spark 3.5.3 (PySpark)", jobs=[(ANALYTICS, "loganalysis.analytics.top_n")],
             input=paths["parsed"], output=f"{analytics}/<table>",
             tables=["top_resources_by_hits", "top_resources_by_bytes", "top_hosts", "resource_type_breakdown"],
             step="analytics"),
        dict(slug="anomalies", number="08", group="Analytics", title="Anomalies",
             summary="Hours that were unusually busy, unusually quiet, or had no data at all.",
             engine="Spark 3.5.3 (PySpark)", jobs=[(ANALYTICS, "loganalysis.analytics.anomalies")],
             input=paths["parsed"], output=f"{analytics}/anomalies", tables=["anomalies"], step="analytics"),
        dict(slug="engines", number="09", group="Validation", title="Engine comparison",
             summary="The same three counts computed by Hadoop MapReduce and by Spark: do they agree, and which "
                     "is faster?",
             engine="Hadoop MapReduce on YARN vs Spark",
             jobs=[("mapreduce", "mapreduce/run.py"), ("compare", "scripts/compare_engines.py")],
             input=f"{paths['raw_glob']} (MapReduce), {analytics} (Spark)",
             output=f"{paths['mapreduce']}/<job>, {analytics}/engine_benchmark",
             tables=["engine_benchmark"], step="mapreduce"),
        dict(slug="export", number="10", group="Serving", title="Export & API",
             summary="How the results are copied into small JSON files and served to this dashboard.",
             engine="Spark export + Flask", jobs=[("export", "loganalysis.export.to_serving")],
             input=analytics, output=paths["serving"], tables=[], step="export"),
    ]


STAGES = _stages(load_config("cluster")["paths"])
BY_SLUG = {s["slug"]: s for s in STAGES}

# The stepper along the top: the pipeline itself, each step opening the page that shows it.
STEPPER = [("Source", "ingestion"), ("Ingestion (HDFS)", "ingestion"), ("Parsing & ETL", "etl"),
           ("Analytics (Spark)", "errors"), ("MapReduce comparison", "engines"), ("Export (JSON)", "export"),
           ("Dashboard (Flask API)", "export")]


def schema(name):
    """[(column, type)] for a table or report, from the contracts."""
    return list((TABLES.get(name) or REPORTS[name]).columns)


def neighbours(slug):
    i = [s["slug"] for s in STAGES].index(slug)
    return (STAGES[i - 1] if i > 0 else None), (STAGES[i + 1] if i < len(STAGES) - 1 else None)
