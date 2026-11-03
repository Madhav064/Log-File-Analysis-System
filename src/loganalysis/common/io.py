"""Reading and writing the pipeline's datasets. Jobs never build paths or call .write themselves."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from loganalysis.contracts import TABLES

from .log import get_logger

log = get_logger("io")


def table_path(cfg, name):
    """Where a §6.4 table lives: <paths.analytics>/<name>, or <paths.quality> for the quality report."""
    layer = TABLES[name].layer
    return cfg["paths"][layer] if layer == "quality" else f"{cfg['paths'][layer]}/{name}"


def conform(df: DataFrame, name: str) -> DataFrame:
    """`df` with exactly the contract's columns, in contract order and types. Raises on missing or extra columns,
    so a job can never silently write a table that breaks its readers."""
    expected = [col for col, _ in TABLES[name].columns]
    missing, extra = set(expected) - set(df.columns), set(df.columns) - set(expected)
    if missing or extra:
        raise ValueError(f"table {name!r} does not match its contract: missing {sorted(missing)}, "
                         f"unexpected {sorted(extra)}")
    return df.select(*[F.col(col).cast(typ).alias(col) for col, typ in TABLES[name].columns])


def write_dataset(df: DataFrame, path: str, partition_by=None):
    """Overwrite `path` with `df` as Parquet (idempotent: a re-run replaces the whole folder)."""
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)
    log.info("wrote %s", path)


def write_table(df: DataFrame, cfg, name: str):
    write_dataset(conform(df, name), table_path(cfg, name))


def read_table(spark: SparkSession, cfg, name: str) -> DataFrame:
    return spark.read.parquet(table_path(cfg, name))


def read_parsed(spark: SparkSession, cfg) -> DataFrame:
    """The ETL output (§6.2). Fails with a hint when the ETL has not run yet."""
    path = cfg["paths"]["parsed"]
    try:
        return spark.read.parquet(path)
    except Exception as e:  # AnalysisException: Path does not exist
        raise SystemExit(f"cannot read parsed data at {path} ({str(e).splitlines()[0]}); "
                         f"run `python scripts/pipeline.py etl --env {cfg['env']}` first") from e
