"""Export the analytics tables and run reports to the dashboard's serving JSON (task C7, plan M7).

    python -m loganalysis.export.to_serving --env local|cluster

Reads every table in contracts.TABLES, formats timestamps in Spark (so they keep the server's -04:00 offset;
collecting a timestamp would convert it to the Python process's zone), refuses a table above
export.max_rows_per_table, copies the run reports (contracts.REPORTS), and writes manifest.json last.
"""
import time

from pyspark.sql import functions as F

from loganalysis.common.io import read_table
from loganalysis.common.job import run_job
from loganalysis.common.log import get_logger
from loganalysis.common.reports import now_iso, read_report
from loganalysis.contracts import REPORTS, TABLES, column_names
from loganalysis.export.serving import missing_entry, write_manifest, write_serving_file

log = get_logger("export")

# Tables a complete pipeline run may still lack: the benchmark only exists after compare_engines.py.
OPTIONAL = {"engine_benchmark"}
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"   # 1995-07-13T09:00:00-04:00
DATE_FORMAT = "yyyy-MM-dd"


def json_ready(df, name):
    """`df` with the contract's columns, timestamps and dates turned into ISO strings inside Spark."""
    cols = []
    for col, typ in TABLES[name].columns:
        if typ == "timestamp":
            cols.append(F.date_format(col, TIMESTAMP_FORMAT).alias(col))
        elif typ == "date":
            cols.append(F.date_format(col, DATE_FORMAT).alias(col))
        else:
            cols.append(F.col(col))
    return df.select(*cols)


def collect_rows(df, name, max_rows):
    """The table's rows as lists; fails when the table is larger than the dashboard should ever load."""
    rows = df.limit(max_rows + 1).collect()
    if len(rows) > max_rows:
        raise SystemExit(f"table {name!r} has more than {max_rows:,} rows (export.max_rows_per_table); "
                         f"the dashboard reads small aggregates only")
    return [[r[c] for c in df.columns] for r in rows]


def export_tables(spark, cfg, out):
    entries = {}
    for name in TABLES:
        try:
            df = read_table(spark, cfg, name)
        except Exception as e:  # AnalysisException: Path does not exist
            if name in OPTIONAL:
                log.warning("%s not found, marked missing (%s)", name, str(e).splitlines()[0])
                entries[name] = missing_entry(column_names(name))
                continue
            raise SystemExit(f"cannot read table {name!r} ({str(e).splitlines()[0]}); run the "
                             f"`etl` and `analytics` stages first") from e
        rows = collect_rows(json_ready(df, name), name, cfg["export"]["max_rows_per_table"])
        entries[name] = write_serving_file(out, name, column_names(name), rows)
        log.info("%s: %d rows", name, len(rows))
    return entries


def export_reports(cfg, out, export_started, export_seconds):
    """Copy the run reports. The export's own runtime is added to pipeline_runs here, because the runner can
    only record it after this job has finished."""
    entries = {}
    for name in REPORTS:
        records = read_report(cfg, name)
        if name == "pipeline_runs":
            records = [r for r in records or [] if r["stage"] != "export"] + [{
                "stage": "export", "job": "loganalysis.export.to_serving", "env": cfg["env"],
                "started_at": export_started, "duration_sec": round(export_seconds, 1), "status": "ok"}]
        if records is None:
            log.warning("%s not found (written by %s), marked missing", name, REPORTS[name].built_by)
            entries[name] = missing_entry(column_names(name))
            continue
        cols = column_names(name)
        entries[name] = write_serving_file(out, name, cols, [[r[c] for c in cols] for r in records])
    return entries


def run(spark, cfg):
    started, t0 = now_iso(), time.monotonic()  # monotonic: the VM clock can step back during a run
    out = cfg["paths"]["serving"]
    entries = export_tables(spark, cfg, out)
    entries.update(export_reports(cfg, out, started, time.monotonic() - t0))
    path = write_manifest(out, now_iso(), cfg, entries)
    log.info("manifest written to %s (%d files)", path, sum(not e["missing"] for e in entries.values()))


if __name__ == "__main__":
    run_job("export", run, __doc__)
