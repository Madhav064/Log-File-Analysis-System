"""Read and write the small JSON run reports (contracts.REPORTS). Pure Python: no Spark import.

A report file is <paths.reports>/<name>.json shaped {"columns": [...], "rows": [[...], ...]}, the same shape
as the dashboard's serving files, so the export can copy it as it is.
"""
import json
import os
from datetime import datetime
from pathlib import Path

from loganalysis.contracts import REPORTS, column_names


def report_path(cfg, name):
    return Path(cfg["paths"]["reports"]) / f"{name}.json"


def now_iso():
    """The current time as an ISO-8601 string with the machine's offset."""
    return datetime.now().astimezone().isoformat(timespec="seconds")


def to_rows(name, records):
    """[{column: value}] -> [[value, ...]] in contract order. Raises on a missing or unexpected column."""
    expected = column_names(name)
    rows = []
    for rec in records:
        if set(rec) != set(expected):
            raise ValueError(f"report {name!r} does not match its contract: missing "
                             f"{sorted(set(expected) - set(rec))}, unexpected {sorted(set(rec) - set(expected))}")
        rows.append([rec[col] for col in expected])
    return rows


def write_report(cfg, name, records):
    """Overwrite the report with `records` (a list of dicts). Written to a temp file, then renamed."""
    if name not in REPORTS:
        raise KeyError(f"unknown report {name!r}")
    path = report_path(cfg, name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps({"columns": list(column_names(name)), "rows": to_rows(name, records)}, indent=1))
    os.replace(tmp, path)
    return path


def read_report(cfg, name):
    """The report as a list of dicts, or None when it has not been written yet."""
    path = report_path(cfg, name)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return [dict(zip(data["columns"], row)) for row in data["rows"]]
