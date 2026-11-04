"""Writing the dashboard's serving files (plan §6.5). Pure Python, no Spark: the Spark half is to_serving.py.

Every file is data/serving/<name>.json shaped {"columns": [...], "rows": [[...], ...]}. manifest.json is
written last, so a reader never takes a half-finished export for a complete one.
"""
import json
import math
import os
from pathlib import Path

MANIFEST = "manifest.json"


def clean(value):
    """A value JSON can carry: NaN and infinities become null (JSON has no NaN)."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    return value


def write_json(directory, filename, payload):
    """Write `payload` to directory/filename through a temp file and a rename (never a half-written file)."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    tmp = directory / (filename + ".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":"), allow_nan=False))
    os.replace(tmp, directory / filename)
    return directory / filename


def write_serving_file(directory, name, columns, rows):
    """Write one table; returns its manifest entry."""
    rows = [[clean(v) for v in row] for row in rows]
    write_json(directory, f"{name}.json", {"columns": list(columns), "rows": rows})
    return {"file": f"{name}.json", "rows": len(rows), "columns": list(columns), "missing": False}


def missing_entry(columns):
    return {"file": None, "rows": 0, "columns": list(columns), "missing": True}


def write_manifest(directory, generated_at, cfg, entries):
    """Last step of an export. `entries` maps each table or report name to its entry."""
    return write_json(directory, MANIFEST, {
        "generated_at": generated_at, "dataset": cfg["dataset"]["name"], "env": cfg["env"],
        "timezone": cfg["timezone"], "tables": entries,
    })
