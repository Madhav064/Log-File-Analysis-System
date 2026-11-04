"""The pure-Python half of the export (C7): file shape, NaN handling, atomic writes, manifest."""
import json

import pytest

from loganalysis.export.serving import MANIFEST, clean, missing_entry, write_manifest, write_serving_file

CFG = {"dataset": {"name": "NASA HTTP Jul+Aug 1995"}, "env": "local", "timezone": "America/New_York"}


def test_nan_and_infinity_become_null_also_inside_maps():
    assert clean(float("nan")) is None and clean(float("inf")) is None
    assert clean(1.5) == 1.5 and clean("x") == "x" and clean(None) is None
    assert clean({"NO_MATCH": 1, "x": float("nan")}) == {"NO_MATCH": 1, "x": None}


def test_a_table_is_written_as_columns_and_rows(tmp_path):
    entry = write_serving_file(tmp_path, "top_error_hosts", ("host", "errors"), [("a.com", 3), ("b.com", 1)])
    assert json.loads((tmp_path / "top_error_hosts.json").read_text()) == {
        "columns": ["host", "errors"], "rows": [["a.com", 3], ["b.com", 1]]}
    assert entry == {"file": "top_error_hosts.json", "rows": 2, "columns": ["host", "errors"], "missing": False}
    assert not list(tmp_path.glob("*.tmp"))


def test_the_manifest_lists_every_entry_including_missing_ones(tmp_path):
    entries = {"top_error_hosts": write_serving_file(tmp_path, "top_error_hosts", ("host", "errors"), []),
               "engine_benchmark": missing_entry(("job",))}
    write_manifest(tmp_path, "2026-11-12T10:00:00-04:00", CFG, entries)
    m = json.loads((tmp_path / MANIFEST).read_text())
    assert m["dataset"] == "NASA HTTP Jul+Aug 1995" and m["timezone"] == "America/New_York"
    assert m["tables"]["engine_benchmark"]["missing"] is True and m["tables"]["top_error_hosts"]["rows"] == 0


def test_a_nan_that_slipped_through_is_refused_rather_than_written_as_invalid_json(tmp_path):
    from loganalysis.export.serving import write_json
    with pytest.raises(ValueError):
        write_json(tmp_path, "x.json", {"v": float("nan")})
