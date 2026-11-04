import json
import os

import pytest
from app.services.repository import DataMissing, Repository


def write(d, name, payload):
    (d / name).write_text(json.dumps(payload))


@pytest.fixture
def data_dir(tmp_path):
    write(tmp_path, "top_error_hosts.json", {"columns": ["host", "errors"], "rows": [["a.com", 3]]})
    write(tmp_path, "manifest.json", {"tables": {
        "top_error_hosts": {"file": "top_error_hosts.json", "rows": 1, "columns": ["host", "errors"],
                            "missing": False},
        "engine_benchmark": {"file": None, "rows": 0, "columns": ["job"], "missing": True}}})
    return tmp_path


def test_rows_come_back_as_dicts(data_dir):
    assert Repository(data_dir).rows("top_error_hosts") == [{"host": "a.com", "errors": 3}]


def test_missing_table_and_missing_export_raise_data_missing(data_dir, tmp_path_factory):
    repo = Repository(data_dir)
    with pytest.raises(DataMissing):
        repo.rows("engine_benchmark")
    assert repo.available("top_error_hosts") and not repo.available("engine_benchmark")
    empty = Repository(tmp_path_factory.mktemp("empty"))
    with pytest.raises(DataMissing, match="pipeline.py export"):
        empty.rows("top_error_hosts")
    assert not empty.available("top_error_hosts")


def test_a_new_export_is_picked_up_without_a_restart(data_dir):
    repo = Repository(data_dir)
    assert repo.rows("top_error_hosts")[0]["errors"] == 3
    write(data_dir, "top_error_hosts.json", {"columns": ["host", "errors"], "rows": [["a.com", 7]]})
    m = data_dir / "manifest.json"
    m.write_text(m.read_text())
    st = m.stat()
    os.utime(m, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))  # make sure the mtime changes
    assert repo.rows("top_error_hosts")[0]["errors"] == 7
