"""The parts of the HDFS loader that turn `hdfs` command output into the ingestion report."""
from loganalysis.ingestion.hdfs_loader import count_lines, parse_fsck_status, parse_stat

MB128 = 134217728


def test_stat_gives_size_replication_block_size_and_block_count():
    # the real July file: 205 MB in 128 MB blocks -> 2 blocks
    assert parse_stat("205242368 1 134217728\n") == (205242368, 1, MB128, 2)
    assert parse_stat(f"{MB128} 2 {MB128}") == (MB128, 2, MB128, 1)  # exactly one block
    assert parse_stat(f"0 1 {MB128}") == (0, 1, MB128, 0)


def test_fsck_status_is_read_from_the_status_line():
    out = " Total size:\t373056138 B\n Total blocks (validated):\t4\n Status: HEALTHY\n"
    assert parse_fsck_status(out) == "HEALTHY"
    assert parse_fsck_status("nothing useful") == "UNKNOWN"


def test_report_records_follow_the_contract(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from loganalysis.common.reports import to_rows
    from loganalysis.ingestion import hdfs_loader

    local = tmp_path / "NASA_access_log_Jul95"
    local.write_bytes(b"line\n" * 3)
    monkeypatch.setattr(hdfs_loader, "hdfs", lambda *a, **k: SimpleNamespace(stdout=f"15 1 {MB128}\n"))
    cfg = {"dataset": {"files": [{"name": "NASA_access_log_Jul95", "sha256": "ab" * 32}]}}
    [rec] = hdfs_loader.report_records(cfg, [(local, "1995-07", "hdfs://nn/logs/raw/x")], "HEALTHY")
    to_rows("ingestion_report", [rec])  # raises if a column is missing or extra
    assert (rec["lines"], rec["hdfs_bytes"], rec["blocks"], rec["fsck_status"]) == (3, 15, 1, "HEALTHY")


def test_count_lines_counts_a_cut_off_last_line_and_binary_junk(tmp_path):
    p = tmp_path / "log"
    p.write_bytes(b"a\n\xff\xfe junk\nlast line without newline")
    assert count_lines(p) == 3
    p.write_bytes(b"a\nb\n")
    assert count_lines(p) == 2
    p.write_bytes(b"")
    assert count_lines(p) == 0
