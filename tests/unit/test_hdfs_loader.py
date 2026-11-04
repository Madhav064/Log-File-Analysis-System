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


def test_count_lines_counts_a_cut_off_last_line_and_binary_junk(tmp_path):
    p = tmp_path / "log"
    p.write_bytes(b"a\n\xff\xfe junk\nlast line without newline")
    assert count_lines(p) == 3
    p.write_bytes(b"a\nb\n")
    assert count_lines(p) == 2
    p.write_bytes(b"")
    assert count_lines(p) == 0
