"""Create the HDFS layout and upload the raw logs. Idempotent: a file already in HDFS with the same
size is skipped (use --force to re-upload). Needs the `hdfs` command, so run it in the client container.

    python -m loganalysis.ingestion.hdfs_loader [--force]
"""
import argparse
import os
import subprocess

from loganalysis.common.config import load_config
from loganalysis.common.reports import now_iso, write_report
from loganalysis.ingestion.download import ensure_raw_files

LAYOUT = ("raw", "parsed", "rejected", "quality", "analytics", "mapreduce")
FSCK_KEYS = ("Total size", "Total files", "Total blocks", "Average block replication", "Default replication", "Status")


def hdfs(*args, check=True):
    return subprocess.run(["hdfs", *args], capture_output=True, text=True, check=check)


def hdfs_size(uri):
    r = hdfs("dfs", "-stat", "%b", uri, check=False)
    return int(r.stdout.strip()) if r.returncode == 0 else None


def parse_stat(text):
    """Output of `hdfs dfs -stat "%b %r %o"` -> (bytes, replication, block_size, blocks)."""
    size, replication, block_size = (int(v) for v in text.split())
    return size, replication, block_size, -(-size // block_size) if size else 0  # ceil division


def parse_fsck_status(text):
    """'Status: HEALTHY' (or CORRUPT ...) from `hdfs fsck` output, or 'UNKNOWN'."""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("Status:"):
            return line.split(":", 1)[1].strip()
    return "UNKNOWN"


def count_lines(path):
    """Lines as Spark counts them: a last line without a newline (the cut-off end of July) still counts."""
    n, last = 0, b"\n"
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            n += chunk.count(b"\n")
            last = chunk[-1:]
    return n + (last != b"\n")


def load(cfg, force=False):
    paths = cfg["paths"]
    for key in LAYOUT:
        hdfs("dfs", "-mkdir", "-p", paths[key])
    uploaded = []
    for local, month in ensure_raw_files(cfg):
        target = f"{paths['raw']}/month={month}/{local.name}"
        uploaded.append((local, month, target))
        size = local.stat().st_size
        if not force and hdfs_size(target) == size:
            print(f"  {target}: already in HDFS ({size:,} bytes), skipped")
            continue
        hdfs("dfs", "-mkdir", "-p", f"{paths['raw']}/month={month}")
        # lite keeps 1 copy; the full profile (infra/full.env) sets DFS_REPLICATION=2
        hdfs("dfs", f"-Ddfs.replication={os.environ.get('DFS_REPLICATION', '1')}", "-put", "-f", str(local), target)
        stored = hdfs_size(target)
        if stored != size:
            raise RuntimeError(f"size mismatch for {target}: local {size:,}, HDFS {stored}")
        print(f"  {target}: uploaded ({size:,} bytes)")
    print("HDFS block summary for", paths["raw"])
    fsck = hdfs("fsck", paths["raw"], "-files", "-blocks", check=False).stdout
    shown = set()  # fsck repeats Total size / Total files for erasure-coded data (always 0 here)
    for line in fsck.splitlines():
        key = next((k for k in FSCK_KEYS if line.strip().startswith(k)), None)
        if key and key not in shown:
            shown.add(key)
            print("  " + line.strip())
    path = write_report(cfg, "ingestion_report", report_records(cfg, uploaded, parse_fsck_status(fsck)))
    print(f"Ingestion report written to {path}")


def report_records(cfg, uploaded, fsck_status):
    """One ingestion_report row per file. Every file passed the SHA-256 check in ensure_raw_files (it raises
    otherwise), so checksum_verified is True."""
    sha = {f["name"]: f["sha256"] for f in cfg["dataset"]["files"]}
    loaded_at = now_iso()
    records = []
    for local, month, target in uploaded:
        stat = hdfs("dfs", "-stat", "%b %r %o", target).stdout
        size, replication, block_size, blocks = parse_stat(stat)
        records.append({
            "file": local.name, "month": month, "hdfs_path": target, "local_bytes": local.stat().st_size,
            "hdfs_bytes": size, "lines": count_lines(local), "sha256": sha[local.name], "checksum_verified": True,
            "replication": replication, "block_size": block_size, "blocks": blocks, "fsck_status": fsck_status,
            "loaded_at": loaded_at,
        })
    return records


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="re-upload even if the size matches")
    cfg = load_config("cluster")
    load(cfg, ap.parse_args(argv).force)


if __name__ == "__main__":
    main()
