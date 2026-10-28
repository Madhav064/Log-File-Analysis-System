"""Create the HDFS layout and upload the raw logs. Idempotent: a file already in HDFS with the same
size is skipped (use --force to re-upload). Needs the `hdfs` command, so run it in the client container.

    python -m loganalysis.ingestion.hdfs_loader [--force]
"""
import argparse
import os
import subprocess

from loganalysis.common.config import load_config
from loganalysis.ingestion.download import ensure_raw_files

LAYOUT = ("raw", "parsed", "rejected", "quality", "analytics", "mapreduce")
FSCK_KEYS = ("Total size", "Total files", "Total blocks", "Average block replication", "Default replication", "Status")


def hdfs(*args, check=True):
    return subprocess.run(["hdfs", *args], capture_output=True, text=True, check=check)


def hdfs_size(uri):
    r = hdfs("dfs", "-stat", "%b", uri, check=False)
    return int(r.stdout.strip()) if r.returncode == 0 else None


def load(cfg, force=False):
    paths = cfg["paths"]
    for key in LAYOUT:
        hdfs("dfs", "-mkdir", "-p", paths[key])
    for local, month in ensure_raw_files(cfg):
        target = f"{paths['raw']}/month={month}/{local.name}"
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
    shown = set()  # fsck repeats Total size / Total files for erasure-coded data (always 0 here)
    for line in hdfs("fsck", paths["raw"], "-files", "-blocks", check=False).stdout.splitlines():
        key = next((k for k in FSCK_KEYS if line.strip().startswith(k)), None)
        if key and key not in shown:
            shown.add(key)
            print("  " + line.strip())


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="re-upload even if the size matches")
    cfg = load_config("cluster")
    load(cfg, ap.parse_args(argv).force)


if __name__ == "__main__":
    main()
