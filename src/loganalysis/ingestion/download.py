"""Download the NASA logs, verify their SHA-256 and decompress them. Safe to re-run.

    python -m loganalysis.ingestion.download [--env local|cluster]

The host is slow and long transfers time out, so downloads resume (HTTP Range) and retry. A .gz that is
already in data/raw/ (for example a teammate's backup copy) is accepted if its checksum matches.
"""
import argparse
import gzip
import hashlib
import shutil
import time
import urllib.error
import urllib.request
from pathlib import Path

from loganalysis.common.config import load_config

CHUNK = 1 << 20


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


def download(url, dest, expected_sha, retries=8, timeout=60):
    """Fetch `url` to `dest` (resuming a .part file) and verify the checksum. Returns dest."""
    dest = Path(dest)
    if dest.exists() and sha256(dest) == expected_sha:
        print(f"  {dest.name}: already downloaded, checksum ok")
        return dest
    part = dest.with_name(dest.name + ".part")
    for attempt in range(1, retries + 1):
        have = part.stat().st_size if part.exists() else 0
        req = urllib.request.Request(url, headers={"Range": f"bytes={have}-"} if have else {})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resume = resp.status == 206
                received = 0
                with open(part, "ab" if resume else "wb") as out:
                    while chunk := resp.read(CHUNK):
                        out.write(chunk)
                        received += len(chunk)
                expected = resp.headers.get("Content-Length")
                # read(n) does not raise when the server hangs up early, so check the length ourselves
                if expected is not None and received != int(expected):
                    raise OSError(f"connection closed early ({received:,} of {int(expected):,} bytes)")
            break
        except urllib.error.HTTPError as e:
            if e.code == 416:  # we already have every byte: go verify it
                break
            print(f"  attempt {attempt}/{retries}: HTTP {e.code}")
        except (urllib.error.URLError, OSError) as e:
            print(f"  attempt {attempt}/{retries}: {e}; resuming from {have:,} bytes")
        time.sleep(min(5 * attempt, 30))
    else:
        raise RuntimeError(f"could not download {url} after {retries} attempts (partial file kept in {part})")
    got = sha256(part)
    if got != expected_sha:
        part.unlink()
        raise RuntimeError(f"checksum mismatch for {url}: expected {expected_sha}, got {got}")
    part.replace(dest)
    print(f"  {dest.name}: downloaded, checksum ok")
    return dest


def decompress(gz, out):
    """gzip files can't be split, so HDFS gets the plain text (docs/PROJECT_PLAN.md P6)."""
    gz, out = Path(gz), Path(out)
    if out.exists() and out.stat().st_mtime >= gz.stat().st_mtime:
        return out
    tmp = out.with_name(out.name + ".tmp")
    with gzip.open(gz, "rb") as src, open(tmp, "wb") as dst:
        shutil.copyfileobj(src, dst, CHUNK)
    tmp.replace(out)
    return out


def ensure_raw_files(cfg):
    """Download and decompress every dataset file. Returns [(plain_text_path, month), ...]."""
    target = Path(cfg["paths"]["download_dir"])
    target.mkdir(parents=True, exist_ok=True)
    result = []
    for f in cfg["dataset"]["files"]:
        gz = download(f["url"], target / (f["name"] + ".gz"), f["sha256"])
        result.append((decompress(gz, target / f["name"]), f["month"]))
    return result


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--env", default="local", choices=("local", "cluster"))
    cfg = load_config(ap.parse_args(argv).env)
    for path, month in ensure_raw_files(cfg):
        print(f"{month}: {path} ({path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
