#!/usr/bin/env python3
"""Run the whole pipeline, or any single stage.

    python scripts/pipeline.py all                      --env cluster
    python scripts/pipeline.py etl                      --env local      # no Docker, on the sample
    python scripts/pipeline.py analytics --only errors,traffic --env cluster

Stages, in order: ingest, etl, analytics, mapreduce, compare, export. It stops at the first failure.
`--env cluster` runs inside the client container; started from the host, the runner re-runs itself
there through `docker compose exec`, so the same command works on macOS, Windows and Linux.
"""
import argparse
import importlib.util
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = str(ROOT / "src")
sys.path.insert(0, SRC)

COMPOSE = ["docker", "compose", "-f", str(ROOT / "infra" / "docker-compose.yml")]
ANALYTICS = ("errors", "traffic", "top_n", "anomalies")
STAGES = ("ingest", "etl", "analytics", "mapreduce", "compare", "export")
# Spark jobs are built by other tracks; this names the task to look at when one is still missing.
BUILT_BY = {"loganalysis.etl.parse_logs": "B4", "loganalysis.export.to_serving": "C7",
            **{f"loganalysis.analytics.{m}": t for m, t in zip(ANALYTICS, ("B5", "B6", "B7", "B8"))}}


def spark_job(module, env):
    """Every Spark job is runnable as `python -m <module> --env <env>` (plan, architectural rule 6)."""
    return [sys.executable, "-m", module, "--env", env]


def commands(stage, env, only):
    if stage == "ingest":
        if env == "cluster":
            return [[sys.executable, "-m", "loganalysis.ingestion.hdfs_loader"]]
        return [spark_job("loganalysis.ingestion.download", env)]
    if stage == "etl":
        return [spark_job("loganalysis.etl.parse_logs", env)]
    if stage == "analytics":
        return [spark_job(f"loganalysis.analytics.{name}", env) for name in only or ANALYTICS]
    if stage == "mapreduce":
        return [[sys.executable, str(ROOT / "mapreduce" / "run.py"), "--env", env]]
    if stage == "compare":
        return [[sys.executable, str(ROOT / "scripts" / "compare_engines.py"), "--env", env]]
    return [spark_job("loganalysis.export.to_serving", env)]


def missing_module(cmd):
    """The `python -m` module this command needs if it does not exist yet, else None."""
    if cmd[1] != "-m":
        return None
    try:
        return None if importlib.util.find_spec(cmd[2]) else cmd[2]
    except ModuleNotFoundError:  # its parent package does not exist yet either
        return cmd[2]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("stage", choices=("all", *STAGES))
    ap.add_argument("--env", default="local", choices=("local", "cluster"))
    ap.add_argument("--only", default="", help=f"analytics only: comma-separated subset of {', '.join(ANALYTICS)}")
    args = ap.parse_args(argv)
    only = [o for o in args.only.split(",") if o]
    if bad := [o for o in only if o not in ANALYTICS]:
        ap.error(f"--only: unknown job(s) {', '.join(bad)}; choose from {', '.join(ANALYTICS)}")

    if args.env == "cluster" and "HADOOP_HOME" not in os.environ:  # on the host: hop into the client container
        # -T: no TTY, so it also works from PowerShell and git-bash
        cmd = [*COMPOSE, "exec", "-T", "client", "python", "scripts/pipeline.py", *(argv or sys.argv[1:])]
        sys.exit(subprocess.run(cmd).returncode)

    env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [SRC, os.environ.get("PYTHONPATH")]))}
    started = time.time()
    for stage in STAGES if args.stage == "all" else (args.stage,):
        for cmd in commands(stage, args.env, only):
            if module := missing_module(cmd):
                task = f" (task {BUILT_BY[module]})" if module in BUILT_BY else ""
                sys.exit(f"\n[{stage}] FAILED: {module} does not exist yet{task}")
            print(f"\n[{stage}] {time.strftime('%H:%M:%S')} start: {' '.join(cmd[1:])}", flush=True)
            t0 = time.time()
            if subprocess.run(cmd, cwd=ROOT, env=env).returncode:
                sys.exit(f"\n[{stage}] FAILED after {time.time() - t0:.0f}s; stopping, later stages were not run")
            print(f"[{stage}] done in {time.time() - t0:.0f}s", flush=True)
    print(f"\nAll requested stages finished in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
