#!/usr/bin/env python3
"""Run the MapReduce jobs on YARN (--env cluster, inside the client container) or locally by piping
the same mapper and reducer through a sort on the bundled sample (--env local, no Docker needed).

    python mapreduce/run.py [--env local|cluster] [status_count hourly_traffic top_resources]

Output: <paths.mapreduce>/<job>/part-*  (text, `key<TAB>value` lines)
"""
import argparse
import glob
import os
import subprocess
import sys
import zipfile
from collections import namedtuple
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / "src"))

from loganalysis.common.config import load_config  # noqa: E402
from loganalysis.parsing.streaming import MALFORMED_COUNTER  # noqa: E402

Stage = namedtuple("Stage", "mapper mapper_args reducer reducer_args combiner reducers")
SUM = "sum_reducer.py"
JOBS = {
    "status_count": [Stage("status_count/mapper.py", "", SUM, "", True, 2)],
    "hourly_traffic": [Stage("hourly_traffic/mapper.py", "", SUM, "", True, 2)],
    "top_resources": [
        Stage("top_resources/mapper.py", "", SUM, "", True, 2),
        # stage 2: every mapper keeps its local top N, ONE reducer merges them into the global top N
        Stage("top_resources/top_n.py", "{n}", "top_resources/top_n.py", "{n}", False, 1),
    ],
}


def stage_output(cfg, job, i):
    root = cfg["paths"]["mapreduce"]
    return f"{root}/{job}" if i == len(JOBS[job]) - 1 else f"{root}/{job}_stage{i + 1}"


# ---------------------------------------------------------------- cluster (Hadoop Streaming on YARN)
def package_zip():
    """The shared parser, shipped to every node so there is still exactly one parser."""
    dest = ROOT / "build" / "loganalysis.zip"
    dest.parent.mkdir(exist_ok=True)
    pkg = ROOT / "src" / "loganalysis"
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(pkg / "__init__.py", "loganalysis/__init__.py")
        for f in sorted((pkg / "parsing").glob("*.py")):
            z.write(f, f"loganalysis/parsing/{f.name}")
    return dest


def run_cluster(cfg, job, n):
    jar = glob.glob(os.path.join(os.environ["HADOOP_HOME"], "share/hadoop/tools/lib/hadoop-streaming-*.jar"))[0]
    zip_path, source = package_zip(), cfg["paths"]["raw_glob"]
    for i, st in enumerate(JOBS[job]):
        out = stage_output(cfg, job, i)
        subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", out], check=True)
        mapper = f"python3 {Path(st.mapper).name} {st.mapper_args.format(n=n)}".strip()
        reducer = f"python3 {Path(st.reducer).name} {st.reducer_args.format(n=n)}".strip()
        files = dict.fromkeys([str(HERE / st.mapper), str(HERE / st.reducer), str(zip_path)])
        cmd = ["hadoop", "jar", jar,
               "-D", f"mapreduce.job.name=loganalysis-{job}-stage{i + 1}",
               "-D", f"mapreduce.job.reduces={st.reducers}",
               "-files", ",".join(files),
               "-mapper", mapper, "-reducer", reducer,
               "-input", source, "-output", out]
        if st.combiner:
            cmd += ["-combiner", reducer]
        subprocess.run(cmd, check=True)
        source = out
    return stage_output(cfg, job, len(JOBS[job]) - 1)


# ---------------------------------------------------------------- local (no Docker)
def run_local(cfg, job, n):
    files = sorted(glob.glob(cfg["paths"]["raw_glob"]))
    if not files:
        sys.exit(f"no input files match {cfg['paths']['raw_glob']}")
    data = b"".join(Path(f).read_bytes().rstrip(b"\n") + b"\n" for f in files)
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    malformed = 0

    def pipe(script, args, payload):
        r = subprocess.run([sys.executable, str(HERE / script), *args.format(n=n).split()],
                           input=payload, capture_output=True, env=env, check=True)
        return r.stdout, r.stderr.decode().count(MALFORMED_COUNTER)

    for st in JOBS[job]:
        mapped, bad = pipe(st.mapper, st.mapper_args, data)
        malformed += bad
        # what Hadoop's shuffle does: order by key (bytes before the first tab); stable
        shuffled = b"".join(sorted(mapped.splitlines(keepends=True), key=lambda line: line.split(b"\t", 1)[0]))
        data, _ = pipe(st.reducer, st.reducer_args, shuffled)
    out = Path(stage_output(cfg, job, len(JOBS[job]) - 1))
    out.mkdir(parents=True, exist_ok=True)
    (out / "part-00000").write_bytes(data)
    print(f"  malformed lines skipped: {malformed}")
    return str(out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("jobs", nargs="*", help=f"any of {', '.join(JOBS)} (default: all)")
    ap.add_argument("--env", default="local", choices=("local", "cluster"))
    args = ap.parse_args(argv)
    unknown = [j for j in args.jobs if j not in JOBS]
    if unknown:
        ap.error(f"unknown job(s): {', '.join(unknown)}; choose from {', '.join(JOBS)}")
    cfg = load_config(args.env)
    for job in args.jobs or JOBS:
        print(f"[{job}] {args.env}")
        out = (run_cluster if args.env == "cluster" else run_local)(cfg, job, cfg["top_n"])
        print(f"[{job}] output: {out}")


if __name__ == "__main__":
    main()
