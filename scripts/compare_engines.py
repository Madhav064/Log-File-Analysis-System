#!/usr/bin/env python3
"""Check that Spark and MapReduce give the same answers, then benchmark both.

    python scripts/compare_engines.py [--env local|cluster] [--runs 3] [--no-benchmark]

For status counts, hourly traffic and top resources it loads the Spark table (analytics Parquet) and
the MapReduce output, normalises them and reports every difference. The benchmark times a full run
of each job (JVM start-up included, as a user would experience it), takes the median of --runs, and
writes the `engine_benchmark` table. Hardware and profile are printed next to the numbers.
"""
import argparse
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from loganalysis.common.config import load_config  # noqa: E402

# MapReduce job -> the Spark table that must match it, and the Spark module that produces it.
# `key` is a SQL expression evaluated by Spark, so timestamps are formatted in the SESSION time zone
# (collecting them into Python would use the container's zone instead).
JOBS = {
    "status_count": dict(table="status_counts", key="CAST(status AS STRING)", value="requests",
                         ranked=False, spark_module="loganalysis.analytics.errors"),
    "hourly_traffic": dict(table="traffic_hourly", key="date_format(hour_ts, \"yyyy-MM-dd'T'HH\")",
                           value="requests", ranked=False, spark_module="loganalysis.analytics.traffic"),
    "top_resources": dict(table="top_resources_by_hits", key="resource", value="requests",
                          ranked=True, spark_module="loganalysis.analytics.top_n"),
}


# ---------------------------------------------------------------- pure comparison logic (unit-tested)
def parse_mapreduce(text):
    """'key<TAB>count' lines -> [(key, int)], in file order."""
    rows = []
    for line in text.splitlines():
        if line.strip():
            key, _, value = line.rpartition("\t")
            rows.append((key, int(value)))
    return rows


def compare(spark_rows, mr_rows, ranked):
    """Return {"match": bool, "only_spark": [...], "only_mr": [...], "different": [(key, spark, mr)]}.

    Unranked tables must match key for key. A ranked table (top N) is cut to the same N and compared in
    order; ties are broken by key on both sides, so the lists are exactly comparable.
    """
    if ranked:
        spark_rows = sorted(spark_rows, key=lambda kv: (-kv[1], kv[0]))[:len(mr_rows)]
        mr_rows = sorted(mr_rows, key=lambda kv: (-kv[1], kv[0]))
    s, m = dict(spark_rows), dict(mr_rows)
    out = {"only_spark": sorted(s.keys() - m.keys()), "only_mr": sorted(m.keys() - s.keys()),
           "different": [(k, s[k], m[k]) for k in sorted(s.keys() & m.keys()) if s[k] != m[k]]}
    out["match"] = not any(out.values()) and (not ranked or [k for k, _ in spark_rows] == [k for k, _ in mr_rows])
    return out


# ---------------------------------------------------------------- reading the two engines' results
def read_mapreduce(cfg, job):
    where = f"{cfg['paths']['mapreduce']}/{job}"
    if cfg["env"] == "cluster":
        r = subprocess.run(["hdfs", "dfs", "-cat", f"{where}/part-*"], capture_output=True, text=True, check=True)
        return r.stdout
    return "".join(p.read_text() for p in sorted(Path(where).glob("part-*")))


def read_spark(spark, cfg, spec):
    rows = spark.read.parquet(f"{cfg['paths']['analytics']}/{spec['table']}") \
        .selectExpr(f"{spec['key']} AS k", f"CAST({spec['value']} AS LONG) AS v").collect()
    return [(r.k, r.v) for r in rows]


def spark_session(cfg):
    from loganalysis.common.spark import get_spark  # same session settings as every Spark job
    return get_spark("compare", cfg)


def median_runtime(cmd, runs, cwd=ROOT):
    times = []
    for i in range(runs):
        t0 = time.time()
        if subprocess.run(cmd, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
            sys.exit(f"benchmark run failed: {' '.join(cmd)}")
        times.append(time.time() - t0)
    return round(statistics.median(times), 1)


# ---------------------------------------------------------------- main
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--env", default="local", choices=("local", "cluster"))
    ap.add_argument("--runs", type=int, default=3, help="benchmark runs per job and engine (median is kept)")
    ap.add_argument("--no-benchmark", action="store_true", help="only check that the results match")
    args = ap.parse_args(argv)
    cfg = load_config(args.env)

    results = {}
    spark = spark_session(cfg)
    try:
        for job, spec in JOBS.items():
            try:
                spark_rows = read_spark(spark, cfg, spec)
            except Exception as e:  # AnalysisException when the Parquet table is missing
                sys.exit(f"[{job}] cannot read Spark table '{spec['table']}': {str(e).splitlines()[0]}\n"
                         f"  run `pipeline.py analytics` first")
            mr_text = read_mapreduce(cfg, job)
            if not mr_text.strip():
                sys.exit(f"[{job}] MapReduce output is empty or missing; run `pipeline.py mapreduce` first")
            mr_rows = parse_mapreduce(mr_text)
            diff = compare(spark_rows, mr_rows, spec["ranked"])
            results[job] = (diff, len(spark_rows), len(mr_rows))
            verdict = "MATCH" if diff["match"] else "DIFFERENT"
            print(f"[{job}] {verdict}: spark {len(spark_rows)} rows, mapreduce {len(mr_rows)} rows")
            for label in ("only_spark", "only_mr", "different"):
                for item in diff[label][:10]:
                    print(f"    {label}: {item}")
    finally:
        # Release the cluster before benchmarking: every benchmark run is its own Spark application, and on the
        # lite profile this session would otherwise hold all the worker's cores and leave each run WAITING forever.
        spark.stop()

    if not args.no_benchmark:
        rows = []
        for job, spec in JOBS.items():
            diff, n_spark, n_mr = results[job]
            for engine, cmd, n in (
                ("spark", [sys.executable, "-m", spec["spark_module"], "--env", args.env], n_spark),
                ("mapreduce", [sys.executable, str(ROOT / "mapreduce" / "run.py"), job, "--env", args.env], n_mr),
            ):
                rows.append((job, engine, median_runtime(cmd, args.runs), n, diff["match"]))
        out = f"{cfg['paths']['analytics']}/engine_benchmark"
        schema = "job string, engine string, runtime_sec double, output_rows long, results_match boolean"
        spark = spark_session(cfg)  # a fresh, short session just to write the results
        try:
            spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(out)
        finally:
            spark.stop()
        print(f"\nBenchmark (median of {args.runs} runs) written to {out}")
        print(f"Hardware: {platform.machine()}, {os.cpu_count()} cpus, env={args.env}, "
              f"profile={os.environ.get('DFS_REPLICATION', '1') == '2' and 'full' or 'lite'}")
        for r in rows:
            print(f"  {r[0]:15} {r[1]:10} {r[2]:8.1f}s  rows={r[3]}  match={r[4]}")
    if not all(d["match"] for d, _, _ in results.values()):
        sys.exit("results differ between engines (explain or fix every difference)")


if __name__ == "__main__":
    main()
