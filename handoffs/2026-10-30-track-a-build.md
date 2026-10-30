# Handoff: Track A build (platform, ingestion, MapReduce, runner)

**Date:** 2026-10-30 · **Author:** Dilpreet · **State of `main`:** everything below is committed and pushed.
**Read this first if you are picking up Track B or Track C.** Plan: [`docs/PROJECT_PLAN.md`](../docs/PROJECT_PLAN.md). Tasks: [`docs/TEAM_TASKS.md`](../docs/TEAM_TASKS.md).

## Where things stand

| Task | Status | Notes |
|---|---|---|
| S2 skeleton | review | `.gitattributes` (LF), `.gitignore`, `requirements/`, `ruff.toml`, `pytest.ini`, `config/pipeline.yaml` |
| A1 download + sample | review | resumable downloader, `data/sample/nasa_sample.log` (2,024 lines) |
| A2/A3 cluster | review | one multi-arch image, compose `lite`/`full`, healthchecks, `scripts/smoke_test.sh` |
| A4 HDFS loader | review | idempotent, size-verified, `fsck` summary |
| A5–A7 MapReduce | review | `status_count`, `hourly_traffic`, `top_resources` |
| A8 runner | review | `scripts/pipeline.py` |
| A9 compare + benchmark | **in progress** | comparison works; the benchmark half cannot run until the Spark jobs exist |
| A10 | removed | merged into C8 |

"review" means finished and tested, waiting for a second pair of eyes (plan §9).

## Verified on the real cluster (3,461,613 lines, 373 MB)

- Smoke test passes: HDFS, SparkPi, MapReduce wordcount, a Python Hadoop Streaming mapper (1m49s).
- Ingest: 373,056,138 bytes into 4 HDFS blocks, HEALTHY; a second run skips both files.
- MapReduce on YARN, all three jobs: 287 s. Output equals an independent Python count **exactly** (statuses, 1,369 hourly buckets, top 20 including tie order). The `malformed_lines=12` counter shows on YARN.
- Spark vs MapReduce: all three MATCH. This used throwaway Spark tables I built with `regexp_extract` and the shared patterns (deleted afterwards). **Spark's Java regex accepted exactly 3,461,601 lines, the same as the Python parser**, so the "one regex in both engines" design holds on real data.
- 36 unit tests pass on Python 3.12 and on the container's 3.10; `ruff` clean.

## Facts about the data that the code must respect

- 12 lines are rejected on real data: 1 `NO_MATCH` (the truncated end of the July file) and 11 `BAD_REQUEST` (binary junk, status 400). Everything else parses, including ~3,900 lines with messy request strings.
- **The July file stops at `28 Jul 13:32:25`; August starts `01 Aug 00:00:01`.** That is 3 days 10½ hours of missing data, separate from the hurricane outage (`01 Aug 14:52:01` to `03 Aug 04:36:13`). The anomaly job should produce **two** runs of gap hours; only 1,369 hours have traffic.
- Status codes seen: 200, 302, 304, 400 (4), 403, 404, 500 (65), 501 (41). 5xx is tiny.
- Read input with `errors="replace"`: the data contains non-UTF-8 bytes.
- Server time is UTC-04:00 for the whole dataset; `spark-defaults.conf` sets the session zone to `America/New_York`.

## How to run

```bash
docker compose -f infra/docker-compose.yml --profile lite up -d            # first build downloads ~1 GB
docker compose -f infra/docker-compose.yml exec client bash scripts/smoke_test.sh
python scripts/pipeline.py mapreduce --env cluster                          # from the host or inside the client
python scripts/pipeline.py all --env cluster                                # stops at etl until B4 exists
docker compose -f infra/docker-compose.yml --profile lite down              # keeps HDFS volumes (-v wipes them)
```

Without the cluster (also the supported route on Windows): `docker compose -f infra/docker-compose.yml --profile lite run --rm --no-deps client python -m pytest`.
Cluster UIs: HDFS http://localhost:9870, YARN http://localhost:8088, Spark http://localhost:8080.

## Decisions made while building (and why)

- **One image for all services**, built from `ubuntu:22.04` plus the official Hadoop and Spark tarballs. `apache/hadoop` is amd64-only, so it would run emulated on Apple Silicon.
- **Java 11 in the image, not 17.** Hadoop 3.4.1's YARN fails to start on 17 (`InaccessibleObjectException`) without `--add-opens` flags for every daemon and task JVM.
- **No `${env.X}` placeholders in Hadoop XML.** YARN task containers get a filtered environment, so the placeholder stayed literal and every MapReduce job crashed. Replication is applied by the loader (`DFS_REPLICATION`, set to 2 in `infra/full.env`).
- **NameNode healthcheck only checks it answers; the DataNode healthcheck requires safe mode OFF.** Requiring OFF on the NameNode deadlocks after any restart with data in HDFS.
- **The parser lives in `src/loganalysis/parsing/`** with numbered regex groups (named groups are not valid Java). `streaming.py` is the stdin helper for mappers.
- `top_resources` stage 2 is one script, `top_n.py`, used as both mapper and reducer. Ties break on (count desc, key asc) so Spark and MapReduce can be compared exactly.

## For Track B

- **`src/loganalysis/parsing/parser.py` is a seed for B1, not finished.** Still to build: `schema.py` (StructType, reason constants), the Spark path, and the Spark-vs-Python parity test. The patterns in `log_pattern.py` already work in Spark (see above); `regexp_extract` returns `""` on no match.
- `src/loganalysis/common/config.py` is the minimal B3 loader (`load_config(env)` resolves paths). Extend it with the Spark session factory and logging.
- **Every Spark job must be runnable as `python -m loganalysis.<pkg>.<module> --env local|cluster`.** The runner calls them that way and reports missing ones with the owning task.
- Table names and columns are in plan §6.4. `compare_engines.py` reads `status_counts(status, requests)`, `traffic_hourly(hour_ts, requests)` and `top_resources_by_hits(resource, requests)` and expects them under `paths.analytics`. `quality_report` needs the new `unique_hosts` column.
- Anomaly baseline: null out gap hours before the window, and require `min_baseline_points` (config) real values.
- When the jobs exist, run `python scripts/compare_engines.py --env cluster`; it also writes `engine_benchmark`.

## For Track C

- Nothing is built yet. Start from `dashboard/mock_data/` as the plan says. Port 5000 is reserved, and C8 adds the `dashboard` service to `infra/docker-compose.yml` (the existing services show the pattern).
- The summary KPI "unique hosts" comes from `quality_report.unique_hosts`; distinct counts cannot be summed from the hourly or daily tables.
- Label the two data holes honestly on the dashboard: one is missing data (28 Jul to 1 Aug), the other a server outage (1 to 3 Aug).

## Not done / not verified

- **Windows (amd64) build is untested.** The Dockerfile is architecture-neutral, but I could only build arm64. One Windows teammate should do a first `up` early (plan task S4).
- **The `full` profile has never been started** (second DataNode, second Spark worker, history server).
- **The benchmark half of `compare_engines.py`** and `pipeline.py` stages `etl`, `analytics`, `compare`, `export` are untested end to end because the Spark jobs do not exist.
- **One unexplained stall:** a single YARN job idled for ~14 minutes with a silent ResourceManager (node stayed RUNNING, disk 80%). It never recurred; a rerun took 1m49s. Probably the Docker VM being paused or starved, but unproven.
- Cosmetic: in the `lite` profile there is no history server, so finished-job tracking URLs point at `historyserver:19888`. Jobs still succeed.
- The container's Ubuntu pytest is 6.x and ignores the `pythonpath` option (a harmless warning; `PYTHONPATH` is set by the image).

## Practical notes

- Docker's VM needs about 8 GB. The lite cluster limits add up to ~6 GB; on Windows set `memory=8GB` in `.wslconfig`.
- The image is ~3 GB. After the first build, restarts are fast.
- **History was rewritten once** (commit hashes changed, the content did not). If you cloned before commit `86a0de3`, run `git fetch && git reset --hard origin/main`.
- Raw data lives in `data/raw/` (git-ignored). `pipeline.py ingest` downloads it again if missing; the downloader accepts any `.gz` whose SHA-256 matches `config/pipeline.yaml`.

## Open questions (plan §12)

1. Who takes Tracks B and C (owners are blank in `TEAM_TASKS.md`)?
2. The demo date.
3. Any instructor requirements on versions, report format or required analyses.
