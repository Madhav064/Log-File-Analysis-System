# Handoff: Track B build (parsing, Spark ETL, analytics, CI)

**Date:** 2026-11-03 · **Author:** Madhav · **Branch:** `feat/track-b` (pushed; not merged into `main` yet)
**Updated 2026-11-03:** verified end to end on the real cluster with the full dataset; see "On the real cluster" below.
**Read this if you are picking up Track C, reviewing Track B, or running the full pipeline.** Plan: [`docs/PROJECT_PLAN.md`](../docs/PROJECT_PLAN.md). Tasks: [`docs/TEAM_TASKS.md`](../docs/TEAM_TASKS.md). Track A's handoff: [`2026-10-30-track-a-build.md`](2026-10-30-track-a-build.md).

## Where things stand

| Task | Status | What was built |
|---|---|---|
| B1 parser | review | `parsing/schema.py` (names, reason codes, StructTypes), `parsing/spark_parser.py` (the native `regexp_extract` twin of `parse_line`), shared `TIMESTAMP_PATTERN` |
| B2 parser tests | review | 6 new edge cases, a Spark-vs-Python **parity test** over the fixtures + the whole sample |
| B3 common | review | `common/spark.py` (session factory), `common/io.py` (paths, read/write, contract check), `common/job.py` (the `--env` CLI), `common/log.py`, and `contracts.py` |
| B4 ETL | review | `etl/parse_logs.py`: parsed Parquet partitioned by `log_date`, rejected lines, one-row `quality_report` |
| B5 errors | review | `analytics/errors.py` |
| B6 traffic | review | `analytics/traffic.py` |
| B7 top-N | review | `analytics/top_n.py` |
| B8 anomalies | review | `analytics/anomalies.py` |
| B9 CI | review | `.github/workflows/ci.yml`: ruff + pytest on every push |

"review" means finished and tested, waiting for a second pair of eyes (plan §9). One commit per task on `feat/track-b`; open the PR from <https://github.com/Madhav064/Log-File-Analysis-System/pull/new/feat/track-b>.

## Verified

- **CI is green on the first push** ([CI runs for the branch](https://github.com/Madhav064/Log-File-Analysis-System/actions?query=branch%3Afeat%2Ftrack-b)): ruff clean, pyspark 3.5.3 on Java 17, and the whole suite passed: **97 tests (51 pure-Python + 46 Spark)**. The pyspark check before the tests guarantees the Spark tests ran rather than skipped.
- **Parity:** the Spark parser and the Python parser give every line the same verdict and identical fields, over all 22 fixture lines, the full 2,024-line sample, and a CRLF and a binary-junk line.
- **Integration on the sample** (ETL → errors, traffic, top-N, anomalies, written and read back as Parquet):
  - all 15 Spark tables match their contract (column names, order and types);
  - `quality_report` accounts for every line, rejections by reason, and unique hosts, matching an independent Python count;
  - `status_counts`, `traffic_hourly` totals and `top_resources_by_hits` match the Python count;
  - **Spark matches MapReduce** on all three of Track A's jobs (run locally through `mapreduce/run.py`) using `compare_engines.compare()`;
  - re-running a job replaces its output.
- **Anomalies** on 10 days of synthetic traffic: exactly the planted 3-hour gap, traffic spike and error-rate spike are flagged and nothing else; the hour after the gap gets a baseline of the 4 real days only; days without enough baseline are not scored.
- On this Windows laptop (no pyspark): the 51 pure-Python tests pass and the Spark tests skip cleanly.

### On the real cluster, full dataset (Windows 11, `lite` profile, 3 Nov 2026)

First run of the project on Windows (amd64): this also closes Track A's "Windows build untested" item.

| Step | Result |
|---|---|
| Image build (`build client`) | 10½ min, 3.07 GB; Hadoop and Spark checksums OK |
| Tests in the client container (Python 3.10, Java 11, Spark 3.5.3) | **97 passed** (after the tzdata fix below) |
| `scripts/smoke_test.sh` | passed: HDFS, SparkPi, MapReduce wordcount, Python streaming mapper |
| `pipeline.py ingest` | 373,056,138 bytes in 4 blocks, HEALTHY, checksums OK (same as Track A) |
| `pipeline.py etl` (2½ min) | **3,461,613 lines = 3,461,601 parsed + 12 rejected** (11 `BAD_REQUEST`, 1 `NO_MATCH`), 137,978 unique hosts, 1 Jul 00:00:01 to 31 Aug 23:59:53 −04:00. The cache held on the 512 MB executor |
| `pipeline.py analytics` (2¼ min) | all 14 tables written; status counts and hourly traffic both sum to 3,461,601; 1,369 hours and 58 days with traffic |
| `pipeline.py mapreduce` (5 min) | all 3 jobs succeeded on YARN, `malformed_lines=12` each |
| `compare_engines.py --no-benchmark` | **MATCH on all three**: status_count 8 rows, hourly_traffic 1,369, top_resources 20 |
| **I4 anomalies** | **exactly two gap runs: 28 Jul 14:00 → 31 Jul 23:00 (82 h) and 1 Aug 15:00 → 3 Aug 03:00 (37 h)**. Drops on 1 Aug 14:00 and 3 Aug 04:00–11:00 (slow recovery after the outage). 28 Jul 13:00 is not flagged. 268 rows: 119 gap, 34 + 23 requests spike/drop, 88 + 4 error_rate spike/drop |

What the data shows (useful for the demo): **13 July** is the busiest day (134,203 requests), hour (09:00, 14,926) and minute (09:49, 405), and the anomaly job flags a 10-hour traffic surge that morning; the STS-70 shuttle launch was that morning. Busiest hour of day is 15:00; weekends get about half the weekday traffic. Top resource `/images/NASA-logosmall.gif` (208,798); top host `piweba3y.prodigy.com` (21,988); top broken link `/pub/winvn/readme.txt` (2,004 × 404).

### Fixed during the cluster run

- **`fix(infra)`: `tzdata` added to the image** (Track A's Dockerfile, one word). Without it Python silently ignores `TZ` and stays on UTC, so 5 Spark tests failed in the container (timestamps collected into Python were 4 hours off). The jobs themselves were never affected. The test fixture now fails loudly if the zone is not applied.
- **`fix(compare)`: deadlock in `compare_engines.py`** (Track A's script). Its Spark session held both of the lite worker's cores, so the first benchmark run waited for executors forever. The session is now stopped before benchmarking; a unit test pins the order.
- **Disk:** the first image build (compose building all 9 services in parallel) filled C: (Docker's disk image grew to 12.9 GB) and Docker crashed. Docker's data was moved to D:, and the image was built once with `build client`. The README now says to build once first, lists ~15 GB free disk as a prerequisite, and explains how to move Docker's data.

## Not done / not verified

- **The benchmark half of A9 has not run** (cut for time; plan §11 lists it as "Could"). The deadlock that made it hang is fixed, so `python scripts/compare_engines.py --env cluster` should now finish (about 20 minutes on lite: each Spark and MapReduce job runs 3 times) and write `engine_benchmark`. Until then that table does not exist, so the dashboard's Pipeline page should treat it as optional.
- **`error_rate` anomalies are noisy:** 88 spikes, most of them small in absolute terms (e.g. 1% errors in an hour against a 0.5% baseline gives z ≈ 3.5), because a 7-point std of a tiny rate is unstable. This is the method the plan agreed (P8), not a bug, so I left it. Recommendation for the team: either a higher `z_threshold` for error_rate only, or also requiring a minimum absolute rise (e.g. +1 percentage point). The `requests` anomalies look meaningful as they are.
- The `full` profile has still never been started.
- `--env local` on plain Windows still needs Hadoop's `winutils.exe`, so use the container there (as Track A noted).

## Decisions made while building (and why)

Asked and answered (2026-11-03):
- `top_error_resources` = **top N per error status**. `day_of_week` = **ISO 1–7** (Monday = 1). `traffic_peaks` uses a new config key **`peaks_top_k: 10`**. Profile averages divide by **days with traffic**. `anomalies` holds **flagged hours only**.

Mine (each is written into plan §6.4 / M5d too):
- **`contracts.py` is the contract in code.** `common.io.write_table()` casts every table to its columns and types and raises on a missing or extra column. Track C can import `TABLES` instead of copying column lists.
- **Python patterns are compiled with `re.ASCII`**, because Java's `\s`/`\S`/`\d` are ASCII-only and Python's are Unicode-aware. A non-breaking space would otherwise split a request differently in Spark and MapReduce. No effect on the real data's verdicts (the junk there is U+FFFD, which is not whitespace in either engine).
- **The timestamp shape is checked by one regex in both engines** (`TIMESTAMP_PATTERN`, month names case-sensitive) before Spark's `to_timestamp`, because Spark's date parser is case-insensitive and would accept `01/jul/1995`.
- **`spark.sql.legacy.timeParserPolicy=CORRECTED`** in every session: without it, Spark 3 throws `SparkUpgradeException` on `32/Jul` instead of returning null.
- **`--env local` forces `fs.defaultFS=file:///` and `driver.host=127.0.0.1`**, so local mode also works inside the client container (whose `spark-defaults.conf` points plain paths at HDFS and the driver at the `client` hostname).
- `compare_engines.py` now builds its session with `get_spark()` (one line changed), so it gets the same settings, including the local-mode fixes above.
- **Ties** in every ranking break on the key ascending (count desc, key asc), like Track A's `top_n.py`. Global top-N uses `orderBy().limit()`, not a window, to avoid a single-partition sort.
- **Anomaly baseline for `error_rate`** also leaves out hours with fewer than `min_requests` requests, not just gap hours (a 2-request hour with 1 error is noise, not a baseline). Std is the sample std.
- **`quality_report`** goes to `paths.quality` (`/logs/quality/`, plan §6.1), not `/logs/analytics/`. `common.io.table_path(cfg, name)` knows this; use it instead of building paths.
- **Unique hosts are exact** (`countDistinct`), per plan M5b.
- The runner's fail-fast unit test now checks the export stage (C7), because the ETL exists and the old test would have run it for real.

## How to run

```bash
# inside the client container (or any machine with pyspark 3.5.3 + Java 11/17)
python scripts/pipeline.py etl --env local          # the sample -> data/local/...
python scripts/pipeline.py analytics --env local
python -m loganalysis.analytics.anomalies --env local   # any single job
python -m pytest -q                                  # 97 tests; the 46 Spark ones skip where pyspark is missing
```

Without the cluster, from the host: `docker compose -f infra/docker-compose.yml --profile lite run --rm --no-deps client python -m pytest`.

## Code map

```
src/loganalysis/
  contracts.py              TABLES: every output table's layer, columns, types, producing module
  common/spark.py           get_spark(app, cfg)       common/io.py   table_path, conform, write_table, read_parsed
  common/job.py             run_job(name, run, doc)   common/log.py  get_logger
  parsing/schema.py         reason codes, PARSED_COLUMNS, parsed_struct()
  parsing/spark_parser.py   annotate(lines) -> parsed_records(), rejected_records()
  etl/parse_logs.py         read_raw, quality_report, run
  analytics/common.py       hour_bucket, top_k, pct_of_total
  analytics/{errors,traffic,top_n,anomalies}.py   pure compute functions + compute(parsed, cfg) + run(spark, cfg)
tests/
  conftest.py               spark fixture, make_parsed(lines), log_line(...)
  unit/test_parser_parity.py, unit/test_analytics_*.py, unit/test_contracts.py
  integration/test_sample_pipeline.py   ETL -> 4 jobs on the sample; contracts; Python count; Spark vs MapReduce
```

## For Track C

- **Import the contract:** `from loganalysis.contracts import TABLES, column_names`, and `from loganalysis.common.io import table_path` for where each table lives (note `quality_report` → `paths.quality`). Mock data built from `TABLES` cannot drift from the real output.
- **Timestamps:** format them inside Spark (`date_format(col, "yyyy-MM-dd'T'HH:mm:ssXXX")`) before collecting. Collecting a timestamp converts it to the Python process's time zone, which is UTC in the container, not the server's -04:00.
- `pct` is 0–100, `error_rate` 0–1, `day_of_week` 1 = Monday. `resource_type_breakdown` has every extension seen (dozens of rare ones); group the tail into "other" on the chart.
- `top_error_resources` has several statuses: filter `status = 404` for "broken links".
- Gap rows in `anomalies` have null baseline and z: label them "no data" rather than plotting a z-score. Per the plan, tell the two holes apart honestly (28 Jul–1 Aug is missing data, 1–3 Aug is a server outage).

## Open questions

1. Who takes Track C?
2. The demo date.
3. Who reviews Track B (and Track A)? Both tracks are waiting in "review".
