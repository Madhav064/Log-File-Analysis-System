# Handoff: Track C build (export, dashboard, guide)

**Date:** 2026-11-12 · **Branch:** `feat/track-c` (local only, not pushed; small commits, one change each)
**Read this if you are reviewing Track C, running the demo, or changing the dashboard.** Plan: [`docs/PROJECT_PLAN.md`](../docs/PROJECT_PLAN.md). Tasks: [`docs/TEAM_TASKS.md`](../docs/TEAM_TASKS.md). Earlier handoffs: [Track A](2026-10-30-track-a-build.md), [Track B](2026-11-03-track-b-build.md). How to use the dashboard: [`docs/dashboard-guide.md`](../docs/dashboard-guide.md).

## Where things stand

| Task | Status | What was built |
|---|---|---|
| C1 mock data | **replaced** | No fake data anywhere (team decision). The API tests use the real pipeline's export of the real sample, `dashboard/tests/data/`, rebuilt by `dashboard/tests/make_test_data.py` |
| C2 Flask skeleton | review | `dashboard/app/`: app factory, `DATA_DIR` config, `services/repository.py` (the only reader of the JSON; cached, reloads when `manifest.json` changes), `services/stages.py` (the ten stages, paths from config, schemas from contracts) |
| C3 API | review | All §6.6 endpoints plus `/api/pipeline/ingestion`, `/api/pipeline/runs`, `/api/meta/stages`; 400 / 404 / 503 JSON errors; hourly and daily timelines filled so gaps show as breaks |
| C4–C6 pages | review | Ten pages in pipeline order + Guide, Stitch layout (sidebar, stepper, stage details, previous/next), light/dark mode, Chart.js and fonts vendored (works offline) |
| C7 export | review | `loganalysis/export/to_serving.py` (+ pure-Python `serving.py`): every table → `{columns, rows}`, timestamps formatted in Spark with the −04:00 offset, size guard, manifest written last, temp file + rename |
| C8 Docker | review | `dashboard/Dockerfile` (python 3.10 slim, gunicorn), `dashboard` service in compose (port 5000, repo mounted read-only, 256 MB) |
| C9 docs | review | `docs/dashboard-guide.md` (plain-language guide, shown in the app at `/guide`), `docs/demo-script.md`, README dashboard section and screenshots (`docs/images/`, every page in both modes), plan and task board updated |

New pipeline outputs (so the Ingestion page and runtimes are real, not typed in):
- `contracts.REPORTS` defines two small JSON reports in `paths.reports` (`data/reports/`):
  - `ingestion_report`: written by `hdfs_loader.py`. It holds sizes, lines, SHA-256, blocks, replication and `fsck` status.
  - `pipeline_runs`: written by `pipeline.py`. It holds every job's last runtime and status.
- The export copies both reports into `data/serving/`.

## Verified on the real data (Windows 11, lite profile, 12 Nov 2026)

`pipeline.py all --env cluster` ran every stage in 2,669 s:
- ingest 48 s;
- ETL 226 s;
- analytics 4 × ~1 min;
- MapReduce 339 s;
- compare + benchmark 1,725 s;
- export ~40 s.

| Check | Result |
|---|---|
| ETL | 3,461,613 lines = 3,461,601 parsed + 12 rejected (11 BAD_REQUEST, 1 NO_MATCH); 137,978 hosts; same as Track B |
| Ingestion report | 2 files, 373,056,138 B, 4 blocks of up to 128 MiB, replication 1, HEALTHY, sizes equal local; Jul 1,891,715 lines + Aug 1,569,898 = 3,461,613 |
| Spark vs MapReduce | MATCH on all three (8 / 1,369 / 20 rows), `malformed_lines=12` |
| **Benchmark** (median of 3, start-up included) | status_count Spark 48.1 s / MR 97.9 s · hourly_traffic 77.2 s / 107.5 s · top_resources 41.1 s / 163.3 s |
| Export | 18 files (16 tables + 2 reports), none missing |
| Summary | 65,524,314,915 bytes (65.5 GB), error rate 0.613%, 62 calendar days / **58 with traffic** (29, 30, 31 Jul and 2 Aug have none), 1,369 hours, peaks 13 Jul (day 134,203, hour 09:00 14,926, minute 09:49 405), busiest hour of day 15:00 |
| Anomalies | 268 = 119 gap + 34/23 request spike/drop + 88/4 error-rate spike/drop; gap runs 28 Jul 14:00–31 Jul 23:00 (82 h, labelled *no log data*) and 1 Aug 15:00–3 Aug 03:00 (37 h, *server outage*) |
| Tests | Windows (no Spark): 134 passed, 8 Spark skipped; client container: 132 passed (Spark and the export included; the dashboard tests skip there, no Flask); dashboard: 68 passed; `ruff` clean |
| Browser | All 11 pages in light and dark (headless Chrome): no console errors |

## Fixed or decided while building

- **Runtimes use the monotonic clock.** During the export the Docker VM's clock stepped back about 76 s, and a wall-clock subtraction gave −37.7 s. `pipeline.py` and the export now time with `time.monotonic()`. `compare_engines.py` (Track A) still uses `time.time()` for the benchmark. It was not affected this time, but it could be after the same kind of clock step.
- **How gap runs are labelled.** A run lasting until the end of a month is *no log data* (the logs come as one file per month); any other run is an *outage*. This is in `queries.gap_cause` and tested with the two real runs.
- **Line counting.** `count_lines` counts a last line without a newline, as Spark does. That is why July is 1,891,715 here; the plan's 1,891,714 came from `wc -l`.
- **The dashboard tests in the client image.** Its pytest 6 ignores `pythonpath`, and the image has no Flask. `dashboard/tests/conftest.py` adds the paths and skips those tests there; they run in CI, where `dev.txt` now includes `dashboard.txt`.
- **The Stitch design.** The UI follows the Stitch screens (`../UIdesign`, outside the repo) for layout and colours only. Their invented facts (Airflow, HyperLogLog, 4 DataNodes, "20× faster", fake paths) and the "Telemetry Core" name are not used. A test checks that no such branding appears.
- **The image build.** It was slow (the Apache archive serves ~130 KB/s per connection). I fetched both tarballs in parallel and served them with `--build-arg APACHE_ARCHIVE=http://host.docker.internal:8765`; the Dockerfile still checked the official SHA-512. Nothing in the repo changed for this.

## How to run

```bash
docker compose -f infra/docker-compose.yml --profile lite up -d --wait     # cluster + dashboard
python scripts/pipeline.py all --env cluster                                # ~45 min incl. benchmark
open http://localhost:5000                                                  # the dashboard
python scripts/pipeline.py export --env cluster                             # refresh the dashboard's data only
docker compose -f infra/docker-compose.yml exec -T client python dashboard/tests/make_test_data.py   # rebuild API test data
python -m pytest -q                                                         # needs requirements/dev.txt
```

## Not done / open

- **Nothing is pushed.** `git push -u origin feat/track-c` publishes it (a new branch, so no existing PR changes). Then open a PR for review.
- **Owners:** the Track C rows in `TEAM_TASKS.md` have no owner name yet.
- **`error_rate` anomalies are still noisy** (88 spikes; see the Track B handoff). The Anomalies page explains this on screen, but the threshold question is still open for the team.
- **Not tested:** the `full` profile, and the dashboard on macOS (it is plain Python and was only run on Windows here).
- **Demo:** rehearsals (I6) have not happened yet; the demo date is still open.
- **Stepper width:** at 1440 px the last step ("7. Dashboard") is partly hidden and scrolls sideways. This is cosmetic.
