# Demo script (about 10 minutes)

A walk through the system in pipeline order. Times are targets; rehearse twice (task I6).

## Before the demo (not shown)

- Docker has at least 8 GB of memory, and other containers are stopped.
- The cluster is up and the pipeline has run at least once:
  ```
  docker compose -f infra/docker-compose.yml --profile lite up -d --wait
  python scripts/pipeline.py all --env cluster
  ```
- Open these tabs in this order:
  1. the dashboard, http://localhost:5000
  2. HDFS, http://localhost:9870
  3. YARN, http://localhost:8088
  4. Spark, http://localhost:8080
- Choose light or dark mode to suit the projector (the toggle is in the top bar).

## 1. The problem and the data (1 min): dashboard, 01 Overview

- "We analyse two months of NASA web server logs: one line per request, 3.46 million lines."
- Point at the KPI cards: total requests, unique hosts, data served, error rate, busiest hour.
- Point at the daily chart and its two bands: "**grey** is missing data (the July file stops on 28 July), **red** is a real server outage (the hurricane, 1–3 August). The system tells them apart."
- Point at the stepper along the top: "the dashboard follows the pipeline, one page per stage."

## 2. The architecture (1 min): 02 Architecture

- Read the flow left to right: HDFS storage → Spark and MapReduce → comparison → export → this dashboard.
- "One parser is shared by both engines, and one command runs every stage: `pipeline.py all`."

## 3. Storage in HDFS (1½ min): 03 Ingestion, then the HDFS tab

- Two files, each with its checksum verified, cut into blocks, `fsck` HEALTHY.
- On the HDFS tab (Utilities → Browse), open `/logs/raw/nasa/month=1995-07/`. "The same blocks, seen by Hadoop itself."

## 4. Parsing (1 min): 04 Parsing & ETL

- The real first log line, before and after: 13 typed fields.
- "All but 12 lines parse. The 12 are kept aside with a reason, not guessed at."

## 5. The analyses (3 min): 05 → 08

- **Errors:** the status codes on a log scale. Switch the timeline to *Hour*, then show the top broken links (404).
- **Traffic:** the daily rhythm (afternoon peak) and quieter weekends. Open the busiest-periods table on *Day*.
- **Top N:** switch *Hits* to *Bytes* and show that the ranking changes (big files).
- **Anomalies:** explain the z-score in one sentence ("the same hour on the previous 7 days"). Point at the spikes on the busiest day and the two gap runs, labelled differently.

## 6. Two engines, same answer (1½ min): 09 Engine comparison, then the YARN tab

- Three cards, "results match": Spark and MapReduce agree row for row.
- The runtime chart: how much longer MapReduce takes and why (disk between the steps).
- On the YARN tab, show the finished MapReduce applications and the `malformed_lines` counter.

## 7. How it reaches the page (30 s): 10 Export & API

- The exported files and the manifest.
- Click `/api/summary` in the explorer: "every number on these pages comes from this API."

## 8. Questions

The Guide (sidebar, bottom) explains every page and term if a question goes into detail.
