# How to use the dashboard

This guide explains every screen of the dashboard in plain words. You don't need to know Hadoop or Spark to follow it. Words in **bold** are explained in the [glossary](#glossary) at the end.

## What this project does {#what-it-does}

NASA's Kennedy Space Center web server kept a log of every visit to its website. Each visit, or *request*, is one line of text: who asked, when, for which page, and whether it worked. We have the logs for **July and August 1995**: about 3.46 million lines, 373 MB of text.

The project answers simple questions about those visits:

- **How often did requests fail?** (Errors page)
- **When was the site busy?** (Traffic page)
- **Which pages and visitors were the most active?** (Top N page)
- **Were there unusual hours?** (Anomalies page)

To answer them, the logs pass through a **pipeline**: a fixed series of steps, each one taking the output of the step before.

1. **Ingestion:** copy the logs into **HDFS**, the cluster's shared storage.
2. **Parsing & ETL:** turn each text line into neat columns.
3. **Analytics:** count and rank things with **Spark**.
4. **Engine comparison:** count the same things with **MapReduce**, to check that Spark got them right.
5. **Export:** save the small results as files a web page can read.
6. **This dashboard:** draws those results.

The dashboard follows the same order, so you can walk through the pipeline from start to finish.

## Opening the dashboard {#opening}

Start the cluster and the dashboard, then open **http://localhost:5000** in a browser:

```
docker compose -f infra/docker-compose.yml --profile lite up -d
```

If the pipeline has never run, the charts say **"No data yet"** and show the command to run (`python scripts/pipeline.py all --env cluster`). The dashboard never makes up numbers: everything it shows comes from the last pipeline run.

## The parts of every screen {#layout}

| Part | Where | What it is for |
|---|---|---|
| **Top bar** | top | The project name; a badge with the number of log lines, the dates they cover and the server's time zone; when the pipeline last ran; the **light/dark mode** button. |
| **Pipeline steps** | under the top bar | The pipeline as a row of steps, from Source to Dashboard. A tick (✓) means the step finished successfully. The step you are looking at is filled in. Click a step to open its page. |
| **Sidebar** | left | All pages, numbered 01–10 in pipeline order and grouped: Overview, Data pipeline, Analytics, Validation, Serving. The Guide is at the bottom. |
| **Page title** | top of each page | The stage number and name, plus one sentence saying what the stage does. **How to read this page** jumps to that page's section of this guide. |
| **Charts and tables** | middle | The results. Hover over a chart to see exact numbers. Under each panel, **Source** names the table the numbers come from. |
| **Stage details** | bottom of each page | The technical facts: what the stage reads (**Input**), what it writes (**Output**), which program produced it and how long it took, and the columns of each table on the page (**Schema**). Click the title to hide or show it. |
| **Previous / Next** | very bottom | Walk through the stages in order. |

**Light and dark mode:** the dashboard starts in the same mode as your computer. The button in the top bar switches between them, and the browser remembers your choice.

**Time:** all times are the **server's own clock**, which was 4 hours behind UTC (written −04:00). "09:00" means 9 in the morning in Florida, where the server was.

## 01 · Overview {#stage-overview}

The summary of everything on one page.

- **Total requests:** how many log lines could be read, in other words how many requests the server answered. The small text also gives all lines read and how many were unreadable.
- **Unique hosts:** how many different computers made requests. It is an exact count of distinct names and addresses.
- **Data served:** the total size of all answers the server sent.
- **Error rate:** the share of requests that failed, meaning those with a **status code** of 400 or more.
- **Busiest hour:** the single hour with the most requests, and how many it had.
- **Date range:** how many calendar days the logs cover, and how many of them actually have traffic. These differ because of the two holes in the data explained [below](#gaps).
- **Requests per day:** one point per day. The grey band marks days **with no log data**, and the red band marks the **server outage**. The notes under the chart give the exact hours.
- **Responses by status class:** the share of answers in each family: 2xx success, 3xx redirect, 4xx client error, 5xx server error.
- **What the data shows:** four facts read straight from the tables: the busiest day, the daily rhythm, weekends compared with weekdays, and the most requested file.
- **Pipeline at a glance:** each stage, the engine that ran it, how long it took and whether it worked. Click a stage to open its page.

## 02 · Architecture {#stage-architecture}

A map of the system. It doesn't depend on the data.

- **Data flow:** read it left to right. The logs come in (Source), are stored in HDFS (Storage), processed by Spark and MapReduce (Processing), checked against each other (Validation), then exported and shown here (Serving). Dashed boxes are shared parts: the **parser** used by both engines, and the **pipeline runner** that starts every stage.
- **Technology stack:** the software and versions used.
- **Cluster services:** the programs (**containers**) that make up the cluster, what each does, and the address of its own web page where it has one.

## 03 · Ingestion (HDFS) {#stage-ingestion}

How the raw logs got into the cluster's storage.

- **One card per file** (July and August):
  - **Lines:** how many lines the file has.
  - **Size:** how big it is.
  - **Partition:** the folder it went into (`month=1995-07`).
  - **SHA-256:** the file's **checksum**, a fingerprint that proves the download is complete and unchanged.
  - **Blocks:** how many pieces HDFS cut the file into.
  - **Size check:** confirms that the copy in HDFS is exactly as large as the file on disk.
- **What the loader does, in order:** the six steps, from download to health check.
- **HDFS storage:**
  - the total stored;
  - the number of **blocks**;
  - the **replication** (how many copies of each block are kept);
  - the result of the **fsck** health check. HEALTHY means no block is missing or damaged.
  - The bar shows the blocks to scale. Splitting files into blocks is what lets many workers read them at the same time.
- **HDFS layout:** the folders the pipeline uses, and the command that runs this stage.

## 04 · Parsing & ETL {#stage-etl}

**ETL** means *extract, transform, load*: here, turning raw text lines into tidy columns.

- **Lines read / Parsed / Rejected / Reject rate:** every line is either parsed (turned into columns) or rejected (kept aside with a reason). Almost every line parses.
- **One line, before and after:** a real line from the logs, and the 13 fields the parser makes from it. The table explains each field.
- **Parsed vs rejected:** the bar shows the split. The rejected part is drawn wider than it really is, so you can see it at all. The table lists each **rejection reason**:
  - `NO_MATCH`: the line is not in the expected format at all, such as the cut-off last line of the July file.
  - `BAD_REQUEST`: the request part is garbage, such as binary junk.
  - `BAD_TIMESTAMP` and `BAD_STATUS`: an impossible date, or a status number outside 100–599.
- **Quality checks:**
  - nothing is lost (parsed + rejected = lines read);
  - Spark and plain Python read every line the same way;
  - every table has exactly the columns it should;
  - times stay in the server's clock.

## 05 · Errors {#stage-errors}

An **error** is any request whose **status code** is 400 or higher.

- **Requests by status code:** one bar per code. The height scale is **logarithmic**: each step up means ten times more. Without it, the rare codes (a few dozen 500s) would be invisible next to millions of 200s.
- **Status codes in numbers:** the same data as a table, with what each code means (404 = "not found", 500 = "server error" …).
- **Errors over time:** the percentage of requests that failed, as two lines: 4xx (problems with the request, mostly broken links) and 5xx (problems on the server).
  - **Hour/Day** changes how finely time is cut.
  - The **date boxes** zoom in on part of the two months.
  - A break in a line means nothing was logged then.
- **Resources with the most errors:** pages or files that failed most often. Choose a status in the box; 404 lists the **broken links**.
- **Hosts with the most errors:** the computers whose requests failed most often.

## 06 · Traffic {#stage-traffic}

When the site was busy.

- **Requests over time:** the solid line is requests and the dashed line is **unique hosts**, both per hour or per day. Use **Hour/Day** and the date boxes as on the Errors page. The grey and red bands are the [two holes in the data](#gaps).
- **Average requests by hour of day:** a typical day, hour by hour. The darkest bar is the busiest hour.
- **Average requests by day of week:** a typical week. Weekend days are drawn in a second colour.
- **Busiest periods:** the top 10 minutes, hours and days. Use the tabs to switch.

"Average" here means averaged over the days that actually have traffic, so the missing days don't drag the averages down.

## 07 · Top N {#stage-top-n}

"Top N" means "the N largest": the most popular things.

- **Rank by Hits/Bytes:** *hits* counts requests. *Bytes* counts data sent, so a large file asked for less often can still rank high.
- **Show 10/20:** how many to list.
- **Most requested resources:** pages and files, longest bar first. Long paths are shortened; hover to see the full path.
- **Most active hosts:** the computers that made the most requests, and how much data they received.
- **Requests by file type:** the mix of file types, taken from the file extension (gif, html, jpg …). "directory" means a folder address ending in `/`, and "none" means no extension. The many rare types are grouped as "other".

## 08 · Anomalies {#stage-anomalies}

An **anomaly** is an hour that doesn't look like the hours around it.

- **How an hour is judged:** each hour is compared with *the same hour of day* on the previous 7 days. 09:00 is compared with the last seven mornings at 09:00, because traffic always rises and falls through the day. The comparison gives a **z-score**:
  - **Spike:** far busier than usual (z above +3).
  - **Drop:** far quieter than usual (z below −3).
  - **Gap:** no requests at all.
  - The settings shown come from `config/pipeline.yaml`.
- **What was flagged:** how many hours of each kind. "Error rate" spikes are often small in real terms: when errors are normally very rare, a few extra already look unusual.
- **Requests per hour, with flagged hours:** the hourly line with ▲ spikes and ▼ drops marked, and the gaps shaded. Hover a marker to see its z-score and the usual value. Use the date boxes to zoom in.
- **Every flagged hour:** the full list. Filter it by metric (requests or error rate) and by kind. "Usual (mean)" and "Spread (std)" are the **baseline** the hour was compared with. Gap rows say "no data" because there was nothing to compare.

### The two holes in the data {#gaps}

The logs have two stretches with no requests at all. They look the same on a chart but have different causes, and the dashboard labels them differently:

| Hole | When (server time) | Cause | Shown as |
|---|---|---|---|
| **No log data** | 28 Jul ~14:00 → 31 Jul 23:00 | The July log file simply stops on 28 July, and the August file starts on 1 August. The hours in between are in neither file. They are **missing from the data**, not a quiet server. | grey band |
| **Server outage** | 1 Aug ~15:00 → 3 Aug 03:00 | Inside the August file, the server logged nothing: it had been shut down (a hurricane hit Florida). | red band |

The anomaly job leaves both out of every baseline, so the following week is still compared fairly.
