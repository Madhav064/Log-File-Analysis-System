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
