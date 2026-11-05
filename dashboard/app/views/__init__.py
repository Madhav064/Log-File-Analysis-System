"""The HTML pages. Each page renders its frame (sidebar, stepper, stage details) on the server; its charts
load their data from the API in the browser."""
from pathlib import Path

from flask import Blueprint, abort, current_app, render_template

from ..services.repository import DataMissing
from ..services.stages import BY_SLUG, STAGES, STEPPER, neighbours, schema

views = Blueprint("views", __name__)

# Which stepper step each page belongs to (index into STEPPER).
STEP_OF = {"ingestion": 1, "etl": 2, "errors": 3, "traffic": 3, "top-n": 3, "anomalies": 3, "engines": 4,
           "export": 5}
GROUPS = ["Overview", "Data pipeline", "Analytics", "Validation", "Serving"]


def repo():
    return current_app.extensions["repository"]


def safe(fn, default=None):
    try:
        return fn()
    except DataMissing:
        return default


def run_state():
    """Latest runtime and status per (stage, job), and whether each stepper step has completed."""
    runs = safe(lambda: repo().rows("pipeline_runs"), [])
    by_job = {(r["stage"], r["job"]): r for r in runs}
    ok = {r["stage"] for r in runs if r["status"] == "ok"}
    failed = {r["stage"] for r in runs if r["status"] != "ok"}
    stages_ok = ok - failed
    exported = safe(lambda: repo().manifest()) is not None
    done = [repo().available("ingestion_report"), "ingest" in stages_ok, "etl" in stages_ok,
            "analytics" in stages_ok, {"mapreduce", "compare"} <= stages_ok, exported, exported]
    last = max((r["started_at"] for r in runs), default=None)
    return by_job, done, last, bool(failed)


def header():
    """Dataset badge numbers, straight from the exported quality report."""
    q = safe(lambda: repo().rows("quality_report")[0])
    m = safe(lambda: repo().manifest())
    return {"quality": q, "manifest": m}


def stage_details(stage, by_job):
    tables = []
    for name in stage["tables"]:
        entry = safe(lambda n=name: repo().manifest()["tables"].get(n))
        tables.append({"name": name, "schema": schema(name), "rows": entry["rows"] if entry else None,
                       "missing": (entry or {}).get("missing", True)})
    jobs = [{"stage": st, "job": job, "run": by_job.get((st, job))} for st, job in stage["jobs"]]
    return {"tables": tables, "jobs": jobs}


def render_stage(slug):
    stage = BY_SLUG.get(slug) or abort(404)
    by_job, done, last_run, any_failed = run_state()
    prev, nxt = neighbours(slug)
    return render_template(
        f"pages/{slug}.html", stage=stage, stages=STAGES, groups=GROUPS, stepper=STEPPER, step_done=done,
        current_step=STEP_OF.get(slug), prev=prev, next=nxt, details=stage_details(stage, by_job),
        last_run=last_run, any_failed=any_failed, head=header())


@views.get("/")
def overview():
    return render_stage("overview")


@views.get("/<slug>")
def page(slug):
    if slug == "overview":
        return overview()
    return render_stage(slug)


@views.get("/guide")
def guide():
    import markdown

    path = Path(current_app.config["GUIDE_PATH"])
    text = path.read_text(encoding="utf-8") if path.exists() else "# Guide\n\nThe guide file was not found."
    html = markdown.markdown(text, extensions=["tables", "toc", "fenced_code", "attr_list"])
    by_job, done, last_run, any_failed = run_state()
    return render_template("pages/guide.html", html=html, stage={"slug": "guide", "title": "Guide"},
                           stages=STAGES, groups=GROUPS, stepper=STEPPER, step_done=done, current_step=None,
                           last_run=last_run, any_failed=any_failed, head=header())
