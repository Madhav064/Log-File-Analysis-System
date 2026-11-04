"""The REST API (plan §6.6). Every response is JSON: {"source": [tables], "data": ...}; errors are
{"error": "...", "code": 400|404|503}."""
from flask import Blueprint, current_app, jsonify, request

from ..services import queries as q
from ..services.repository import DataMissing
from ..services.stages import STAGES, schema
from .validation import BadParam, choice, date_range, integer

api = Blueprint("api", __name__, url_prefix="/api")

GRANULARITY = ("hour", "day")
MAX_N = 100


def repo():
    return current_app.extensions["repository"]


def ok(source, data, **extra):
    return jsonify({"source": source if isinstance(source, list) else [source], "data": data, **extra})


@api.errorhandler(BadParam)
def bad_param(e):
    return jsonify({"error": str(e), "code": 400}), 400


@api.errorhandler(DataMissing)
def data_missing(e):
    return jsonify({"error": str(e), "code": 503}), 503


@api.app_errorhandler(404)
def not_found(e):
    if request.path.startswith("/api/"):
        return jsonify({"error": f"no such endpoint: {request.path}", "code": 404}), 404
    return e


# ------------------------------------------------------------------------------------------------- summary
@api.get("/summary")
def summary():
    return ok(["quality_report", "traffic_daily", "traffic_hourly", "status_counts", "traffic_peaks",
               "hour_of_day_profile", "anomalies"], q.summary(repo()))


# -------------------------------------------------------------------------------------------------- errors
@api.get("/errors/status")
def errors_status():
    rows = sorted(repo().rows("status_counts"), key=lambda r: r["status"])
    return ok("status_counts", rows, classes=q.status_classes(rows))


@api.get("/errors/timeline")
def errors_timeline():
    granularity = choice(request.args, "granularity", GRANULARITY, "hour")
    start, end = date_range(request.args, *q.data_range(repo()))
    return ok("error_timeline_hourly", q.error_timeline(repo(), granularity, start, end), granularity=granularity)


@api.get("/errors/top-resources")
def errors_top_resources():
    n = integer(request.args, "n", 10, 1, MAX_N)
    rows = repo().rows("top_error_resources")
    statuses = sorted({r["status"] for r in rows})
    status = integer(request.args, "status", None, 100, 599)
    if status is not None:
        rows = [r for r in rows if r["status"] == status]
    return ok("top_error_resources", q.top_n(rows, "requests", n), statuses=statuses)


@api.get("/errors/top-hosts")
def errors_top_hosts():
    n = integer(request.args, "n", 10, 1, MAX_N)
    return ok("top_error_hosts", q.top_n(repo().rows("top_error_hosts"), "errors", n))


# ------------------------------------------------------------------------------------------------- traffic
@api.get("/traffic/timeline")
def traffic_timeline():
    granularity = choice(request.args, "granularity", GRANULARITY, "day")
    start, end = date_range(request.args, *q.data_range(repo()))
    source, rows = q.traffic_timeline(repo(), granularity, start, end)
    return ok(source, rows, granularity=granularity)


@api.get("/traffic/profile")
def traffic_profile():
    by = choice(request.args, "by", ("hour", "weekday"), "hour")
    name = "hour_of_day_profile" if by == "hour" else "day_of_week_profile"
    key = "hour" if by == "hour" else "day_of_week"
    return ok(name, sorted(repo().rows(name), key=lambda r: r[key]), by=by)


@api.get("/traffic/peaks")
def traffic_peaks():
    granularity = choice(request.args, "granularity", ("minute", "hour", "day"))
    return ok("traffic_peaks", q.peaks(repo(), granularity))


# --------------------------------------------------------------------------------------------------- top N
@api.get("/top/resources")
def top_resources():
    by = choice(request.args, "by", ("hits", "bytes"), "hits")
    n = integer(request.args, "n", 10, 1, MAX_N)
    name, key = ("top_resources_by_hits", "requests") if by == "hits" else ("top_resources_by_bytes", "bytes")
    return ok(name, q.top_n(repo().rows(name), key, n), by=by)


@api.get("/top/hosts")
def top_hosts():
    n = integer(request.args, "n", 10, 1, MAX_N)
    return ok("top_hosts", q.top_n(repo().rows("top_hosts"), "requests", n))


@api.get("/top/resource-types")
def top_resource_types():
    rows, chart = q.resource_types(repo().rows("resource_type_breakdown"))
    return ok("resource_type_breakdown", rows, chart=chart)


# ----------------------------------------------------------------------------------------------- anomalies
@api.get("/anomalies")
def anomalies():
    metric = choice(request.args, "metric", ("requests", "error_rate"))
    kind = choice(request.args, "kind", ("spike", "drop", "gap"))
    every = repo().rows("anomalies")
    rows = [r for r in every if (not metric or r["metric"] == metric) and (not kind or r["kind"] == kind)]
    return ok("anomalies", sorted(rows, key=lambda r: (r["hour_ts"], r["metric"])),
              counts=q.anomaly_counts(every), gap_runs=q.gap_runs(every))


# ------------------------------------------------------------------------------------------------ pipeline
@api.get("/pipeline/quality")
def pipeline_quality():
    return ok("quality_report", repo().rows("quality_report")[0])


@api.get("/pipeline/benchmark")
def pipeline_benchmark():
    return ok("engine_benchmark", repo().rows("engine_benchmark"))


@api.get("/pipeline/ingestion")
def pipeline_ingestion():
    return ok("ingestion_report", repo().rows("ingestion_report"))


@api.get("/pipeline/runs")
def pipeline_runs():
    return ok("pipeline_runs", repo().rows("pipeline_runs"))


@api.get("/meta/stages")
def meta_stages():
    stages = [{**s, "jobs": [list(j) for j in s["jobs"]],
               "schemas": {t: [list(c) for c in schema(t)] for t in s["tables"]}} for s in STAGES]
    return ok([], stages)


@api.get("/health")
def health():
    try:
        manifest = repo().manifest()
    except DataMissing as e:
        return jsonify({"status": "no data", "error": str(e), "code": 503}), 503
    tables = manifest["tables"]
    present = [n for n, e in tables.items() if not e["missing"]]
    return jsonify({"status": "ok", "generated_at": manifest["generated_at"], "dataset": manifest["dataset"],
                    "env": manifest["env"], "files_present": len(present), "files_listed": len(tables),
                    "missing": sorted(set(tables) - set(present)), "tables": tables})
