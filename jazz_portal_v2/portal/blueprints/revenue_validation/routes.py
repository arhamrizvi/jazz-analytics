import json
import threading
from flask import render_template, request, jsonify, Response, current_app

from portal.blueprints.revenue_validation import bp
from portal.blueprints.revenue_validation.components import COMPONENTS, GROUP_COLORS
from portal.blueprints.revenue_validation import engine
from portal import db


def _enrich_components() -> dict:
    """Add has_raid / has_hive flags by checking the query store."""
    return {
        key: {
            "label":    meta.label,
            "group":    meta.group,
            "has_raid": db.get_query(key, "raid") is not None,
            "has_hive": db.get_query(key, "hive") is not None,
        }
        for key, meta in COMPONENTS.items()
    }


@bp.route("/")
def index():
    return render_template(
        "revenue_validation/index.html",
        components=_enrich_components(),
        group_colors=GROUP_COLORS,
    )


@bp.route("/api/run", methods=["POST"])
def api_run():
    body         = request.json or {}
    selected     = body.get("components", list(COMPONENTS.keys()))
    source       = body.get("source", "raid")
    start_date   = body.get("start_date", "")
    end_date     = body.get("end_date", "")
    raid_start   = body.get("raid_start") or start_date
    raid_end     = body.get("raid_end")   or end_date
    hive_start   = body.get("hive_start") or start_date
    hive_end     = body.get("hive_end")   or end_date
    run_by       = (body.get("run_by") or "anonymous").strip() or "anonymous"

    if not selected:
        return jsonify({"error": "No components selected"}), 400
    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date required"}), 400

    max_workers = current_app.config.get("MAX_WORKERS", 6)
    job_id = engine.build_job(selected, source, start_date, end_date,
                              raid_start, raid_end, hive_start, hive_end,
                              run_by=run_by)

    threading.Thread(
        target=engine.run_validation,
        args=(job_id, selected, source, start_date, end_date,
              raid_start, raid_end, hive_start, hive_end, max_workers, run_by),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_id})


@bp.route("/api/job/<job_id>")
def api_job_status(job_id):
    job = engine.get_job(job_id)
    if not job:
        return jsonify({"error": "not found"}), 404
    return jsonify(job)


@bp.route("/api/last-run")
def api_last_run():
    start_date = request.args.get("start_date", "")
    end_date   = request.args.get("end_date", "")
    source     = request.args.get("source", "")
    if not start_date or not end_date or not source:
        return jsonify({"error": "start_date, end_date, source required"}), 400
    cached = db.get_last_run(start_date, end_date, source)
    if not cached:
        return jsonify({"error": "no_cache"}), 404
    return jsonify(cached)


@bp.route("/api/runs")
def api_runs():
    return jsonify(db.list_recent_runs())


@bp.route("/api/job/<job_id>/stream")
def api_job_stream(job_id):
    """Server-Sent Events — live progress + incremental results while job runs."""
    import time

    def generate():
        sent_keys: set = set()
        while True:
            job = engine.get_job(job_id) or {}
            all_results = job.get("results", {})
            # only send keys the client hasn't seen yet
            new_keys    = set(all_results) - sent_keys
            new_results = {k: all_results[k] for k in new_keys}
            sent_keys.update(new_keys)
            payload = json.dumps({
                "status":      job.get("status"),
                "done":        job.get("done", 0),
                "total":       job.get("total", 0),
                "new_results": new_results,
            })
            yield f"data: {payload}\n\n"
            if job.get("status") == "done":
                break
            time.sleep(0.5)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":       "keep-alive",
        },
    )


@bp.route("/api/connectivity")
def api_connectivity():
    """TCP probe Oracle + Hive — used by the network indicator in the UI."""
    import socket, time

    def probe(host, port, timeout=5):
        t0 = time.time()
        try:
            s = socket.create_connection((host, port), timeout=timeout)
            s.close()
            return True, round((time.time() - t0) * 1000)
        except OSError:
            return False, None

    from portal import db as _db
    oracle_row = _db.get_connection("raid")
    hive_row   = _db.get_connection("hive")

    oracle_ok, oracle_ms = probe(oracle_row["host"], int(oracle_row["port"])) if oracle_row else (False, None)
    hive_ok,   hive_ms   = probe(hive_row["host"],   int(hive_row["port"]))   if hive_row   else (False, None)

    return jsonify({
        "oracle": {"ok": oracle_ok, "ms": oracle_ms, "host": oracle_row["host"] if oracle_row else None},
        "hive":   {"ok": hive_ok,   "ms": hive_ms,   "host": hive_row["host"]   if hive_row   else None},
    })
