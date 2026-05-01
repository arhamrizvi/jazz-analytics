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

    if not selected:
        return jsonify({"error": "No components selected"}), 400
    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date required"}), 400

    max_workers = current_app.config.get("MAX_WORKERS", 6)
    job_id = engine.build_job(selected, source, start_date, end_date,
                              raid_start, raid_end, hive_start, hive_end)

    threading.Thread(
        target=engine.run_validation,
        args=(job_id, selected, source, start_date, end_date,
              raid_start, raid_end, hive_start, hive_end, max_workers),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_id})


@bp.route("/api/job/<job_id>")
def api_job_status(job_id):
    job = engine.get_job(job_id)
    if not job:
        return jsonify({"error": "not found"}), 404
    return jsonify(job)


@bp.route("/api/job/<job_id>/stream")
def api_job_stream(job_id):
    """Server-Sent Events — live progress while job runs."""
    import time

    def generate():
        while True:
            job = engine.get_job(job_id) or {}
            payload = json.dumps({
                "status": job.get("status"),
                "done":   job.get("done", 0),
                "total":  job.get("total", 0),
            })
            yield f"data: {payload}\n\n"
            if job.get("status") == "done":
                break
            time.sleep(0.4)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":       "keep-alive",
        },
    )
