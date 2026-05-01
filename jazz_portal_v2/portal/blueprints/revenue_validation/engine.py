"""
portal/blueprints/revenue_validation/engine.py
===============================================
All execution logic for Revenue Validation.

Public API
----------
run_validation(job)  ->  None   (mutates job dict in-place, runs in background thread)
build_job(...)       ->  dict   (creates the job dict before kicking off the thread)
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import numpy as np

from portal import db, connections as conn_mgr
from portal.blueprints.revenue_validation.components import COMPONENTS

# ── Job store ─────────────────────────────────────────────────────────────────
# Simple in-memory dict keyed by job_id.
# Good enough for a local / small-team deployment.
# For multi-process deployments swap this for Redis.

_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()


def get_job(job_id: str) -> dict | None:
    with _jobs_lock:
        return _jobs.get(job_id)


def all_job_ids() -> list[str]:
    with _jobs_lock:
        return list(_jobs.keys())


# ── Date helpers ─────────────────────────────────────────────────────────────
# Dates arrive from the frontend as YYYYMMDD (dashes stripped by JS).
# RAID (Oracle) accepts YYYYMMDD natively via bind params.
# Hive expects YYYY-MM-DD in string literals.

def _to_hive_date(d: str) -> str:
    """Convert YYYYMMDD → YYYY-MM-DD for Hive string interpolation."""
    d = d.replace("-", "")   # tolerate if dashes weren't stripped
    if len(d) == 8:
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    return d  # pass through if already formatted or unexpected


def _to_oracle_date(d: str) -> str:
    """Ensure YYYYMMDD for Oracle bind params (strip dashes if present)."""
    return d.replace("-", "")


# ── Query execution ───────────────────────────────────────────────────────────

def _run_query(sql: str, source: str, start_date: str, end_date: str) -> pd.DataFrame:
    t0 = time.time()
    if source == "raid":
        oracle_sd = _to_oracle_date(start_date)
        oracle_ed = _to_oracle_date(end_date)
        conn = conn_mgr.get_oracle()
        try:
            df = pd.read_sql(sql, conn, params={"start_date": oracle_sd, "end_date": oracle_ed})
        finally:
            conn.close()
    else:
        hive_sd = _to_hive_date(start_date)
        hive_ed = _to_hive_date(end_date)
        rendered = sql.replace("{start_date}", hive_sd).replace("{end_date}", hive_ed)
        conn = conn_mgr.get_hive()
        try:
            df = pd.read_sql(rendered, conn)
        finally:
            conn.close()
    df.columns = df.columns.str.lower()
    df["_seconds"] = round(time.time() - t0, 2)
    return df


# ── RAID vs Hive comparison ───────────────────────────────────────────────────
# Rule (from original notebook):
#   Flag Hive as preferred source when
#   (hive_total_events - raid_total_events) / raid_total_events < -1%

def _compare(df_raid: pd.DataFrame, df_hive: pd.DataFrame,
             indexes: list[str]) -> pd.DataFrame:
    cols = ["total_events", "total_amount"]

    r = df_raid.set_index(indexes)[cols]
    h = df_hive.set_index(indexes)[cols]
    r, h = r.align(h, join="outer")

    diff = (r - h).rename(columns=lambda c: f"{c}_diff")
    merged = pd.concat([r.add_suffix("_raid"), h.add_suffix("_hive"), diff], axis=1).fillna(0)

    merged["total_events_diff_pct"] = np.where(
        merged["total_events_raid"] != 0,
        merged["total_events_diff"] / merged["total_events_raid"] * 100,
        0,
    ).round(2)

    merged["preferred_source"] = np.where(
        merged["total_events_diff_pct"] < -1, "hive", "raid"
    )

    merged["total_amount"] = np.where(
        merged["preferred_source"] == "hive",
        merged["total_amount_hive"],
        merged["total_amount_raid"],
    )

    return merged.reset_index()


# ── Single-component runner ───────────────────────────────────────────────────

def _run_component(comp_key: str, source: str,
                   raid_start: str, raid_end: str,
                   hive_start: str, hive_end: str) -> dict:
    meta = COMPONENTS[comp_key]
    result: dict = {
        "key":          comp_key,
        "label":        meta.label,
        "group":        meta.group,
        "total_events": 0,
        "total_amount": 0.0,
        "errors":       {},   # per-source errors visible in the UI
    }

    raid_sql = db.get_query(comp_key, "raid")
    hive_sql = db.get_query(comp_key, "hive")

    df_raid = df_hive = None

    # ── RAID ──────────────────────────────────────────────────────────────
    if source in ("raid", "both") and raid_sql:
        try:
            df_raid = _run_query(raid_sql, "raid", raid_start, raid_end)
            result["raid_total_events"] = int(df_raid["total_events"].sum()) if "total_events" in df_raid.columns else 0
            result["raid_total_amount"] = float(df_raid["total_amount"].sum()) if "total_amount" in df_raid.columns else 0.0
            result["raid_rows"]         = df_raid.to_dict("records")
        except Exception as exc:
            result["errors"]["raid"] = str(exc)

    # ── Hive ──────────────────────────────────────────────────────────────
    if source in ("hive", "both") and hive_sql:
        try:
            df_hive = _run_query(hive_sql, "hive", hive_start, hive_end)
            result["hive_total_events"] = int(df_hive["total_events"].sum()) if "total_events" in df_hive.columns else 0
            result["hive_total_amount"] = float(df_hive["total_amount"].sum()) if "total_amount" in df_hive.columns else 0.0
            result["hive_rows"]         = df_hive.to_dict("records")
        except Exception as exc:
            result["errors"]["hive"] = str(exc)

    # ── Comparison (both sources) ─────────────────────────────────────────
    if source == "both" and df_raid is not None and df_hive is not None:
        comp_df = _compare(df_raid, df_hive, meta.indexes)
        result["comparison_rows"]  = comp_df.to_dict("records")
        result["total_events"]     = int(result.get("raid_total_events", 0))
        result["total_amount"]     = float(comp_df["total_amount"].sum()) if "total_amount" in comp_df.columns else 0.0
    elif source == "raid" and df_raid is not None:
        result["total_events"] = result.get("raid_total_events", 0)
        result["total_amount"] = result.get("raid_total_amount", 0.0)
    elif source == "hive" and df_hive is not None:
        result["total_events"] = result.get("hive_total_events", 0)
        result["total_amount"] = result.get("hive_total_amount", 0.0)
    else:
        result["total_events"] = 0
        result["total_amount"] = 0.0

    return result


# ── Job builder & runner ──────────────────────────────────────────────────────

def build_job(components: list[str], source: str,
              start_date: str, end_date: str,
              raid_start: str, raid_end: str,
              hive_start: str, hive_end: str) -> str:
    job_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    with _jobs_lock:
        _jobs[job_id] = {
            "status":   "pending",
            "total":    len(components),
            "done":     0,
            "results":  {},
            "errors":   {},
            "params": {
                "source":     source,
                "start_date": start_date,
                "end_date":   end_date,
            },
        }
    return job_id


def run_validation(job_id: str, components: list[str], source: str,
                   start_date: str, end_date: str,
                   raid_start: str, raid_end: str,
                   hive_start: str, hive_end: str,
                   max_workers: int = 6):
    """Runs in a background thread. Mutates the job dict in _jobs."""

    def _mark(key, value):
        with _jobs_lock:
            _jobs[job_id][key] = value

    _mark("status", "running")

    results: dict = {}
    errors:  dict = {}

    def task(comp_key: str):
        return comp_key, _run_component(
            comp_key, source,
            raid_start, raid_end,
            hive_start, hive_end,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(task, k): k for k in components}
        for fut in as_completed(futures):
            k = futures[fut]
            try:
                key, res = fut.result()
                results[key] = res
            except Exception as exc:
                errors[k] = str(exc)
            with _jobs_lock:
                _jobs[job_id]["done"] += 1

    with _jobs_lock:
        _jobs[job_id].update(status="done", results=results, errors=errors)
