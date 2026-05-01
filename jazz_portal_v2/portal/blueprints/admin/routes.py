from datetime import datetime, timezone

from flask import (
    render_template, request, jsonify,
    session, redirect, url_for,
)

from portal.blueprints.admin import bp
from portal.blueprints.revenue_validation.components import COMPONENTS
from portal.auth import admin_required
from portal import db


# ── Auth ──────────────────────────────────────────────────────────────────────

@bp.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        u = request.form.get("username", "").strip()
        p = request.form.get("password", "")
        if db.verify_admin(u, p):
            session["admin"] = u
            return redirect(request.args.get("next") or url_for("admin.query_manager"))
        error = "Invalid credentials."
    return render_template("admin/login.html", error=error)


@bp.route("/logout")
def logout():
    session.pop("admin", None)
    return redirect(url_for("home.index"))


# ── Query Manager ─────────────────────────────────────────────────────────────

@bp.route("/queries")
@admin_required
def query_manager():
    queries = db.all_queries()
    for q in queries:
        meta = COMPONENTS.get(q["component_key"])
        q["group"]           = meta.group  if meta else "—"
        q["component_label"] = meta.label  if meta else q["component_key"]
    return render_template("admin/query_manager.html",
                           queries=queries, admin=session["admin"])


@bp.route("/queries/<int:query_id>")
@admin_required
def query_detail(query_id):
    q = db.get_query_row(query_id)
    if not q:
        return "Not found", 404
    meta = COMPONENTS.get(q["component_key"])
    q["group"]           = meta.group if meta else "—"
    q["component_label"] = meta.label if meta else q["component_key"]
    audit = db.get_query_audit(query_id)
    return render_template("admin/query_detail.html",
                           q=q, audit=audit, admin=session["admin"])


@bp.route("/api/queries/<int:query_id>", methods=["PUT"])
@admin_required
def api_update_query(query_id):
    body    = request.json or {}
    new_sql = body.get("query_sql", "").strip()
    note    = body.get("note", "manual edit")
    if not new_sql:
        return jsonify({"error": "query_sql required"}), 400
    try:
        db.update_query(query_id, new_sql, by=session["admin"], note=note)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
    return jsonify({"ok": True})


@bp.route("/api/queries/<int:query_id>/restore/<int:audit_id>", methods=["POST"])
@admin_required
def api_restore_query(query_id, audit_id):
    try:
        db.restore_query(query_id, audit_id, by=session["admin"])
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
    return jsonify({"ok": True})


@bp.route("/api/queries/<int:query_id>/audit")
@admin_required
def api_query_audit(query_id):
    return jsonify(db.get_query_audit(query_id))


# ── Connections ───────────────────────────────────────────────────────────────

@bp.route("/connections")
@admin_required
def connections():
    return render_template("admin/connections.html",
                           connections=db.all_connections(), admin=session["admin"])


@bp.route("/api/connections", methods=["POST"])
@admin_required
def api_upsert_connection():
    body = request.json or {}
    required = ["conn_key", "label", "db_type", "host", "port", "database", "username"]
    missing = [f for f in required if not body.get(f)]
    if missing:
        return jsonify({"error": f"Missing: {', '.join(missing)}"}), 400

    password = body.get("password", "").strip()
    if not password:
        existing = db.get_connection(body["conn_key"])
        if existing:
            password = existing["password"]
        else:
            return jsonify({"error": "password required for new connections"}), 400

    db.upsert_connection(
        conn_key=body["conn_key"].strip(), label=body["label"].strip(),
        db_type=body["db_type"], host=body["host"].strip(),
        port=int(body["port"]), database=body["database"].strip(),
        username=body["username"].strip(), password=password,
        extra_params=body.get("extra_params", {}), by=session["admin"],
    )
    return jsonify({"ok": True})


@bp.route("/api/connections/<conn_key>/test", methods=["POST"])
@admin_required
def api_test_connection(conn_key):
    ok, msg = db.test_connection(conn_key)
    return jsonify({"ok": ok, "message": msg})


@bp.route("/api/connections/<conn_key>/audit")
@admin_required
def api_connection_audit(conn_key):
    with db.get_conn() as c:
        row = c.execute("SELECT id FROM connections WHERE conn_key=?", (conn_key,)).fetchone()
    if not row:
        return jsonify([])
    return jsonify(db.get_connection_audit(row["id"]))


# ── Audit log ─────────────────────────────────────────────────────────────────

@bp.route("/audit")
@admin_required
def audit_log():
    return render_template("admin/audit_log.html",
                           log=db.get_full_query_audit(), admin=session["admin"])


# ── Admin users ───────────────────────────────────────────────────────────────

@bp.route("/users")
@admin_required
def users():
    return render_template("admin/users.html",
                           users=db.list_admins(), admin=session["admin"])


@bp.route("/users/add", methods=["POST"])
@admin_required
def user_add():
    u = request.form.get("username", "").strip()
    p = request.form.get("password", "")
    if u and p:
        db.create_admin(u, p)
    return redirect(url_for("admin.users"))


@bp.route("/users/delete", methods=["POST"])
@admin_required
def user_delete():
    u = request.form.get("username", "")
    if u and u != session["admin"]:
        db.delete_admin(u)
    return redirect(url_for("admin.users"))
