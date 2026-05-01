"""
portal/auth.py
==============
admin_required decorator used by all admin blueprint routes.
"""

import functools
from flask import session, redirect, url_for, request, jsonify


def admin_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("admin"):
            if request.is_json:
                return jsonify({"error": "Unauthorised"}), 401
            return redirect(url_for("admin.login", next=request.full_path))
        return f(*args, **kwargs)
    return wrapper
