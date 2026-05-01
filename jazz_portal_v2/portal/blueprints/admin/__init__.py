from flask import Blueprint

bp = Blueprint(
    "admin",
    __name__,
    template_folder="templates",
    url_prefix="/admin",
)

from portal.blueprints.admin import routes  # noqa: E402, F401
