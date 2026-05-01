from flask import Blueprint

bp = Blueprint(
    "revenue_validation",
    __name__,
    template_folder="templates",
    url_prefix="/revenue-validation",
)

from portal.blueprints.revenue_validation import routes  # noqa: E402, F401
