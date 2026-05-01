from flask import Blueprint

bp = Blueprint("commissions", __name__, template_folder="templates", url_prefix="/commissions")

from portal.blueprints.commissions import routes  # noqa
