from flask import Blueprint

bp = Blueprint("home", __name__, template_folder="templates", url_prefix="/")

from portal.blueprints.home import routes  # noqa
