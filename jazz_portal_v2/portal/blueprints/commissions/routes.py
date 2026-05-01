from flask import render_template
from portal.blueprints.commissions import bp


@bp.route("/")
def index():
    return render_template("home/placeholder.html",
                           module="Commissions", icon="💰",
                           description="Commission calculation and reconciliation module. Under development.")
