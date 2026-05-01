from flask import render_template
from portal.blueprints.home import bp


MODULES = [
    {
        "name":        "Revenue Validation",
        "url":         "/revenue-validation/",
        "icon":        "✅",
        "description": "Reconcile Traffic, GPRS, Non-Usage, Jazz Share and Adv. Service Fee between RAID and Hive. 12 components, parallel execution.",
        "status":      "live",
        "color":       "#00c9f5",
    },
    {
        "name":        "Commissions",
        "url":         "/commissions/",
        "icon":        "💰",
        "description": "Commission calculation and reconciliation across distribution channels.",
        "status":      "soon",
        "color":       "#f5a623",
    },
    {
        "name":        "AIT Validation",
        "url":         "#",
        "icon":        "🧾",
        "description": "Advance Income Tax validation against billing records.",
        "status":      "soon",
        "color":       "#7c3aed",
    },
    {
        "name":        "GST Validation",
        "url":         "#",
        "icon":        "📊",
        "description": "General Sales Tax reconciliation and reporting.",
        "status":      "soon",
        "color":       "#0fba80",
    },
    {
        "name":        "IN Balance Movement",
        "url":         "#",
        "icon":        "📦",
        "description": "Track prepaid IN balance movements across time periods.",
        "status":      "soon",
        "color":       "#f04040",
    },
    {
        "name":        "IN-GL",
        "url":         "#",
        "icon":        "📒",
        "description": "IN to General Ledger reconciliation.",
        "status":      "soon",
        "color":       "#a78bfa",
    },
]


@bp.route("/")
def index():
    return render_template("home/index.html", modules=MODULES)
