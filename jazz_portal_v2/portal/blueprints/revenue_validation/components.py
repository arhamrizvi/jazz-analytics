"""
portal/blueprints/revenue_validation/components.py
===================================================
Static metadata for each RV component.
SQL lives in SQLite (portal.db), not here.

Structure
---------
COMPONENTS  dict[key -> ComponentMeta]

ComponentMeta fields
--------------------
label    : str   Human-readable name shown in the UI
group    : str   Group header (used for colour coding and summary cards)
indexes  : list  Columns to align RAID and Hive DataFrames on for comparison
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ComponentMeta:
    label: str
    group: str
    indexes: list[str] = field(default_factory=lambda: ["start_date"])


COMPONENTS: dict[str, ComponentMeta] = {
    "traffic_vas": ComponentMeta(
        label="Traffic VAS",
        group="Traffic",
        indexes=["start_date", "traffic_type"],
    ),
    "traffic_other": ComponentMeta(
        label="Traffic OTHER",
        group="Traffic",
        indexes=["start_date", "traffic_type"],
    ),
    "gprs": ComponentMeta(
        label="GPRS",
        group="GPRS",
    ),
    "nonusage_sub_bundles": ComponentMeta(
        label="Non-Usage Subscription Bundles",
        group="Non-Usage",
    ),
    "nonusage_vas_rbt_air": ComponentMeta(
        label="Non-Usage VAS RBT AIR",
        group="Non-Usage",
    ),
    "nonusage_vas_air": ComponentMeta(
        label="Non-Usage VAS AIR",
        group="Non-Usage",
    ),
    "nonusage_sdp_other": ComponentMeta(
        label="Non-Usage SDP Other",
        group="Non-Usage",
    ),
    "nonusage_air_other": ComponentMeta(
        label="Non-Usage AIR Other",
        group="Non-Usage",
    ),
    "nonusage_vas_rbt": ComponentMeta(
        label="Non-Usage VAS RBT",
        group="Non-Usage",
    ),
    "nonusage_vas_vic": ComponentMeta(
        label="Non-Usage VAS VIC",
        group="Non-Usage",
    ),
    "jazz_share": ComponentMeta(
        label="Jazz Share",
        group="Jazz Share",
    ),
    "jazz_adv_fee": ComponentMeta(
        label="Jazz Adv. Service Fee",
        group="Jazz Adv. Service Fee",
    ),
}


# Group ordering for display
GROUP_ORDER = [
    "Traffic",
    "GPRS",
    "Non-Usage",
    "Jazz Share",
    "Jazz Adv. Service Fee",
]

GROUP_COLORS = {
    "Traffic":               "#00c9f5",
    "GPRS":                  "#f5a623",
    "Non-Usage":             "#7c3aed",
    "Jazz Share":            "#0fba80",
    "Jazz Adv. Service Fee": "#f04040",
}
