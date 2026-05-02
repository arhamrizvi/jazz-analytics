"""
portal/db.py
============
Single SQLite store for the entire portal.

Tables
------
admin_users       id | username | password_hash | created_at
connections       id | conn_key | label | db_type | host | port | database
                  | username | password | extra_params | is_active
                  | updated_at | updated_by
connection_audit  id | conn_id | changed_by | changed_at | old_snapshot | note
queries           id | component_key | source | label | query_sql
                  | updated_at | updated_by
query_audit       id | query_id | changed_by | changed_at | old_sql | note

Notes
-----
- Passwords in connection_audit are replaced with '***REDACTED***'
- seed_*() functions only INSERT when the row does not already exist,
  so live edits are never overwritten on restart
"""

import json
import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

_DB_PATH: Path = None  # set by bootstrap()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ── Schema ─────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS admin_users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    NOT NULL UNIQUE,
    password_hash TEXT    NOT NULL,
    created_at    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS connections (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    conn_key      TEXT    NOT NULL UNIQUE,
    label         TEXT    NOT NULL,
    db_type       TEXT    NOT NULL CHECK(db_type IN ('oracle','hive')),
    host          TEXT    NOT NULL,
    port          INTEGER NOT NULL,
    database      TEXT    NOT NULL,
    username      TEXT    NOT NULL,
    password      TEXT    NOT NULL,
    extra_params  TEXT    NOT NULL DEFAULT '{}',
    is_active     INTEGER NOT NULL DEFAULT 1,
    updated_at    TEXT    NOT NULL,
    updated_by    TEXT    NOT NULL DEFAULT 'system'
);

CREATE TABLE IF NOT EXISTS connection_audit (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    conn_id      INTEGER NOT NULL REFERENCES connections(id),
    changed_by   TEXT    NOT NULL,
    changed_at   TEXT    NOT NULL,
    old_snapshot TEXT    NOT NULL,
    note         TEXT
);

CREATE TABLE IF NOT EXISTS queries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    component_key TEXT    NOT NULL,
    source        TEXT    NOT NULL CHECK(source IN ('raid','hive')),
    label         TEXT    NOT NULL,
    query_sql     TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL,
    updated_by    TEXT    NOT NULL DEFAULT 'system',
    UNIQUE(component_key, source)
);

CREATE TABLE IF NOT EXISTS query_audit (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id    INTEGER NOT NULL REFERENCES queries(id),
    changed_by  TEXT    NOT NULL,
    changed_at  TEXT    NOT NULL,
    old_sql     TEXT    NOT NULL,
    note        TEXT
);

CREATE INDEX IF NOT EXISTS idx_q_audit_qid  ON query_audit(query_id);
CREATE INDEX IF NOT EXISTS idx_ca_conn_id   ON connection_audit(conn_id);

CREATE TABLE IF NOT EXISTS run_cache (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at        TEXT    NOT NULL,
    run_by        TEXT    NOT NULL DEFAULT 'anonymous',
    start_date    TEXT    NOT NULL,
    end_date      TEXT    NOT NULL,
    source        TEXT    NOT NULL,
    component_key TEXT    NOT NULL,
    result_json   TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rc_lookup
    ON run_cache(start_date, end_date, source, component_key, run_at);
"""


# ── Bootstrap ──────────────────────────────────────────────────────────────

def bootstrap(db_path: Path):
    global _DB_PATH
    _DB_PATH = db_path
    with get_conn() as c:
        c.executescript(_SCHEMA)
    _seed_connections()
    _seed_queries()
    _seed_default_admin()


# ── Admin users ────────────────────────────────────────────────────────────

def _hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def verify_admin(username: str, password: str) -> bool:
    with get_conn() as c:
        row = c.execute(
            "SELECT password_hash FROM admin_users WHERE username=?", (username,)
        ).fetchone()
    return row is not None and row["password_hash"] == _hash(password)


def create_admin(username: str, password: str):
    with get_conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO admin_users (username,password_hash,created_at) VALUES (?,?,?)",
            (username, _hash(password), _now())
        )


def list_admins() -> list[dict]:
    with get_conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT id,username,created_at FROM admin_users ORDER BY username"
        ).fetchall()]


def delete_admin(username: str):
    with get_conn() as c:
        c.execute("DELETE FROM admin_users WHERE username=?", (username,))


def _seed_default_admin():
    with get_conn() as c:
        count = c.execute("SELECT COUNT(*) FROM admin_users").fetchone()[0]
    if count == 0:
        create_admin("admin", "admin123")
        print("[portal.db] Default admin created -- admin / admin123  << CHANGE THIS")


# ── Connections ────────────────────────────────────────────────────────────

def get_connection(conn_key: str) -> dict | None:
    with get_conn() as c:
        row = c.execute(
            "SELECT * FROM connections WHERE conn_key=? AND is_active=1", (conn_key,)
        ).fetchone()
    return dict(row) if row else None


def all_connections() -> list[dict]:
    with get_conn() as c:
        rows = c.execute(
            "SELECT * FROM connections ORDER BY db_type, conn_key"
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["password_masked"] = "●" * min(len(d.get("password", "")), 8)
        result.append(d)
    return result


def upsert_connection(conn_key, label, db_type, host, port,
                      database, username, password, extra_params, by) -> int:
    now = _now()
    with get_conn() as c:
        existing = c.execute(
            "SELECT * FROM connections WHERE conn_key=?", (conn_key,)
        ).fetchone()
        if existing:
            snapshot = dict(existing)
            snapshot["password"] = "***REDACTED***"
            c.execute(
                "INSERT INTO connection_audit (conn_id,changed_by,changed_at,old_snapshot,note) VALUES (?,?,?,?,?)",
                (existing["id"], by, now, json.dumps(snapshot), "updated")
            )
            c.execute(
                """UPDATE connections SET label=?,db_type=?,host=?,port=?,database=?,
                   username=?,password=?,extra_params=?,updated_at=?,updated_by=?
                   WHERE conn_key=?""",
                (label, db_type, host, port, database, username, password,
                 json.dumps(extra_params), now, by, conn_key)
            )
            return existing["id"]
        else:
            cur = c.execute(
                """INSERT INTO connections
                   (conn_key,label,db_type,host,port,database,username,password,
                    extra_params,is_active,updated_at,updated_by)
                   VALUES (?,?,?,?,?,?,?,?,?,1,?,?)""",
                (conn_key, label, db_type, host, port, database, username,
                 password, json.dumps(extra_params), now, by)
            )
            return cur.lastrowid


def test_connection(conn_key: str) -> tuple[bool, str]:
    row = get_connection(conn_key)
    if not row:
        return False, "Connection not found"
    try:
        if row["db_type"] == "oracle":
            import oracledb
            dsn = oracledb.makedsn(row["host"], row["port"], service_name=row["database"])
            oracledb.connect(user=row["username"], password=row["password"], dsn=dsn).close()
        else:
            from pyhive import hive
            hive.Connection(host=row["host"], port=row["port"],
                            username=row["username"], database=row["database"]).close()
        return True, "Connection successful"
    except Exception as exc:
        return False, str(exc)


def get_connection_audit(conn_id: int, limit: int = 50) -> list[dict]:
    with get_conn() as c:
        return [dict(r) for r in c.execute(
            """SELECT ca.*, c.conn_key, c.label FROM connection_audit ca
               JOIN connections c ON ca.conn_id=c.id
               WHERE ca.conn_id=? ORDER BY ca.changed_at DESC LIMIT ?""",
            (conn_id, limit)
        ).fetchall()]


# ── Queries ────────────────────────────────────────────────────────────────

def get_query(component_key: str, source: str) -> str | None:
    with get_conn() as c:
        row = c.execute(
            "SELECT query_sql FROM queries WHERE component_key=? AND source=?",
            (component_key, source)
        ).fetchone()
    return row["query_sql"] if row else None


def get_query_row(query_id: int) -> dict | None:
    with get_conn() as c:
        row = c.execute("SELECT * FROM queries WHERE id=?", (query_id,)).fetchone()
    return dict(row) if row else None


def all_queries() -> list[dict]:
    with get_conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM queries ORDER BY component_key, source"
        ).fetchall()]


def update_query(query_id: int, new_sql: str, by: str, note: str = "manual edit"):
    now = _now()
    with get_conn() as c:
        row = c.execute("SELECT * FROM queries WHERE id=?", (query_id,)).fetchone()
        if not row:
            raise ValueError(f"Query id={query_id} not found")
        c.execute(
            "INSERT INTO query_audit (query_id,changed_by,changed_at,old_sql,note) VALUES (?,?,?,?,?)",
            (query_id, by, now, row["query_sql"], note)
        )
        c.execute(
            "UPDATE queries SET query_sql=?,updated_at=?,updated_by=? WHERE id=?",
            (new_sql, now, by, query_id)
        )


def restore_query(query_id: int, audit_id: int, by: str):
    now = _now()
    with get_conn() as c:
        audit_row = c.execute(
            "SELECT * FROM query_audit WHERE id=? AND query_id=?", (audit_id, query_id)
        ).fetchone()
        current = c.execute("SELECT * FROM queries WHERE id=?", (query_id,)).fetchone()
        if not audit_row or not current:
            raise ValueError("Audit or query row not found")
        c.execute(
            "INSERT INTO query_audit (query_id,changed_by,changed_at,old_sql,note) VALUES (?,?,?,?,?)",
            (query_id, by, now, current["query_sql"], f"restored from audit #{audit_id}")
        )
        c.execute(
            "UPDATE queries SET query_sql=?,updated_at=?,updated_by=? WHERE id=?",
            (audit_row["old_sql"], now, by, query_id)
        )


def get_query_audit(query_id: int, limit: int = 100) -> list[dict]:
    with get_conn() as c:
        return [dict(r) for r in c.execute(
            """SELECT qa.*, q.component_key, q.source, q.label
               FROM query_audit qa JOIN queries q ON qa.query_id=q.id
               WHERE qa.query_id=? ORDER BY qa.changed_at DESC LIMIT ?""",
            (query_id, limit)
        ).fetchall()]


def get_full_query_audit(limit: int = 500) -> list[dict]:
    with get_conn() as c:
        return [dict(r) for r in c.execute(
            """SELECT qa.id, qa.changed_by, qa.changed_at, qa.note, qa.query_id,
                      q.component_key, q.source, q.label
               FROM query_audit qa JOIN queries q ON qa.query_id=q.id
               ORDER BY qa.changed_at DESC LIMIT ?""",
            (limit,)
        ).fetchall()]


# ── Seed data ──────────────────────────────────────────────────────────────

def _seed_connections():
    seeds = [
        ("raid", "RAID (Oracle)", "oracle", "10.50.148.15", 1521,
         "RAID", "RA_ALERT_USER1", os.getenv("RAID_PASSWORD", "Hash#ra123"), {}),
        ("hive", "Hive (raid_jazz)", "hive", "10.50.142.230", 10000,
         "raid_jazz", "arham.ali", "", {}),
    ]
    for conn_key, label, db_type, host, port, database, username, password, extra in seeds:
        with get_conn() as c:
            exists = c.execute(
                "SELECT 1 FROM connections WHERE conn_key=?", (conn_key,)
            ).fetchone()
        if not exists:
            upsert_connection(conn_key, label, db_type, host, port,
                              database, username, password, extra, by="system")


def _seed_queries():
    seeds = [
        # ── Traffic VAS ────────────────────────────────────────────────────
        ("traffic_vas", "raid", "Traffic VAS — RAID", """\
SELECT /*+ PARALLEL*/ start_date, traffic_type,
       sum(event_count)                      AS total_events,
       round(sum(charge_amount) / 1.195, 0)  AS total_amount
FROM   RDMBKUCDAT.RAID_T_PRE_OUT_CDR_AGG
WHERE  start_date BETWEEN :start_date AND :end_date
  AND  lower(called_no_grp) LIKE 'vas_%'
  AND  subscriber_type = 'PRE'
GROUP BY start_date, traffic_type
ORDER BY 1"""),

        ("traffic_vas", "hive", "Traffic VAS — Hive", """\
SELECT start_date, traffic_type,
       sum(event_count)                     AS total_events,
       round(sum(charge_amount) / 1.195, 0) AS total_amount
FROM   RAID_JAZZ.PRE_OUT_CDR
WHERE  start_date BETWEEN '{start_date}' AND '{end_date}'
  AND  lower(called_no_grp) LIKE 'vas_%'
  AND  service_class NOT IN (
         '52','62','65','70','72','73','88','90',
         '101','102','103','106','107','108')
GROUP BY start_date, traffic_type
ORDER BY start_date, traffic_type"""),

        # ── Traffic OTHER ──────────────────────────────────────────────────
        ("traffic_other", "raid", "Traffic OTHER — RAID", """\
SELECT /*+ PARALLEL*/ start_date, traffic_type,
       sum(event_count)                      AS total_events,
       round(sum(charge_amount) / 1.195, 0)  AS total_amount
FROM   RDMBKUCDAT.RAID_T_PRE_OUT_CDR_AGG
WHERE  start_date BETWEEN :start_date AND :end_date
  AND  lower(called_no_grp) NOT LIKE 'vas_%'
  AND  subscriber_type = 'PRE'
  AND  traffic_type IN ('VOICE', 'SMS')
GROUP BY start_date, traffic_type
ORDER BY 1"""),

        ("traffic_other", "hive", "Traffic OTHER — Hive", """\
SELECT start_date, traffic_type,
       sum(event_count)                     AS total_events,
       round(sum(charge_amount) / 1.195, 0) AS total_amount
FROM   RAID_JAZZ.PRE_OUT_CDR
WHERE  start_date BETWEEN '{start_date}' AND '{end_date}'
  AND  lower(called_no_grp) NOT LIKE 'vas_%'
  AND  service_class NOT IN (
         '52','62','65','70','72','73','88','90',
         '101','102','103','106','107','108')
GROUP BY start_date, traffic_type
ORDER BY start_date, traffic_type"""),

        # ── GPRS ───────────────────────────────────────────────────────────
        ("gprs", "raid", "GPRS — RAID", """\
SELECT /*+ PARALLEL*/ start_date,
       sum(event_count)                      AS total_events,
       round(sum(charge_amount) / 1.195, 0)  AS total_amount
FROM   RDMBKUCDAT.RAID_T_PRE_OUT_CDR_AGG
WHERE  start_date BETWEEN :start_date AND :end_date
  AND  subscriber_type = 'PRE'
  AND  traffic_type = 'DATA'
GROUP BY start_date
ORDER BY 1"""),

        ("gprs", "hive", "GPRS — Hive", """\
SELECT f_start_date                      AS start_date,
       sum(f_event_count)                AS total_events,
       sum(f_amount / 1.195)             AS total_amount
FROM   fms_jazz.dtl_network_usage
WHERE  f_control_point = 'CCN_GPRS'
  AND  f_start_date BETWEEN '{start_date}' AND '{end_date}'
  AND  f_destination NOT IN (
         '52','62','65','70','72','73','88','90',
         '101','102','103','106','107','108')
GROUP BY f_start_date
ORDER BY f_start_date"""),

        # ── Non-Usage: Subscription Bundles ────────────────────────────────
        ("nonusage_sub_bundles", "raid", "Non-Usage Sub Bundles — RAID", """\
SELECT /*+ PARALLEL*/ start_date,
       sum(event_count)                      AS total_events,
       round(sum(charge_amount) / 1.195, 0)  AS total_amount
FROM   RDMBKUCDAT.raid_t_pre_out_daily
WHERE  start_date BETWEEN :start_date AND :end_date
  AND  subscriber_type = 'PRE'
  AND  event_type = 'timeBasedActions'
GROUP BY start_date
ORDER BY 1"""),

        ("nonusage_sub_bundles", "hive", "Non-Usage Sub Bundles — Hive", """\
SELECT start_date,
       COUNT(*)                              AS total_events,
       abs(round(sum(charge_amount) / 1.195, 0)) AS total_amount
FROM (
    SELECT *
    FROM   raid_jazz.pre_out_sdp_erc_cdr_prd
    WHERE  event_type = 'timeBasedActions'
      AND  subscriber_type = 'PRE'
      AND  start_date BETWEEN '{start_date_raw}' AND '{end_date_raw}'
) A
GROUP BY start_date
ORDER BY start_date"""),

        # ── Non-Usage: VAS RBT AIR ─────────────────────────────────────────
        ("nonusage_vas_rbt_air", "raid", "Non-Usage VAS RBT AIR — RAID", """\
SELECT /*+ PARALLEL*/ start_date,
       sum(event_count)                          AS total_events,
       abs(round(sum(reload_amount) / 1.195, 0)) AS total_amount
FROM   RDMBKUCDAT.raid_t_pre_out_cft
WHERE  start_date BETWEEN :start_date AND :end_date
  AND  SUBSCRIBER_TYPE = 'PRE'
  AND  event_type = 'VAS'
  AND  service_used LIKE '%RBT%'
GROUP BY start_date
ORDER BY 1"""),

        # ── Non-Usage: VAS AIR ─────────────────────────────────────────────
        ("nonusage_vas_air", "raid", "Non-Usage VAS AIR — RAID", """\
SELECT /*+ PARALLEL*/ start_date,
       sum(event_count)                          AS total_events,
       abs(round(sum(reload_amount) / 1.195, 0)) AS total_amount
FROM   RDMBKUCDAT.raid_t_pre_out_cft
WHERE  start_date BETWEEN :start_date AND :end_date
  AND  SUBSCRIBER_TYPE = 'PRE'
  AND  event_type = 'VAS'
  AND  service_used NOT LIKE '%RBT%'
  AND  SERVICE_USED NOT IN ('HBSIN_VAS', 'HBSOUT_VAS')
GROUP BY start_date
ORDER BY 1"""),

        # ── Non-Usage: SDP Other ───────────────────────────────────────────
        ("nonusage_sdp_other", "raid", "Non-Usage SDP Other — RAID", """\
SELECT /*+ PARALLEL*/ start_date,
       sum(event_count)                      AS total_events,
       round(sum(charge_amount) / 1.195, 0)  AS total_amount
FROM   RDMBKUCDAT.raid_t_pre_out_daily a
WHERE  start_date BETWEEN :start_date AND :end_date
  AND  subscriber_type = 'PRE'
  AND  service_used_sub_cat NOT IN ('|normal|NOT-OK')
  AND  service_used IN (
         'AccountAdjustment - Discard:',
         'FAT_Expiry_Confiscation',
         'Life Cycle Change - Discard|FATExp:subscriberDeleted',
         'Service Fee Deduction',
         'USSD Mobilink Helpline:')
GROUP BY start_date
ORDER BY 1"""),

        # ── Non-Usage: AIR Other ───────────────────────────────────────────
        ("nonusage_air_other", "raid", "Non-Usage AIR Other — RAID", """\
SELECT /*+ PARALLEL(b,20)*/ start_date,
       sum(event_count)                           AS total_events,
       round(sum(abs(reload_amount)) / 1.195, 0)  AS total_amount
FROM   RDMBKUCDAT.raid_t_pre_out_cft b
WHERE  start_date BETWEEN :start_date AND :end_date
  AND  subscriber_type = 'PRE'
  AND  event_type IN ('USSDC', 'HelplineCharges')
  AND  recharge_type <> 'BUNDLEUPSELL_CASH'
  AND  SERVICE_USED <> 'USSDC'
GROUP BY start_date
ORDER BY 1"""),

        # ── Non-Usage: VAS RBT (Hive only) ────────────────────────────────
        ("nonusage_vas_rbt", "hive", "Non-Usage VAS RBT — Hive", r"""\
SELECT start_date,
       SUM(cdr_count)              AS total_events,
       SUM(charge_amount) / 1.195  AS total_amount
FROM (
    SELECT start_date,
           COUNT(event_type) AS cdr_count,
           SUM(
               CAST(split(exploded_column, '\\|')[1] AS DOUBLE) /
               POWER(10, CAST(split(exploded_column, '\\|')[2] AS INT))
           ) AS charge_amount
    FROM   raid_jazz.pre_out_sdp_erc_cdr_prd
    LATERAL VIEW explode(split(raid_description, '\\|\\|')) exploded AS exploded_column
    WHERE  start_date BETWEEN '{start_date_raw}' AND '{end_date_raw}'
      AND  event_type = 'periodicAccountMgmt'
      AND  service_class NOT IN (
             '52','62','65','70','72','73','88','90',
             '101','102','103','106','107','108')
      AND  LOWER(exploded_column) LIKE '%amount_%'
      AND  LOWER(exploded_column) LIKE '%rbt%'
    GROUP BY start_date, exploded_column
) b
WHERE charge_amount != 0
GROUP BY start_date
ORDER BY start_date"""),

        # ── Non-Usage: VAS VIC (Hive only) ────────────────────────────────
        ("nonusage_vas_vic", "hive", "Non-Usage VAS VIC — Hive", r"""\
SELECT
    start_date,
    SUM(cdr_count)              AS total_events,
    SUM(charge_amount) / 1.195  AS total_amount
FROM (
    SELECT
        start_date,
        COUNT(event_type) AS cdr_count,
        SUM(
            CAST(split(exploded_column, '\\|')[1] AS DOUBLE) /
            POWER(10, CAST(split(exploded_column, '\\|')[2] AS INT))
        ) AS charge_amount
    FROM   raid_jazz.pre_out_sdp_erc_cdr_prd
    LATERAL VIEW explode(split(raid_description, '\\|\\|')) exploded AS exploded_column
    WHERE  start_date BETWEEN '{start_date_raw}' AND '{end_date_raw}'
      AND  event_type = 'periodicAccountMgmt'
      AND  service_class NOT IN (
             '52','62','65','70','72','73','88','90',
             '101','102','103','106','107','108')
      AND  LOWER(exploded_column) LIKE '%amount_%'
      AND  LOWER(exploded_column) NOT LIKE '%rbt%'
      AND  LOWER(exploded_column) NOT LIKE '%tax%'
    GROUP BY start_date, exploded_column
) b
WHERE  charge_amount > 0
GROUP BY start_date
ORDER BY start_date"""),

        # ── Jazz Share ─────────────────────────────────────────────────────
        ("jazz_share", "raid", "Jazz Share — RAID", """\
SELECT /*+ PARALLEL*/ start_date,
       count(EVENT_COUNT)                          AS total_events,
       round((sum(EVENT_COUNT) * 8.60) / 1.195, 0) AS total_amount
FROM   RDMBKUCDAT.raid_t_pre_out_cft_agg
WHERE  start_date BETWEEN :start_date AND :end_date
  AND  SUBSCRIBER_TYPE = 'PRE'
  AND  event_type = 'M2M_Recharge'
GROUP BY Start_Date
ORDER BY 1"""),

        # ── Jazz Adv. Service Fee ──────────────────────────────────────────
        ("jazz_adv_fee", "raid", "Jazz Adv. Service Fee — RAID", """\
SELECT START_DATE,
       sum(t_event)  AS total_events,
       SUM(T_FEE)    AS total_amount
FROM (
    SELECT /*+ PARALLEL*/ START_DATE, CALLED_NO_GRP,
           abs(round(
               SUBSTR(Replace(called_no_grp,'JA_Payback_',''), 1,
                      INSTR(Replace(called_no_grp,'JA_Payback_',''),'|') - 1)
               / 1.195, 2)) AS fee_wot,
           sum(event_count) AS t_event,
           abs(round(
               SUBSTR(Replace(called_no_grp,'JA_Payback_',''), 1,
                      INSTR(Replace(called_no_grp,'JA_Payback_',''),'|') - 1)
               / 1.195, 2) * sum(event_count)) AS t_fee
    FROM   rdmbkucdat.raid_t_pre_out_cft t
    WHERE  start_date BETWEEN :start_date AND :end_date
      AND  called_no_grp LIKE 'JA_Payback_%'
      AND  called_no_grp LIKE '%|%'
    GROUP BY START_DATE, CALLED_NO_GRP
)
GROUP BY START_DATE
ORDER BY START_DATE"""),

        ("jazz_adv_fee", "hive", "Jazz Adv. Service Fee — Hive", """\
SELECT f_start_date           AS start_date,
       sum(payback_count)     AS total_events,
       sum(ja_fee)            AS total_amount
FROM (
    SELECT f_start_date,
           sum(CASE WHEN cast(split(F_sg_risk,'[|]')[DAIDpos] AS FLOAT) < 0
                    THEN 1 ELSE 0 END) AS payback_count,
           round(abs(sum(
               CASE WHEN cast(split(F_sg_risk,'[|]')[DAIDpos] AS FLOAT) < 0
                    THEN cast(split(F_sg_risk,'[|]')[DAIDpos] AS FLOAT)
                    ELSE 0 END
           )) / 1.195, 0) AS ja_fee
    FROM   fms_jazz.dtl_in
    LATERAL VIEW posexplode(split(F_CO_ACTIVATION_DATE,'[|]')) datab AS DAIDpos, DAID
    WHERE  f_control_point = 'IN_AIR'
      AND  f_start_date BETWEEN '{start_date}' AND '{end_date}'
      AND  f_co_activation_date != '_N'
      AND  f_dedicated_account_type NOT IN (
             '62','88','90','52','70','65','72','73',
             '101','102','103','106','107','108')
      AND  split(F_CO_ACTIVATION_DATE,'[|]')[DAIDpos] = '17'
    GROUP BY f_start_date
) a
GROUP BY f_start_date
ORDER BY f_start_date"""),
    ]

    with get_conn() as c:
        for comp_key, source, label, sql in seeds:
            exists = c.execute(
                "SELECT 1 FROM queries WHERE component_key=? AND source=?",
                (comp_key, source)
            ).fetchone()
            if not exists:
                c.execute(
                    "INSERT INTO queries (component_key,source,label,query_sql,updated_at,updated_by) VALUES (?,?,?,?,?,?)",
                    (comp_key, source, label, sql.strip(), _now(), "system")
                )


# ── Run cache ──────────────────────────────────────────────────────────────

def save_run_results(run_at: str, run_by: str,
                     start_date: str, end_date: str,
                     source: str, results: dict):
    """Persist all component results from a completed job to run_cache."""
    with get_conn() as c:
        for comp_key, result in results.items():
            # Strip internal per-row timing columns before storing
            cleaned = {}
            for k, v in result.items():
                if k in ("raid_rows", "hive_rows", "comparison_rows") and isinstance(v, list):
                    cleaned[k] = [{col: val for col, val in row.items()
                                   if not col.startswith("_")}
                                  for row in v]
                else:
                    cleaned[k] = v
            c.execute(
                """INSERT INTO run_cache
                   (run_at, run_by, start_date, end_date, source, component_key, result_json)
                   VALUES (?,?,?,?,?,?,?)""",
                (run_at, run_by, start_date, end_date, source,
                 comp_key, json.dumps(cleaned))
            )


def get_last_run(start_date: str, end_date: str, source: str) -> dict | None:
    """Return {run_at, run_by, results: {comp_key: result}} for the latest matching run."""
    with get_conn() as c:
        meta = c.execute(
            """SELECT run_at, run_by FROM run_cache
               WHERE start_date=? AND end_date=? AND source=?
               ORDER BY run_at DESC LIMIT 1""",
            (start_date, end_date, source)
        ).fetchone()
        if not meta:
            return None
        run_at, run_by = meta["run_at"], meta["run_by"]
        rows = c.execute(
            """SELECT component_key, result_json FROM run_cache
               WHERE start_date=? AND end_date=? AND source=? AND run_at=?""",
            (start_date, end_date, source, run_at)
        ).fetchall()
    return {
        "run_at":  run_at,
        "run_by":  run_by,
        "results": {r["component_key"]: json.loads(r["result_json"]) for r in rows},
    }


def list_recent_runs(limit: int = 15) -> list[dict]:
    """List the most recent distinct run sessions (one row per job)."""
    with get_conn() as c:
        rows = c.execute(
            """SELECT run_at, run_by, start_date, end_date, source,
                      COUNT(*) AS n_components
               FROM run_cache
               GROUP BY run_at, run_by, start_date, end_date, source
               ORDER BY run_at DESC LIMIT ?""",
            (limit,)
        ).fetchall()
    return [dict(r) for r in rows]
