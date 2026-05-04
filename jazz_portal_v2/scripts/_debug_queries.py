"""
Debug script: probe the 4 zero-returning components.
For each: render the SQL, wrap in COUNT(*), run it, print row count or error.
Run with: python _debug_queries.py
"""
import sys, sqlite3, time
sys.path.insert(0, ".")

DB = "portal.db"

# bootstrap db so _DB_PATH is set
from pathlib import Path
from portal import db as _portal_db
_portal_db._DB_PATH = Path(DB).resolve()

def get_sql(comp_key, source):
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT query_sql FROM queries WHERE component_key=? AND source=?",
        (comp_key, source)
    ).fetchone()
    conn.close()
    return row["query_sql"] if row else None

def to_hive_date(d):
    d = d.replace("-", "")
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d

def render_hive(sql, sd, ed):
    raw_sd = sd.replace("-", "")
    raw_ed = ed.replace("-", "")
    hive_sd = to_hive_date(sd)
    hive_ed = to_hive_date(ed)
    return (sql
            .replace("{start_date_raw}", raw_sd)
            .replace("{end_date_raw}",   raw_ed)
            .replace("{start_date}",     hive_sd)
            .replace("{end_date}",       hive_ed))

def probe_hive(label, comp_key, sd, ed):
    sql = get_sql(comp_key, "hive")
    if not sql:
        print(f"[{label}] NO HIVE QUERY FOUND"); return
    rendered = render_hive(sql, sd, ed)
    # First: raw count to check if any rows exist at all (bypasses outer filters)
    # Build a simpler probe: COUNT from the inner table only
    print(f"\n{'='*60}")
    print(f"[{label}] Rendered SQL:")
    print(rendered)
    print(f"\n[{label}] Running...")
    try:
        from portal import db as _db
        from portal import connections as cm
        conn = cm.get_hive()
        import pandas as pd
        t0 = time.time()
        df = pd.read_sql(rendered, conn)
        conn.close()
        elapsed = round(time.time() - t0, 1)
        print(f"[{label}] OK — {len(df)} rows in {elapsed}s")
        if len(df):
            print(df.to_string())
        else:
            print(f"[{label}] EMPTY RESULT — trying raw count on pre_out_sdp_erc_cdr_prd...")
            probe_hive_count(label, sd, ed)
    except Exception as e:
        print(f"[{label}] ERROR: {e}")

def probe_hive_count(label, sd, ed):
    raw_sd = sd.replace("-", "")
    probe_sql = f"SELECT COUNT(*) AS n FROM raid_jazz.pre_out_sdp_erc_cdr_prd WHERE start_date = '{raw_sd}'"
    try:
        from portal import connections as cm
        import pandas as pd
        conn = cm.get_hive()
        df = pd.read_sql(probe_sql, conn)
        conn.close()
        print(f"[{label}] Raw COUNT for {raw_sd}: {df.iloc[0,0]} rows in pre_out_sdp_erc_cdr_prd")
    except Exception as e:
        print(f"[{label}] Count probe ERROR: {e}")

def probe_raid(label, comp_key, sd, ed):
    sql = get_sql(comp_key, "raid")
    if not sql:
        print(f"[{label}] NO RAID QUERY FOUND"); return
    oracle_sd = sd.replace("-", "")
    oracle_ed = ed.replace("-", "")
    print(f"\n{'='*60}")
    print(f"[{label}] Oracle bind params: start={oracle_sd} end={oracle_ed}")
    print(f"[{label}] SQL:\n{sql}")
    print(f"\n[{label}] Running...")
    try:
        from portal import connections as cm
        import pandas as pd
        conn = cm.get_oracle()
        t0 = time.time()
        df = pd.read_sql(sql, conn, params={"start_date": oracle_sd, "end_date": oracle_ed})
        conn.close()
        elapsed = round(time.time() - t0, 1)
        print(f"[{label}] OK — {len(df)} rows in {elapsed}s")
        if len(df):
            print(df.to_string())
        else:
            print(f"[{label}] EMPTY — trying raw count on raid_t_pre_out_daily...")
            probe_oracle_count(label, oracle_sd)
    except Exception as e:
        print(f"[{label}] ERROR: {e}")

def probe_oracle_count(label, oracle_sd):
    probe_sql = "SELECT COUNT(*) AS n FROM RDMBKUCDAT.raid_t_pre_out_daily WHERE start_date = :d"
    try:
        from portal import connections as cm
        import pandas as pd
        conn = cm.get_oracle()
        df = pd.read_sql(probe_sql, conn, params={"d": oracle_sd})
        conn.close()
        print(f"[{label}] Raw COUNT for {oracle_sd}: {df.iloc[0,0]} rows in raid_t_pre_out_daily")
    except Exception as e:
        print(f"[{label}] Count probe ERROR: {e}")


if __name__ == "__main__":
    SD = "20260428"
    ED = "20260428"

    probe_hive("VAS RBT",     "nonusage_vas_rbt",    SD, ED)
    probe_hive("VAS VIC",     "nonusage_vas_vic",    SD, ED)
    probe_hive("Sub Bundles", "nonusage_sub_bundles", SD, ED)
    probe_raid("SDP Other",   "nonusage_sdp_other",  SD, ED)
    probe_raid("Sub Bundles RAID", "nonusage_sub_bundles", SD, ED)

    print("\nDone.")
