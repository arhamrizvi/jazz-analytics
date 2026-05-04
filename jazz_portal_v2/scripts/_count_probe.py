"""Fast COUNT probes — no complex LATERAL VIEW, just raw row counts to check data availability."""
import sys, time
sys.path.insert(0, ".")
from pathlib import Path
from portal import db as _db
_db._DB_PATH = Path("portal.db").resolve()
from portal import connections as cm
import pandas as pd

SD_RAW  = "20260428"   # YYYYMMDD
SD_DASH = "2026-04-28" # YYYY-MM-DD

def hive_count(label, sql):
    print(f"\n[HIVE] {label}")
    print(f"  SQL: {sql}")
    try:
        conn = cm.get_hive()
        t0 = time.time()
        df = pd.read_sql(sql, conn)
        conn.close()
        print(f"  RESULT: {df.iloc[0,0]} rows  ({round(time.time()-t0,1)}s)")
    except Exception as e:
        print(f"  ERROR: {e}")

def oracle_count(label, sql, params):
    print(f"\n[ORACLE] {label}")
    print(f"  SQL: {sql}  params={params}")
    try:
        conn = cm.get_oracle()
        t0 = time.time()
        df = pd.read_sql(sql, conn, params=params)
        conn.close()
        print(f"  RESULT: {df.iloc[0,0]} rows  ({round(time.time()-t0,1)}s)")
    except Exception as e:
        print(f"  ERROR: {e}")


# 1. Raw count in pre_out_sdp_erc_cdr_prd for that date (both formats)
hive_count("pre_out_sdp_erc_cdr_prd total rows for 20260428 (raw)",
    f"SELECT COUNT(*) AS n FROM raid_jazz.pre_out_sdp_erc_cdr_prd WHERE start_date = '{SD_RAW}'")

hive_count("pre_out_sdp_erc_cdr_prd total rows for 2026-04-28 (dash)",
    f"SELECT COUNT(*) AS n FROM raid_jazz.pre_out_sdp_erc_cdr_prd WHERE start_date = '{SD_DASH}'")

# 2. Count by event_type for that date (find out what actually exists)
hive_count("pre_out_sdp_erc_cdr_prd event_type breakdown (raw date)",
    f"SELECT event_type, COUNT(*) AS n FROM raid_jazz.pre_out_sdp_erc_cdr_prd WHERE start_date = '{SD_RAW}' GROUP BY event_type ORDER BY n DESC")

# 3. Check if periodicAccountMgmt exists
hive_count("periodicAccountMgmt count (raw date)",
    f"SELECT COUNT(*) AS n FROM raid_jazz.pre_out_sdp_erc_cdr_prd WHERE start_date = '{SD_RAW}' AND event_type = 'periodicAccountMgmt'")

# 4. Check timeBasedActions
hive_count("timeBasedActions count (raw date)",
    f"SELECT COUNT(*) AS n FROM raid_jazz.pre_out_sdp_erc_cdr_prd WHERE start_date = '{SD_RAW}' AND event_type = 'timeBasedActions' AND subscriber_type = 'PRE'")

# 5. Oracle: raid_t_pre_out_daily total for that date
oracle_count("raid_t_pre_out_daily total rows",
    "SELECT COUNT(*) AS n FROM RDMBKUCDAT.raid_t_pre_out_daily WHERE start_date = :d",
    {"d": SD_RAW})

# 6. Oracle: SDP Other specific service_used values
oracle_count("raid_t_pre_out_daily SDP Other service_used filter",
    """SELECT COUNT(*) AS n FROM RDMBKUCDAT.raid_t_pre_out_daily
       WHERE start_date = :d AND subscriber_type = 'PRE'
       AND service_used IN (
         'AccountAdjustment - Discard:','FAT_Expiry_Confiscation',
         'Life Cycle Change - Discard|FATExp:subscriberDeleted',
         'Service Fee Deduction','USSD Mobilink Helpline:')""",
    {"d": SD_RAW})

# 7. Oracle: Sub Bundles timeBasedActions
oracle_count("raid_t_pre_out_daily timeBasedActions",
    "SELECT COUNT(*) AS n FROM RDMBKUCDAT.raid_t_pre_out_daily WHERE start_date = :d AND subscriber_type = 'PRE' AND event_type = 'timeBasedActions'",
    {"d": SD_RAW})

print("\nDone.")
