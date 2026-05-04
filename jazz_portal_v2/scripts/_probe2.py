"""Probe 2: inspect raid_description structure for periodicAccountMgmt rows."""
import sys, time
sys.path.insert(0, ".")
from pathlib import Path
from portal import db as _db
_db._DB_PATH = Path("portal.db").resolve()
from portal import connections as cm
import pandas as pd

SD = "20260428"

def hive(label, sql):
    print(f"\n[HIVE] {label}")
    try:
        conn = cm.get_hive()
        t0 = time.time()
        df = pd.read_sql(sql, conn)
        conn.close()
        print(f"  ({round(time.time()-t0,1)}s)  {len(df)} rows")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"  ERROR: {e}")

# 1. Sample raid_description values for periodicAccountMgmt
hive("Sample raid_description (periodicAccountMgmt, 5 rows)",
    f"""SELECT raid_description
        FROM raid_jazz.pre_out_sdp_erc_cdr_prd
        WHERE start_date = '{SD}' AND event_type = 'periodicAccountMgmt'
        LIMIT 5""")

# 2. After explode — what do the exploded values look like?
hive("Exploded column samples (first 10)",
    f"""SELECT exploded_column
        FROM raid_jazz.pre_out_sdp_erc_cdr_prd
        LATERAL VIEW explode(split(raid_description, '\\\\|\\\\|')) e AS exploded_column
        WHERE start_date = '{SD}' AND event_type = 'periodicAccountMgmt'
        LIMIT 10""")

# 3. Does anything match %amount_%?
hive("Count matching LIKE '%amount_%' after explode",
    f"""SELECT COUNT(*) AS n
        FROM raid_jazz.pre_out_sdp_erc_cdr_prd
        LATERAL VIEW explode(split(raid_description, '\\\\|\\\\|')) e AS exploded_column
        WHERE start_date = '{SD}' AND event_type = 'periodicAccountMgmt'
          AND LOWER(exploded_column) LIKE '%amount_%'
        LIMIT 1""")

# 4. Distinct service_class values present
hive("Distinct service_class values in periodicAccountMgmt",
    f"""SELECT service_class, COUNT(*) AS n
        FROM raid_jazz.pre_out_sdp_erc_cdr_prd
        WHERE start_date = '{SD}' AND event_type = 'periodicAccountMgmt'
        GROUP BY service_class ORDER BY n DESC LIMIT 20""")

print("\nDone.")
