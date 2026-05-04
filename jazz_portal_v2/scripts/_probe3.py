"""Probe 3: see actual exploded column values — what's in raid_description?"""
import sys, time
sys.path.insert(0, ".")
from pathlib import Path
from portal import db as _db
_db._DB_PATH = Path("portal.db").resolve()
from portal import connections as cm
import pandas as pd

SD = "20260428"

def hive(label, sql):
    print(f"\n{'='*60}")
    print(f"[{label}]")
    try:
        conn = cm.get_hive()
        t0 = time.time()
        df = pd.read_sql(sql, conn)
        conn.close()
        print(f"  OK — {len(df)} rows in {round(time.time()-t0,1)}s")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"  ERROR: {e}")

# 1. Distinct exploded_column values that contain 'amount' (no rbt filter)
hive("Distinct AMOUNT-like exploded values (top 30)",
    f"""SELECT LOWER(exploded_column) AS col_val, COUNT(*) AS n
        FROM raid_jazz.pre_out_sdp_erc_cdr_prd
        LATERAL VIEW explode(split(raid_description, '\\|\\|')) exploded AS exploded_column
        WHERE start_date = '{SD}' AND event_type = 'periodicAccountMgmt'
          AND LOWER(exploded_column) LIKE '%amount%'
        GROUP BY LOWER(exploded_column)
        ORDER BY n DESC
        LIMIT 30""")

# 2. Sample 10 raw exploded values (no filter) to see the format
hive("Sample 10 raw exploded values (any)",
    f"""SELECT exploded_column
        FROM raid_jazz.pre_out_sdp_erc_cdr_prd
        LATERAL VIEW explode(split(raid_description, '\\|\\|')) exploded AS exploded_column
        WHERE start_date = '{SD}' AND event_type = 'periodicAccountMgmt'
        LIMIT 10""")

print("\nDone.")
