"""Test fixed VAS RBT and VAS VIC queries directly against Hive."""
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
    print(f"SQL preview: {sql[:120].strip()}...")
    print("Running...")
    try:
        conn = cm.get_hive()
        t0 = time.time()
        df = pd.read_sql(sql, conn)
        conn.close()
        elapsed = round(time.time() - t0, 1)
        print(f"OK — {len(df)} rows in {elapsed}s")
        if len(df):
            print(df.to_string(index=False))
        else:
            print("EMPTY RESULT")
    except Exception as e:
        print(f"ERROR: {e}")

# Read fresh queries from portal.db
import sqlite3
db_conn = sqlite3.connect("portal.db")
rbt_sql = db_conn.execute(
    "SELECT query_sql FROM queries WHERE id=13"
).fetchone()[0]
vic_sql = db_conn.execute(
    "SELECT query_sql FROM queries WHERE id=14"
).fetchone()[0]
db_conn.close()

rbt_rendered = rbt_sql.replace("{start_date_raw}", SD).replace("{end_date_raw}", SD)
vic_rendered = vic_sql.replace("{start_date_raw}", SD).replace("{end_date_raw}", SD)

hive("VAS RBT (id=13)", rbt_rendered)
hive("VAS VIC (id=14)", vic_rendered)

print("\nDone.")
