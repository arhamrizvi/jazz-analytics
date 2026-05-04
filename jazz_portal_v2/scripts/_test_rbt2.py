"""Quick test: run VAS RBT query + a no-rbt-filter variant to compare."""
import sys, time
sys.path.insert(0, ".")
from pathlib import Path
from portal import db as _db
_db._DB_PATH = Path("portal.db").resolve()
from portal import connections as cm
import sqlite3, pandas as pd

SD = "20260428"

def hive(label, sql):
    print(f"\n[{label}]")
    try:
        conn = cm.get_hive()
        t0 = time.time()
        df = pd.read_sql(sql, conn)
        conn.close()
        print(f"  {len(df)} rows in {round(time.time()-t0,1)}s")
        if len(df): print(df.to_string(index=False))
        else: print("  EMPTY")
    except Exception as e:
        print(f"  ERROR: {e}")

db_conn = sqlite3.connect("portal.db")
rbt_sql = db_conn.execute("SELECT query_sql FROM queries WHERE id=13").fetchone()[0]
db_conn.close()
rendered = rbt_sql.replace("{start_date_raw}", SD).replace("{end_date_raw}", SD)

hive("VAS RBT (user's query)", rendered)

# Variant: drop the rbt filter entirely — shows what amount_ values exist
no_rbt_sql = rendered.replace(
    "AND LOWER(exploded_column) LIKE '%rbt%'\n", ""
).replace(
    "AND LOWER(exploded_column) LIKE '%rbt%'", ""
)
hive("VAS RBT — no rbt filter (shows all amount_ values)", no_rbt_sql)

print("\nDone.")
