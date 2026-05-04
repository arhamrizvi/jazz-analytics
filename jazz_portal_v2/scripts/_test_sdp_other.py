"""Test SDP Other Oracle query with literal date rendering (no bind params)."""
import sys, time
sys.path.insert(0, ".")
from pathlib import Path
from portal import db as _db
_db._DB_PATH = Path("portal.db").resolve()
from portal import connections as cm
import sqlite3, pandas as pd

SD = "20260428"

db_conn = sqlite3.connect("portal.db")
sql = db_conn.execute(
    "SELECT query_sql FROM queries WHERE id=11"
).fetchone()[0]
db_conn.close()

rendered = sql.replace(":start_date", f"'{SD}'").replace(":end_date", f"'{SD}'")
print("Rendered SQL:")
print(rendered)
print("\nRunning...")
try:
    conn = cm.get_oracle()
    t0 = time.time()
    df = pd.read_sql(rendered, conn)
    conn.close()
    elapsed = round(time.time() - t0, 1)
    print(f"OK — {len(df)} rows in {elapsed}s")
    if len(df): print(df.to_string(index=False))
    else: print("EMPTY")
except Exception as e:
    print(f"ERROR: {e}")

print("\nDone.")
