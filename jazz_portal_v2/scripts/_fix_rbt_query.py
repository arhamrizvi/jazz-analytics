"""Fix VAS RBT query in portal.db — must use a file, not -c, to preserve double-backslash."""
import sqlite3

# In a Python source file r-string, \\| stores 2 backslashes + pipe in the string.
# Hive SQL parser: \\| in string literal → \| → Java regex: literal pipe. Correct.
new_sql = r"""SELECT
    start_date,
    SUM(cdr_count) AS total_events,
    SUM(charge_amount) / 1.195 AS total_amount
FROM (
    SELECT
        start_date,
        COUNT(event_type) AS cdr_count,
        SUM(
            CAST(split(exploded_column, '\\|')[1] AS DOUBLE) /
            POWER(10, CAST(split(exploded_column, '\\|')[2] AS INT))
        ) AS charge_amount
    FROM raid_jazz.pre_out_sdp_erc_cdr_prd
    LATERAL VIEW explode(split(raid_description, '\\|\\|')) exploded AS exploded_column
    WHERE start_date BETWEEN '{start_date_raw}' AND '{end_date_raw}'
      AND event_type = 'periodicAccountMgmt'
      AND service_class NOT IN ('52','62','65','70','72','73','88','90','101','102','103','106','107','108')
      AND LOWER(exploded_column) LIKE '%amount_%'
      AND LOWER(exploded_column) LIKE '%rbt%'
    GROUP BY start_date, exploded_column
) b
WHERE charge_amount != 0
GROUP BY start_date
ORDER BY start_date"""

conn = sqlite3.connect("portal.db")
conn.execute("UPDATE queries SET query_sql=? WHERE id=13", (new_sql,))
conn.commit()

# Verify backslash count matches VIC (id=14)
rbt = conn.execute("SELECT query_sql FROM queries WHERE id=13").fetchone()[0]
vic = conn.execute("SELECT query_sql FROM queries WHERE id=14").fetchone()[0]
conn.close()

print(f"RBT backslashes: {rbt.count(chr(92))}  (VIC has: {vic.count(chr(92))})")
for line in rbt.splitlines():
    if "split" in line or "LATERAL" in line:
        print(repr(line))
