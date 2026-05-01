"""
portal/connections.py
=====================
Single source of truth for live database connections.
All parameters are read from SQLite at call-time — no hardcoded credentials.

To change a connection: Admin → Connections → Edit → Save.
Takes effect on the next run. No restart needed.
"""

from portal import db


class PortalConnectionError(Exception):
    pass


def get_oracle(conn_key: str = "raid"):
    row = db.get_connection(conn_key)
    if not row:
        raise PortalConnectionError(
            f"No active connection '{conn_key}'. Configure it under Admin → Connections."
        )
    try:
        import oracledb
        dsn = oracledb.makedsn(row["host"], int(row["port"]), service_name=row["database"])
        c = oracledb.connect(user=row["username"], password=row["password"], dsn=dsn)
        c.callTimeout = 600_000  # 600 s; raises DPY-4011 instead of hanging forever
        c.module = "jazz_portal_rv"
        return c
    except ImportError:
        raise PortalConnectionError("oracledb not installed. Run: pip install oracledb")
    except Exception as exc:
        raise PortalConnectionError(
            f"Oracle '{conn_key}' ({row['host']}:{row['port']}/{row['database']}): {exc}"
        )


def get_hive(conn_key: str = "hive"):
    row = db.get_connection(conn_key)
    if not row:
        raise PortalConnectionError(
            f"No active connection '{conn_key}'. Configure it under Admin → Connections."
        )
    try:
        from pyhive import hive
        conn = hive.Connection(
            host=row["host"], port=int(row["port"]),
            username=row["username"], database=row["database"]
        )
        conn.cursor().execute("SET hive.exec.dynamic.partition.mode=nonstrict")
        return conn
    except ImportError:
        raise PortalConnectionError(
            "pyhive not installed. Run: pip install pyhive[hive] thrift thrift-sasl sasl"
        )
    except Exception as exc:
        raise PortalConnectionError(
            f"Hive '{conn_key}' ({row['host']}:{row['port']}/{row['database']}): {exc}"
        )
