"""
portal/connections.py
=====================
Single source of truth for live database connections.
All parameters are read from SQLite at call-time — no hardcoded credentials.

To change a connection: Admin → Connections → Edit → Save.
Takes effect on the next run. No restart needed.
"""

import socket
from portal import db

# TCP probe timeout in seconds — fail fast if server is unreachable
_TCP_TIMEOUT = 8


class PortalConnectionError(Exception):
    pass


def _tcp_probe(host: str, port: int) -> None:
    """Raise PortalConnectionError fast if the host:port is not reachable."""
    try:
        s = socket.create_connection((host, port), timeout=_TCP_TIMEOUT)
        s.close()
    except OSError as exc:
        raise PortalConnectionError(
            f"Cannot reach {host}:{port} (network/VPN issue?) — {exc}"
        )


def get_oracle(conn_key: str = "raid"):
    row = db.get_connection(conn_key)
    if not row:
        raise PortalConnectionError(
            f"No active connection '{conn_key}'. Configure it under Admin → Connections."
        )
    _tcp_probe(row["host"], int(row["port"]))
    try:
        import oracledb
        dsn = oracledb.makedsn(row["host"], int(row["port"]), service_name=row["database"])
        c = oracledb.connect(user=row["username"], password=row["password"], dsn=dsn)
        c.callTimeout = 900_000  # 15 min per query call
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
    _tcp_probe(row["host"], int(row["port"]))
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
