import os
from pathlib import Path

BASE_DIR = Path(__file__).parent


class Config:
    # Flask
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-in-production")

    # SQLite store — queries, connections, audit log, admin users
    DB_PATH = Path(os.getenv("PORTAL_DB", str(BASE_DIR / "portal.db")))

    # Parallel workers for validation runs
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))

    # How many audit rows to show in the UI
    AUDIT_LIMIT = 500
