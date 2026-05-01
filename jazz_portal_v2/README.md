# Jazz BA Portal — Local Setup

## Quick start (Windows, Oracle already working)

```powershell
# 1. Create and activate a virtual environment
cd jazz_portal_v2
python -m venv venv
venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run
python run.py
```

Open http://localhost:5000

Default admin login: **admin / admin123** — change it immediately at Admin → Users.

---

## Project structure

```
jazz_portal_v2/
├── run.py                  ← entry point
├── config.py               ← all config (DB path, secret key, workers)
├── portal/
│   ├── __init__.py         ← app factory (create_app)
│   ├── db.py               ← SQLite store: queries, connections, audit, admins
│   ├── connections.py      ← live DB connection factory (reads from SQLite)
│   ├── auth.py             ← admin_required decorator
│   ├── templates/
│   │   └── base.html       ← shared navbar, all pages extend this
│   └── blueprints/
│       ├── home/           ← portal homepage
│       ├── revenue_validation/
│       │   ├── components.py   ← metadata only (label, group, indexes)
│       │   ├── engine.py       ← parallel execution + comparison logic
│       │   └── routes.py       ← Flask routes + SSE job streaming
│       ├── admin/          ← query manager, connections, audit, users
│       └── commissions/    ← stub, ready to fill in
└── portal.db               ← created automatically on first run
```

## Adding a new control (e.g. AIT Validation)

1. `mkdir portal/blueprints/ait_validation`
2. Copy the `commissions` blueprint as a starting point
3. Create `components.py` and `engine.py` for that control's logic
4. Register the blueprint in `portal/__init__.py`
5. Add a nav link in `portal/templates/base.html`
6. Seed queries in `portal/db.py → _seed_queries()`

## Environment variables

| Variable    | Default                    | Purpose                        |
|-------------|----------------------------|--------------------------------|
| PORTAL_DB   | `./portal.db`              | SQLite file location           |
| SECRET_KEY  | `dev-secret-change-in-production` | Flask session signing key |
| MAX_WORKERS | `6`                        | Parallel query workers         |

## Changing DB connections

Go to **Admin → Connections** in the portal UI.
No code changes, no restart needed.
