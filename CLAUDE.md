# CLAUDE.md — Jazz (JazzWorld)
# Last updated: 2026-04-30

## Current Focus
jazz_portal_v2 — Revenue Assurance portal (as of 2026-04-30)

- Portal running locally at http://127.0.0.1:5000, end-to-end functional
- RAID (Oracle) connected via oracledb thick mode (Instant Client 23c at default path)
- 10/12 RAID components returning data; callTimeout=600s set on all Oracle connections
- 2 components blocked by ORA-03113 (Oracle kills session on raid_t_pre_out_daily):
    nonusage_sub_bundles, nonusage_sdp_other → raise with DBA to lift RA_ALERT_USER1 profile
- Hive queries NOT yet validated — server at 10.50.142.230:10000 currently unreachable
- nonusage_vas_rbt and nonusage_vas_vic are Hive-only; code routes correctly, pending server
- Next: re-test Hive when server is up, then deploy to Windows server

## What this is
Analytics, automation, portals, and executive reporting for JazzWorld,
Pakistan's largest telecom. Arham is Analytics Manager.

## Project structure
jazz/
├── jazz_portal_v2/        ← Revenue Assurance portal (active)
├── [commission_validation/] ← Notebooks live elsewhere, not yet here
├── [presentations/]        ← Canva/PowerPoint, external
└── CLAUDE.md

## Active workstreams

### 1. jazz_portal_v2 — Revenue Assurance Portal (Flask)
- Framework: Flask 3.x, entry point: run.py (port 5000)
- Config store: SQLite (portal.db) — connections, queries, audit log, users
- Source DB 1: Oracle via oracledb → RAID at 10.50.148.15:1521, service RAID
- Source DB 2: Hive via pyhive → 10.50.142.230:10000, db raid_jazz / fms_jazz
- Blueprints:
  - home → stub
  - revenue_validation → implemented (parallel RAID vs Hive, SSE streaming)
  - admin → implemented (connection manager, query editor, audit log, users)
  - commissions → stub only, no logic yet
- Concurrency: ThreadPoolExecutor, MAX_WORKERS=6
- Auth: session cookie, SHA-256 hashing (upgrade to bcrypt is a known todo)

### 2. Commission Validation (Jupyter)
- Notebook: New_Sale___Data_-_Commission_-_v5.ipynb (not in this repo yet)
- New sale: PKR 80/MSISDN; Data: PKR 60 for 1,000 MB 4G usage
- Max payout: PKR 140/MSISDN, aggregated at POS_ID level
- Eligibility: DIFFERENCE=1, INFRINGEMENT_SALES_COUNT=0, GREY_TRAFFIC_NOS=0

### 3. MSISDN Pipeline
- SQL Server (10.50.18.144, PREPAID db) → Hive
- Batches of 1,000, retry logic (3 attempts, 5s sleep), per-job Hive reconnect
- Not yet in this repo

### 4. Executive Presentations (external)
- 5G deck: Canva DAGHXBGrq0I (CFO/CEO audience, delivered)
- Fraud Risk Management deck: C-suite, active

## Known technical debt (portal)
- Seed credentials in db.py — RAID password falls back to hardcoded; move to env var
- Asymmetric query coverage: some components have no RAID query (Hive-only) or vice versa
- Auth: SHA-256 password hashing — upgrade to bcrypt before any shared deployment
- Import path coupling — Flask must be run from jazz_portal_v2/ dir

## Rules
- Never modify production Oracle (RAID) or Hive tables directly
- All connection params live in SQLite, editable via admin UI — no hardcoding
- Always include dedup and retry logic in pipeline scripts
- Validate commission models against source before presenting
- Presentations use JazzWorld branding
- Scope: only work within this project folder

## Session start
1. Read this file
2. Ask what the specific task is
3. State your understanding before doing anything
4. Work in small, verifiable steps