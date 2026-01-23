# CGD FastAPI Backend (EC2-first) — SQLAlchemy 2.x

This repo is a **starter backend** for migrating a legacy CGI/Perl CGD codebase to Python.

## Key structure (simple, consistent)
- `cgd/api/routers/` : FastAPI routers (HTTP only)
- `cgd/api/crud/`    : DB queries (SQLAlchemy text()/Core/ORM)
- `cgd/api/schemas/` : Pydantic models (request/response)
- `cgd/db/`          : engine/session + dependencies
- `cgd/core/`        : settings

## Local / EC2 run (no Docker)

### 1) Create venv and install deps
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure environment
Copy `.env.example` to `.env` and set:

- `DATABASE_URL` (SQLAlchemy URL)
- optional `DB_SCHEMA`

```bash
cp .env.example .env
```

### 3) Run (dev)
```bash
uvicorn cgd.main:app --reload --port 8000 --host 0.0.0.0
```

Visit:
- `http://<ec2-host>:8000/health`
- `http://<ec2-host>:8000/api/locus?locus=ACT1`

## EC2 production-ish run (systemd + gunicorn)
See:
- `deploy/systemd/cgd-api.service`
- `deploy/nginx/cgd-api.conf`

Typical flow:
1. Install deps in `/opt/cgd_api` (or similar)
2. Put `.env` next to the code
3. `sudo cp deploy/systemd/cgd-api.service /etc/systemd/system/`
4. Edit paths/user in the unit file
5. `sudo systemctl daemon-reload && sudo systemctl enable --now cgd-api`
6. Configure Nginx reverse proxy to `127.0.0.1:8000`

## Endpoints
- `GET /health` (no prefix)
- `GET /api/locus?locus=...`
- `GET /api/search?class=...&item=...` (legacy-style dispatcher; expand mapping)

## Notes
- The `locus` SQL uses placeholder table/columns (`feature`). Update to match CGD schema.
- Phase 1 migration usually stays on raw SQL via `text()` for speed; ORM can be added later.
