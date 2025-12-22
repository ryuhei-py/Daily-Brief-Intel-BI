# Daily-Brief-Intel-BI

A local-first, auditable, config-driven Daily Brief dashboard and pipeline for **Recruiting** and **Recruiting Web Marketing** decisions.

This project is designed for **operational robustness** and **compliance-conscious collection**:
- Avoids login-required / membership-only sources.
- UI displays **Title + Summary + Link only** (no full-article body display).

## What this delivers (v1.4)
- Daily run (default **07:00 JST**, configurable) + manual run
- “Title + Summary + Link” only (no article body scraping)
- Config-driven sources, watchlists, geo rollups, scoring/alerts rules
- Local-first storage (DuckDB) with run/audit lineage (`run_id`)
- Web UI (FastAPI + Jinja2 + HTMX) for Daily Brief
- Export-friendly outputs for later BI connectivity (via SQL views / exports)

## Docs (design-freeze)
- Docs Index: `docs/INDEX.md`
- Design Freeze (v1.4): `docs/v1.4_design_freeze.md`
- Traceability Matrix (v1.4): `docs/v1.4_traceability_matrix.md`
- PR Build Plan: `docs/pr_build_plan.md`
- ADRs: `docs/adr/`
- Architecture: `docs/architecture.md`
- Runbook: `docs/runbook.md`

## Quickstart (local, Windows PowerShell)
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -e ".[dev]"

python -m tool validate-config
python -m tool init-db
python -m tool run manual
````

## Run the web app

```powershell
$env:APP_SESSION_SECRET="change-me-long-random"
$env:APP_ADMIN_USER="admin"
$env:APP_ADMIN_PASS="adminpass"
$env:APP_VIEWER_USER="viewer"
$env:APP_VIEWER_PASS="viewerpass"

python -m uvicorn src.app.main:app --reload
```

Open: [http://127.0.0.1:8000](http://127.0.0.1:8000)

## Compliance note

Only collect from publicly accessible sources that permit automated access under their terms.
Avoid membership/login-required pages and avoid redistributing full copyrighted article bodies.

## Quality gates

```powershell
python -m ruff check .
python -m pytest -q
```

