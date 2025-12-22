# Runbook — Daily Brief Intel BI (v1.4)

This runbook is for internal operators maintaining the Daily Brief pipeline and web app.
It focuses on **local-first** operations and assumes external sources can break.

---

## 0. Non-negotiables
- UI displays **Title + Summary + Link only** (no article body).
- Do not scrape login/membership-required sources.
- Do not commit secrets. Do not log secrets.

---

## 1. Quick start (local)

### 1.1 Create venv + install deps
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -e ".[dev]"
````

### 1.2 Validate configs

```powershell
python -m tool validate-config
```

### 1.3 Initialize database

```powershell
python -m tool init-db
```

### 1.4 Run once manually

```powershell
python -m tool run manual
```

---

## 2. Web app operations

### 2.1 Required environment variables (example)

```powershell
$env:APP_SESSION_SECRET="change-me-long-random"
$env:APP_ADMIN_USER="admin"
$env:APP_ADMIN_PASS="adminpass"
$env:APP_VIEWER_USER="viewer"
$env:APP_VIEWER_PASS="viewerpass"
```

### 2.2 Start the server

```powershell
python -m uvicorn src.app.main:app --reload
```

Open: [http://127.0.0.1:8000](http://127.0.0.1:8000)

### 2.3 Access control checks

* Not logged in → `/daily` should redirect to `/login`
* viewer → `/daily` allowed; `/run/manual` denied (403)
* operator/admin → `/run/manual` allowed (implementation may return 501 in early PRs)

---

## 3. Quality gates (always run before pushing)

```powershell
python -m ruff check .
python -m pytest -q
```

---

## 4. Common failure modes and fixes

### 4.1 Config validation fails

**Symptoms**

* `python -m tool validate-config` exits non-zero or prints schema errors

**Steps**

1. Fix the YAML indicated by the validation report.
2. Re-run:

   ```powershell
   python -m tool validate-config
   ```

**Notes**

* Config is the contract. Never “hack around” config validation in code.

---

### 4.2 Database init fails

**Symptoms**

* `python -m tool init-db` fails

**Steps**

1. Confirm `output/` is writable.
2. Confirm DuckDB is installed:

   ```powershell
   python -c "import duckdb; print(duckdb.__version__)"
   ```
3. If the DB file is corrupted (rare early on), move it aside and re-init:

   ```powershell
   Move-Item output\db\app.duckdb output\db\app.duckdb.bak
   python -m tool init-db
   ```

---

### 4.3 Run lock prevents execution (concurrent run / stale lock)

**Symptoms**

* manual run fails immediately with a message indicating a lock is held

**Steps**

1. Ensure no other run process is active.
2. If you are confident it is stale, remove the lock file (path depends on implementation; locate it in `output/` or as defined in code).
3. Re-run:

   ```powershell
   python -m tool run manual
   ```

**Prevention**

* Always allow runs to finish cleanly; avoid force-killing processes.

---

### 4.4 Web app fails to start

**Symptoms**

* Uvicorn crashes on import or missing deps

**Steps**

1. Confirm install completed:

   ```powershell
   pip show fastapi uvicorn jinja2 python-multipart
   ```
2. Confirm env vars are set (especially `APP_SESSION_SECRET`).
3. Re-run:

   ```powershell
   python -m uvicorn src.app.main:app --reload
   ```

---

### 4.5 Login works but pages behave unexpectedly

**Symptoms**

* Redirect loops or 403/401 confusion

**Steps**

1. Confirm role gating expectations:

   * viewer: `/daily` only
   * operator/admin: `/run/manual` and `/exports/*`
2. Re-check env vars for usernames/passwords.
3. Restart server to ensure env changes take effect.

---

## 5. Security handling

### 5.1 Secrets storage

* Use environment variables (or a local `.env` that is gitignored).
* Never commit secrets. Never paste secrets into logs/issues.

### 5.2 Logging hygiene

* Logs must mask potential secrets (tokens, passwords).
* If you add new logging, ensure sensitive values are masked.

---

## 6. Data retention (planned)

Default retention is intended to be **30 days**, configurable via config in later PRs.
Until purge tooling is implemented, retention is an operational decision (manual cleanup only if necessary).

---

## 7. Backup and restore (planned)

A formal backup command is planned in later PRs.
For now, the simplest manual backup is:

* copy `output/db/app.duckdb` while the app is stopped.

---

## 8. Operational checklist (daily)

* [ ] `python -m tool validate-config`
* [ ] `python -m tool init-db` (should be idempotent)
* [ ] `python -m tool run manual` (or scheduled run once implemented)
* [ ] Verify `/daily` renders and shows latest successful run context
* [ ] If failures occur, record:

  * timestamp, run_id, error logs, source(s) involved