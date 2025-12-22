# PR Build Plan (End-to-End) — Daily Brief Intel BI

This document defines the **end-to-end PR slicing plan**.  
Principle: **1 PR = 1 responsibility**, and every PR must include explicit **Acceptance Criteria** and be verifiable via `ruff` + `pytest`.

---

## Global PR rules
- Keep PRs small and patch-oriented; avoid mixed concerns.
- Config is the contract: changes should prefer YAML over code edits.
- Never violate non-negotiables:
  - UI shows **Title + Summary + Link only** (no full body).
  - Avoid login-required / membership-only sources.
  - Preserve auditability: everything ties back to `run_id`.
  - Secrets must never be logged or committed.
- Each PR should map to at least one row in the Traceability Matrix. If not, update it first.

---

## Phase overview
- **Phase 0:** Governance + foundation (PR00–PR05)
- **Phase 1:** Collection (news/indicators) + audit persistence (PR06–PR12)
- **Phase 2:** Analysis (watchlist/indices/alerts) (PR13–PR18)
- **Phase 3:** Daily Brief UI completion (PR19–PR23)
- **Phase 4:** Operations (Ops/TTL/Backup/Runbook) (PR24–PR28)
- **Phase 5:** Extensions (BI connectivity, notifications, plugin architecture) (PR29–PR33)

---

## Phase 0 — Governance + foundation

### PR00: Docs freeze (v1.4, ADRs, Traceability, PR Plan)
**Goal:** Freeze design artifacts in-repo to prevent drift.  
**Deliverables:** `docs/INDEX.md`, `docs/v1.4_*`, `docs/adr/*`, README doc links.  
**AC:** Docs exist; README links; code unchanged; `ruff` + `pytest` unaffected.

### PR01: Repo skeleton and tooling baseline
**Goal:** Fix structure so later PRs are predictable.  
**AC:** project installs cleanly; minimal module layout is stable.

### PR02: Config schema / loader / validate CLI
**Goal:** Config is the contract; validate before running.  
**AC:** `python -m tool validate-config` passes for valid YAML, fails for invalid YAML with clear error.

### PR03: DuckDB storage bootstrap and migrations
**Goal:** Local-first persistence with idempotent schema application.  
**AC:** `python -m tool init-db` is idempotent; required tables exist.

### PR04: Run manager (run_id, lock, status, per-source logging)
**Goal:** Robust run lifecycle with concurrency prevention.  
**AC:** `python -m tool run manual` records run_id and status; lock prevents double-run; lock always released.

### PR05: Auth + Daily skeleton UI (FastAPI/Jinja/HTMX)
**Goal:** Operational UI and role gating.  
**AC:** login works; viewer can access `/daily`; viewer denied `/run/manual`; operator allowed.

---

## Phase 1 — Collection + audit persistence

### PR06: Ingest foundation
**Includes:** connector interface, http client, retry/backoff, rate limiting (config-driven).  
**AC:** new source can be added by config + connector mapping; per-source status recorded.

### PR07: News ingestion (RSS / list HTML) — L0
**AC:** news items persisted with `title/summary/url/published_at/source_id/run_id`; dedupe prevents duplicates.

### PR08: Summary normalization (no body display)
**AC:** UI still shows only Title+Summary+Link; summary pipeline does not store or display full bodies.

### PR09: Series resolver foundation (series.yml → resolved source identifiers)
**AC:** unresolved series do not crash; status recorded; resolver cache persists.

### PR10: Indicator ingestion (API/CSV/SDMX as applicable)
**AC:** points stored with series_key/date/value/unit/geo; missing handled.

### PR11: Source health & partial failure hardening
**AC:** one source failure does not crash run; run marked partial; failures visible in ops view.

### PR12: Backfill window (default 30 days, configurable)
**AC:** backfill period configurable; backfill is bounded and safe.

---

## Phase 2 — Analysis (watchlist / indices / alerts)

### PR13: Watchlist mention extraction + provenance
**AC:** mention matches stored with match_type and provenance.

### PR14: Geo rollups application
**AC:** tokyo_metro default; expansion via config.

### PR15: Indices computation (tightness / attention / risk) + explain_json
**AC:** index outputs are reproducible; explain_json includes inputs and missing flags.

### PR16: Alerts P1 (policy/measurement/platform critical events)
**AC:** alerts contain reason + evidence links; severity consistent; config-driven thresholds.

### PR17: Alerts P2 (spike detection vs baseline)
**AC:** windows and thresholds configurable; edge cases tested.

### PR18: Explainability packaging
**AC:** for each alert/index, explain_json is sufficient to justify the result.

---

## Phase 3 — Daily Brief UI completion

### PR19: `/daily` real data rendering
**AC:** renders without crashing with empty DB; renders correct for latest successful run.

### PR20: `/run/manual` UI (operator-only)
**AC:** manual run triggers and status visible; role-gated.

### PR21: `/exports/*` (alerts/digest CSV/JSON) operator-only
**AC:** exports available; schema stable; no secrets.

### PR22: Filters/search (watchlist/geo/date)
**AC:** filters work; performance acceptable for daily scale.

### PR23: UI polish (readability, navigation)
**AC:** daily workflow is fast, clear, and stable.

---

## Phase 4 — Operations

### PR24: TTL/retention purge (default 30 days, configurable)
**AC:** purge works; dry-run optional; safe.

### PR25: Backup/restore (daily snapshot)
**AC:** snapshot created; restore documented.

### PR26: Runbook completion (failure modes + recovery)
**AC:** runbook includes concrete commands and scenarios.

### PR27: Audit logging improvements (optional)
**AC:** access/logging is safe; no PII leakage.

### PR28: Health checks / ops dashboard
**AC:** failures are visible; time-to-diagnose reduced.

---

## Phase 5 — Extensions

### PR29: Notification plugins (Slack/email)
**AC:** plugin interface exists; config-driven; safe defaults.

### PR30: BI connectivity views (DuckDB SQL views)
**AC:** views stable; PowerBI/Tableau connectivity possible.

### PR31: Connector plugin packaging
**AC:** adding connectors is standardized; docs included.

### PR32: Fine-grained authorization (future)
**AC:** roles extensible; no breaking changes.

### PR33: Performance tuning (cache/delta ingestion)
**AC:** faster runs without sacrificing auditability.
