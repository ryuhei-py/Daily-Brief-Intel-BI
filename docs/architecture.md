# Architecture — Daily Brief Intel BI (v1.4)

This document describes the **system structure** and **module boundaries** under the v1.4 design freeze.
It is intended to keep implementation consistent as the codebase grows.

---

## 1. System overview

Daily Brief Intel BI is a local-first system that:
1) Collects news and indicators from public sources (compliance-conscious).
2) Normalizes and persists results in DuckDB with full run lineage (`run_id`).
3) Computes indices and alerts with explainability.
4) Serves an internal web UI (Daily Brief) and exports.

---

## 2. Key design constraints
- UI: **Title + Summary + Link only** (no article body).
- Compliance: avoid login/membership-only and redistribution-restricted sources.
- Config-driven: key behavior must be modifiable via `config/*.yml`.
- Auditability: every output must be traceable to `run_id`.
- Robust operations: partial failure tolerated; retry + lock.

---

## 3. Layered architecture

### 3.1 Presentation layer (Web)
**Tech:** FastAPI + Jinja2 + HTMX  
**Responsibilities**
- Authentication & session handling
- Role gating: viewer/operator
- Render Daily Brief and operational actions (manual run, exports)

**Must not**
- embed business rules directly; consume “query layer” / stored results
- display full article bodies

### 3.2 Application / orchestration layer (Pipeline)
**Responsibilities**
- Run lifecycle management: create run, record status, finalize
- Concurrency control via run lock
- Orchestrate ingest → normalize → compute → persist

### 3.3 Domain logic layer (Core)
**Responsibilities**
- Config schema and loading
- Series resolver registry (future)
- Watchlist matching logic (future)
- Scoring/alert rules (future)
- Explainability object construction (future)

### 3.4 Infrastructure layer (Storage + HTTP)
**Responsibilities**
- DuckDB connection management
- Schema and migrations
- Raw/audit storage (if enabled later)
- HTTP client with retry/rate limit for connectors

---

## 4. Run lineage model

Every run creates a `run_id` and persists:
- run metadata: mode (scheduled/manual), timestamps, status
- per-source statuses (planned deeper)
- derived outputs: news items, series points, indices, alerts
- explain_json for computed results

**Goal:** “Why did we show this?” must be answerable from stored data.

---

## 5. Configuration model (contract surface)

Config files are validated before execution:
- `config/sources.yml` — sources, rate limits, connector kind, enable flags
- `config/watchlist.yml` — entities and categories; matching rules
- `config/geo.yml` — geo rollups (default tokyo_metro)
- `config/schedule.yml` — schedules (default 07:00 JST)
- `config/series.yml` — series keys and resolver mapping
- `config/scoring.yml` — index weights and normalization
- `config/alerts.yml` — thresholds/windows/severity mapping

---

## 6. Security posture (local-first)
- Secrets in environment variables / local .env (gitignored)
- Role-gated operational endpoints
- Logging must mask sensitive values
- No article bodies stored or displayed in UI

---

## 7. Extension points (planned)
- Connector types (RSS/API/HTML list as allowed, respecting terms)
- Series resolvers
- Notification plugins (Slack/email)
- DuckDB SQL views for external BI tools
- Additional geo rollups and watchlist constraints