# ADR-002: Local-First Storage (DuckDB) + Audit Persistence

## Status
Accepted

## Context
The initial deployment is local-first.  
Daily decision support requires:
- reproducibility
- auditability
- traceability over time
- simple backup and restore

We also want a clean path to later BI connectivity (PowerBI/Tableau) via stable views/exports.

## Decision
Use DuckDB as the primary local persistent store.  
Persist run lineage and key outputs, tying all artifacts to `run_id`.

Design schemas for stable export and future BI connectivity (SQL views).

## Consequences
- Local setup is simple and debuggable.
- Backups are straightforward (single file + optional raw snapshots).
- Schema stability and migration discipline become important.

## Acceptance criteria
- `init-db` creates the database and required tables idempotently.
- All key outputs can be traced to a `run_id`.
- Later PRs can add SQL views/exports without re-architecting storage.
