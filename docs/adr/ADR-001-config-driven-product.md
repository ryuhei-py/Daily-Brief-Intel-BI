# ADR-001: Config-Driven Product (Config is the Contract)

## Status
Accepted

## Context
This product requires frequent operational changes:
- add/remove sources
- modify watchlist entities
- expand geo rollups
- change schedule time(s)
- adjust retention window
- tweak scoring and alert thresholds

If these changes require code edits each time, operations become brittle and slow.

## Decision
All variable behavior must be defined in `config/*.yml`.  
Code implements a generic engine; configuration is treated as a **contract**.

Configuration is validated via schemas before execution (fail fast).

## Consequences
- Operational changes are fast and safe.
- Validation becomes critical: misconfiguration becomes a primary failure mode.
- Docs and schema evolution must be carefully managed.

## Acceptance criteria
- Sources can be enabled/disabled and parameterized via `sources.yml` without code changes (within supported connector kinds).
- Watchlist changes in `watchlist.yml` take effect on the next run without code changes.
- Geo and schedule changes in `geo.yml` / `schedule.yml` take effect without code changes.
- Invalid config fails before any run starts, with a clear validation report.
