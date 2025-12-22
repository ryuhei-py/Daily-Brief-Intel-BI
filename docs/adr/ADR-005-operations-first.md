# ADR-005: Operations First (Assume Sources Break)

## Status
Accepted

## Context
External sources will change and fail:
- RSS format changes
- API rate limits
- transient network errors
- HTML layout changes

We must prevent operational fragility and ensure rapid recovery.

## Decision
Adopt “operations-first” design:
- failure isolation per source (one source failing does not crash the whole run)
- bounded retries with backoff
- concurrency prevention via run lock
- retention policy (default 30 days, configurable)
- backup/restore path (daily snapshots)
- secrets hygiene (env/.env; never log secrets)

## Consequences
- Higher reliability and faster recovery.
- Requires run management, logging, and ops visibility features.

## Acceptance criteria
- Lock prevents concurrent runs and always releases.
- Run status clearly captures partial failure vs success.
- Failures are diagnosable from run logs and per-source statuses.
- Retention and backup policies can be implemented without redesign.
- Secrets never appear in logs or committed files.
