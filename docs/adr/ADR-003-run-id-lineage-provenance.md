# ADR-003: Run ID + Lineage (Provenance) as a First-Class Concept

## Status
Accepted

## Context
Without explainability, results cannot be trusted for real business decisions.  
External sources fail; debugging and recovery must be fast.

We need to answer:
- Which run produced this metric/alert?
- Which sources contributed?
- What failed, and why?
- What evidence supports the alert?

## Decision
Every pipeline execution is identified by a `run_id` (UUID).  
Persist:
- run lifecycle (start/end/status/mode)
- per-source run status
- outputs (news items, series points, indices, alerts)
- explain_json and evidence links for derived results

UI always shows run context and evidence links.

## Consequences
- High auditability and debuggability.
- More logging/persistence requirements.
- Run management (locking/status/error capture) is mandatory.

## Acceptance criteria
- Manual run generates a `run_id` and records status.
- Alerts include a reason and evidence link(s) tied to a run.
- UI can display the run_id and evidence for decisions.
