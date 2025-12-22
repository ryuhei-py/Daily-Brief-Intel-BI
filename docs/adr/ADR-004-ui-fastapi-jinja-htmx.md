# ADR-004: Web UI via FastAPI + Jinja2 + HTMX (Operational, Audit-Friendly)

## Status
Accepted

## Context
The tool is used internally, daily.  
We need:
- authentication and role gating
- operational control (manual run, exports)
- audit-friendly server-side rendering
- stable deployment behavior

Streamlit-style apps can be fast for prototypes but are weaker for operational controls and governance.

## Decision
Use FastAPI as the web server and API layer, with:
- Jinja2 templates for server-rendered pages
- HTMX for partial updates and interactivity

Roles:
- viewer: `/daily`
- operator: `/run/manual`, `/exports/*`

UI safety contract: **Title + Summary + Link only**.

## Consequences
- Strong operational governance and security posture.
- Some additional UI engineering vs pure dashboard tools.

## Acceptance criteria
- `/login` exists with session auth.
- `/daily` requires authentication.
- `/run/manual` and `/exports/*` are operator-only.
- UI never displays full article body.
