from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from src.app.auth.deps import get_current_user, get_session_manager, require_role
from src.app.auth.session import SessionData, authenticate_user
from src.core.logging import get_logger
from src.storage import queries
from src.storage.db import connect

router = APIRouter()

templates = Jinja2Templates(directory=Path(__file__).resolve().parent.parent / "templates")
logger = get_logger(__name__)


@router.get("/login", response_class=HTMLResponse)
def login_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login")
async def login(request: Request, manager=Depends(get_session_manager)):
    form = await request.form()
    username = str(form.get("username") or "")
    password = str(form.get("password") or "")
    role = authenticate_user(username, password)
    if not role:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid credentials"},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    token = manager.create_session(username, role)
    response = RedirectResponse(url="/daily", status_code=status.HTTP_303_SEE_OTHER)
    response.set_cookie(
        key=manager.cookie_name,
        value=token,
        httponly=True,
        samesite="lax",
    )
    return response


@router.get("/daily", response_class=HTMLResponse)
def daily(
    request: Request,
    user: SessionData = Depends(get_current_user),
) -> HTMLResponse:
    latest_run = None
    items = []
    counts = []
    health = []
    conn = None

    try:
        conn = connect()
        latest_run = queries.get_latest_run(conn)
        if latest_run:
            items = queries.get_items_for_run(conn, latest_run["run_id"], limit=200)
            counts = queries.get_item_counts_by_source(conn, latest_run["run_id"])
            health = queries.get_source_health(conn, latest_run["run_id"], lookback_runs=20)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not read run history: %s", exc)
    finally:
        if conn is not None:
            conn.close()

    return templates.TemplateResponse(
        "daily.html",
        {
            "request": request,
            "user": user,
            "latest_run": latest_run,
            "items": items,
            "counts": counts,
            "health": health,
        },
    )


@router.get("/run/manual")
def manual_run(_: SessionData = Depends(require_role("operator"))):
    return JSONResponse({"detail": "Manual run not implemented in PR0"}, status_code=501)


@router.get("/exports/{rest_of_path:path}")
def exports(_: SessionData = Depends(require_role("operator"))):
    return JSONResponse({"detail": "Exports not implemented in PR0"}, status_code=501)


@router.get("/exports")
def exports_root(_: SessionData = Depends(require_role("operator"))):
    return JSONResponse({"detail": "Exports not implemented in PR0"}, status_code=501)
