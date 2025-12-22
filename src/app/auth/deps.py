from __future__ import annotations

import os
from functools import lru_cache
from typing import Callable

from fastapi import Depends, HTTPException, Request, status

from .session import SessionData, SessionManager


@lru_cache(maxsize=1)
def get_session_manager() -> SessionManager:
    secret = os.getenv("APP_SESSION_SECRET", "dev-session-secret")
    return SessionManager(secret=secret)


def get_current_user(
    request: Request, manager: SessionManager = Depends(get_session_manager)
) -> SessionData:
    token = request.cookies.get(manager.cookie_name)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    session = manager.read_session(token)
    if not session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    return session


def require_role(role: str) -> Callable[[SessionData], SessionData]:
    def dependency(user: SessionData = Depends(get_current_user)) -> SessionData:
        if user.role != role:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
        return user

    return dependency
