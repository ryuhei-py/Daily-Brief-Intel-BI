from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from src.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SessionData:
    username: str
    role: str
    issued_at: datetime


class SessionManager:
    def __init__(self, secret: str, cookie_name: str = "app_session"):
        self.secret = secret.encode("utf-8")
        self.cookie_name = cookie_name

    def _sign(self, payload: str) -> str:
        signature = hmac.new(self.secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()
        token = f"{payload}.{signature}"
        return base64.urlsafe_b64encode(token.encode("utf-8")).decode("utf-8")

    def _verify(self, token: str) -> Optional[str]:
        try:
            decoded = base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
            payload, signature = decoded.rsplit(".", 1)
        except Exception:
            return None
        expected = hmac.new(self.secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return None
        return payload

    def create_session(self, username: str, role: str) -> str:
        issued_at = datetime.now(tz=timezone.utc).isoformat()
        payload = json.dumps(
            {"username": username, "role": role, "issued_at": issued_at},
            separators=(",", ":"),
        )
        return self._sign(payload)

    def read_session(self, token: str) -> Optional[SessionData]:
        payload = self._verify(token)
        if not payload:
            return None
        try:
            data = json.loads(payload)
            return SessionData(
                username=data["username"],
                role=data["role"],
                issued_at=datetime.fromisoformat(data["issued_at"]),
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed parsing session: %s", exc)
            return None


def authenticate_user(username: str, password: str) -> Optional[str]:
    admin_user = os.getenv("APP_ADMIN_USER")
    admin_pass = os.getenv("APP_ADMIN_PASS")
    viewer_user = os.getenv("APP_VIEWER_USER")
    viewer_pass = os.getenv("APP_VIEWER_PASS")

    if admin_user and admin_pass and username == admin_user and password == admin_pass:
        return "operator"
    if viewer_user and viewer_pass and username == viewer_user and password == viewer_pass:
        return "viewer"
    return None
