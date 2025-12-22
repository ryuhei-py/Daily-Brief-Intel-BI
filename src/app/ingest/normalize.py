from __future__ import annotations

import hashlib
import html
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional


def clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    unescaped = html.unescape(value)
    condensed = re.sub(r"\s+", " ", unescaped).strip()
    return condensed


def parse_date(value: Optional[str]) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        dt = parsedate_to_datetime(value)
        if dt and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime.now(timezone.utc)


def item_key(source_id: str, url: str) -> str:
    return hashlib.sha256(f"{source_id}-{url}".encode("utf-8")).hexdigest()
