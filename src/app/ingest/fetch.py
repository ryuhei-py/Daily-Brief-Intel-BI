from __future__ import annotations

from typing import Iterable, Optional

import httpx

DEFAULT_TIMEOUT = 10.0
USER_AGENT = "DailyBriefIntel/0.0.1"
MAX_RETRIES = 2


class FetchError(Exception):
    pass


def fetch_text(
    url: str,
    allowed_urls: Optional[Iterable[str]] = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> str:
    if allowed_urls is not None:
        if not any(str(url).startswith(allowed) for allowed in allowed_urls):
            raise FetchError("URL not allowed")

    headers = {"User-Agent": USER_AGENT}
    last_exc: Exception | None = None
    for _ in range(MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=timeout, headers=headers) as client:
                response = client.get(url)
                response.raise_for_status()
                return response.text
        except (httpx.HTTPError, httpx.TimeoutException) as exc:  # pragma: no cover - network issue
            last_exc = exc
            continue
    raise FetchError(str(last_exc) if last_exc else "Fetch failed")
