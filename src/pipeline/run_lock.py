from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


class RunLockedError(Exception):
    """Raised when a run is already in progress."""


class RunLock:
    def __init__(self, path: Optional[Path] = None, stale_after_minutes: int = 30):
        self.path = Path(path) if path else Path("output/run.lock")
        self.stale_after = timedelta(minutes=stale_after_minutes)
        self._acquired = False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            modified = datetime.fromtimestamp(self.path.stat().st_mtime, tz=timezone.utc)
            age = datetime.now(tz=timezone.utc) - modified
            if age < self.stale_after:
                raise RunLockedError(f"Existing run lock at {self.path}")
            self.path.unlink(missing_ok=True)
        timestamp = datetime.now(tz=timezone.utc).isoformat()
        self.path.write_text(timestamp, encoding="utf-8")
        self._acquired = True

    def release(self) -> None:
        if self._acquired and self.path.exists():
            self.path.unlink(missing_ok=True)
        self._acquired = False

    def __enter__(self) -> "RunLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.release()
