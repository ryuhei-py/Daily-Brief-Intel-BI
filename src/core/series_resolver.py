from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import yaml
from pydantic import ValidationError

from src.core.config_schema import SeriesConfig
from src.storage.series_cache import upsert_series_resolution


def _load_series_config(config_dir: Path) -> SeriesConfig:
    path = config_dir / "series.yml"
    if not path.exists():
        return SeriesConfig(series=[])
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    try:
        return SeriesConfig(**data)
    except ValidationError:
        return SeriesConfig(series=[])


def _resolve_entry(series_entry, conn) -> Dict[str, Optional[str]]:
    resolver_type = series_entry.resolver.type
    resolver_value = series_entry.resolver.value
    status = "resolved"
    resolved_id: Optional[str] = None
    message: Optional[str] = None

    try:
        if resolver_type in {"passthrough", "source_id"}:
            if resolver_value:
                resolved_id = resolver_value
            else:
                status = "unresolved"
                message = "Missing resolver value"
        else:
            status = "unresolved"
            message = f"Unknown resolver type: {resolver_type}"
    except Exception as exc:  # pragma: no cover - defensive
        status = "error"
        message = str(exc)

    upsert_series_resolution(
        conn=conn,
        series_key=series_entry.key,
        resolver_type=resolver_type,
        resolver_value=resolver_value,
        resolved_id=resolved_id,
        status=status,
        message=message,
    )

    return {
        "resolved_id": resolved_id,
        "status": status,
        "message": message,
    }


def resolve_series_config(config_dir: Path, conn) -> Dict[str, dict]:
    config = _load_series_config(config_dir)
    results: Dict[str, dict] = {}
    for entry in config.series:
        result = _resolve_entry(entry, conn)
        results[entry.key] = result
    return results
