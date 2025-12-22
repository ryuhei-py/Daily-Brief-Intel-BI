from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List
from urllib.parse import urlencode

from src.app.ingest.normalize import clean_text, parse_date


def build_estat_url(base_url: str, params: Dict[str, Any]) -> str:
    query = {
        "appId": params.get("app_id", ""),
        "statsDataId": params.get("dataset_id") or params.get("stats_data_id"),
        "cdCat01": params.get("category"),
        "cdTime": params.get("time"),
    }
    # Remove empty entries
    query = {k: v for k, v in query.items() if v}
    extra = params.get("query")
    if isinstance(extra, dict):
        query.update(extra)
    return f"{base_url}?{urlencode(query)}"


def parse_estat(content: str, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = json.loads(content)
    stats_data = data.get("GET_STATS_DATA", {}).get("STATISTICAL_DATA", {})
    title = stats_data.get("TABLE_INF", {}).get("TITLE", {}).get("@title", source["name"])
    values: Iterable[dict[str, Any]] = stats_data.get("DATA_INF", {}).get("VALUE", []) or []
    now = datetime.now(timezone.utc)
    items: List[Dict[str, Any]] = []
    for idx, record in enumerate(values):
        val = clean_text(record.get("$"))
        time_label = record.get("@time") or record.get("@time_code") or ""
        full_title = clean_text(f"{title} {time_label}".strip())
        url = source.get("url") or ""
        published_at = parse_date(record.get("@date") or record.get("@time") or "")
        items.append(
            {
                "source_id": source["id"],
                "source_name": source["name"],
                "category": source["category"],
                "kind": source["kind"],
                "title": full_title or source["name"],
                "summary": val,
                "url": url,
                "published_at": published_at,
                "fetched_at": now,
            }
        )
    return items
