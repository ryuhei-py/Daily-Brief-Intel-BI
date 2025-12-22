from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Tuple
from uuid import uuid4

from src.app.ingest.estat import build_estat_url, parse_estat
from src.app.ingest.fetch import fetch_text
from src.app.ingest.rss import parse_rss
from src.core.config_loader import load_sources_config
from src.core.logging import get_logger
from src.pipeline.run_manager import create_run, finish_run, record_source_run
from src.storage.db import connect
from src.storage.migrate import init_db

logger = get_logger(__name__)


def _output_root() -> Path:
    return Path(os.getenv("APP_OUTPUT_ROOT", "output/runs"))


def _default_fetcher(url: str, allowed_urls: Iterable[str]) -> str:
    return fetch_text(url, allowed_urls=allowed_urls)


def _write_exports(run_id: str, items: List[Dict[str, object]], stats: dict) -> Path:
    output_dir = _output_root() / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    items_path = output_dir / "brief_items.csv"
    alerts_path = output_dir / "alerts.json"
    stats_path = output_dir / "run_stats.json"

    with items_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "source_id",
                "source_name",
                "category",
                "kind",
                "title",
                "summary",
                "url",
                "published_at",
            ],
        )
        writer.writeheader()
        for item in items:
            writer.writerow(
                {
                    "source_id": item["source_id"],
                    "source_name": item["source_name"],
                    "category": item["category"],
                    "kind": item["kind"],
                    "title": item["title"],
                    "summary": item["summary"],
                    "url": item["url"],
                    "published_at": (
                        item["published_at"].isoformat()
                        if hasattr(item["published_at"], "isoformat")
                        else item["published_at"]
                    ),
                }
            )

    alerts_path.write_text(json.dumps([], indent=2), encoding="utf-8")
    stats_path.write_text(json.dumps(stats, indent=2, default=str), encoding="utf-8")
    return output_dir


def _dedupe_items(items: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    seen = set()
    unique: List[Dict[str, object]] = []
    for item in items:
        key = (item.get("source_id"), item.get("url"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def run_pipeline(
    config_dir: Path | str = "config",
    mode: str = "manual",
    fetcher: Callable[[str, Iterable[str]], str] | None = None,
) -> Tuple[str, Path]:
    config_path = Path(config_dir)
    sources_config = load_sources_config(config_path)
    init_db()
    conn = connect()

    run_id = os.getenv("RUN_ID") or str(uuid4())
    started_at = datetime.now(timezone.utc)
    create_run(run_mode=mode, params_json="{}", conn=conn, run_id=run_id)

    allowed_urls = {s.url for s in sources_config.sources if s.url}
    fetch_fn = fetcher or _default_fetcher

    collected_items: List[Dict[str, object]] = []
    source_stats: Dict[str, dict] = {}

    for source in sources_config.sources:
        if not source.enabled:
            source_stats[source.id] = {"status": "disabled", "count": 0, "error": None}

    enabled_sources = [s for s in sources_config.sources if s.enabled]
    for source in enabled_sources:
        request_url = source.url or ""
        try:
            if source.kind == "rss":
                content = fetch_fn(request_url, allowed_urls)
                items = parse_rss(content, source.model_dump())
            elif source.kind == "estat_api":
                request_url = build_estat_url(request_url, source.params)
                allowed_urls.add(source.url)
                content = fetch_fn(request_url, allowed_urls)
                items = parse_estat(content, source.model_dump())
            else:
                raise ValueError(f"Unsupported source kind: {source.kind}")
            source_stats[source.id] = {"status": "success", "count": len(items), "error": None}
            collected_items.extend(items)
            record_source_run(
                run_id=run_id,
                source_id=source.id,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="success",
                item_count=len(items),
                conn=conn,
            )
        except Exception as exc:
            logger.warning("Source %s failed: %s", source.id, exc)
            source_stats[source.id] = {"status": "failed", "count": 0, "error": str(exc)}
            record_source_run(
                run_id=run_id,
                source_id=source.id,
                started_at=started_at,
                ended_at=datetime.now(timezone.utc),
                status="failed",
                item_count=0,
                error_class=exc.__class__.__name__,
                error_message=str(exc),
                conn=conn,
            )

    unique_items = _dedupe_items(collected_items)

    # Persist metadata
    conn.execute("DELETE FROM runs WHERE run_id = ?", [run_id])
    conn.execute("DELETE FROM sources WHERE run_id = ?", [run_id])
    conn.execute("DELETE FROM items WHERE run_id = ?", [run_id])

    overall_status = "success"
    if any(stat["status"] == "failed" for stat in source_stats.values()):
        overall_status = "partial"
    if not enabled_sources:
        overall_status = "failed"

    for source in sources_config.sources:
        conn.execute(
            """
            INSERT INTO sources (run_id, source_id, source_name, category, kind, enabled)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [run_id, source.id, source.name, source.category, source.kind, source.enabled],
        )

    for item in unique_items:
        conn.execute(
            """
            INSERT INTO items (
                run_id, source_id, source_name, category, kind,
                title, summary, url, published_at, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                item["source_id"],
                item["source_name"],
                item["category"],
                item["kind"],
                item["title"],
                item["summary"],
                item["url"],
                item["published_at"],
                item["fetched_at"],
            ],
        )

    finished_at = datetime.now(timezone.utc)
    conn.execute(
        """
        INSERT INTO runs (run_id, started_at, finished_at, status, item_count, source_count)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [run_id, started_at, finished_at, overall_status, len(unique_items), len(enabled_sources)],
    )
    conn.commit()
    finish_run(run_id=run_id, status=overall_status, conn=conn)
    conn.close()

    stats = {
        "run_id": run_id,
        "status": overall_status,
        "mode": mode,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "item_count": len(unique_items),
        "source_count": len(enabled_sources),
        "sources": source_stats,
    }
    output_dir = _write_exports(run_id, unique_items, stats)
    return run_id, output_dir
