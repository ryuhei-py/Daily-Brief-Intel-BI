from __future__ import annotations

import csv
import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Tuple
from uuid import uuid4

from src.app.config import Settings, SourcesConfig, load_settings, load_sources
from src.app.db import ensure_schema, get_connection
from src.core.logging import get_logger

logger = get_logger(__name__)


def generate_run_id() -> str:
    return os.getenv("RUN_ID") or str(uuid4())


def _write_exports(output_dir: Path, items: Iterable[dict], alerts: list, stats: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    items_path = output_dir / "brief_items.csv"
    alerts_path = output_dir / "alerts.json"
    stats_path = output_dir / "run_stats.json"

    items_list = list(items)
    with items_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file, fieldnames=["title", "summary", "url", "source", "published_at", "fetched_at"]
        )
        writer.writeheader()
        for row in items_list:
            writer.writerow(
                {
                    "title": row["title"],
                    "summary": row["summary"],
                    "url": row["url"],
                    "source": row["source_name"],
                    "published_at": row["published_at"].isoformat(),
                    "fetched_at": row["fetched_at"].isoformat(),
                }
            )

    alerts_path.write_text(json.dumps(alerts, indent=2), encoding="utf-8")
    stats_path.write_text(json.dumps(stats, indent=2, default=str), encoding="utf-8")


def _generate_stub_items(run_id: str, sources: List[dict]) -> list[dict]:
    rnd = random.Random(run_id)
    now = datetime.now(timezone.utc)
    items: list[dict] = []
    enabled_sources = [s for s in sources if s["enabled"]]
    if not enabled_sources:
        raise ValueError("No enabled sources configured.")
    for i in range(10):
        src = enabled_sources[i % len(enabled_sources)]
        published_at = now - timedelta(minutes=i)
        items.append(
            {
                "run_id": run_id,
                "source_name": src["name"],
                "title": f"[{src['name']}] Item {i + 1}",
                "summary": f"Summary for item {i + 1} from {src['name']}",
                "url": f"https://example.com/{src['name']}/{i + 1}",
                "published_at": published_at,
                "fetched_at": now,
            }
        )
    rnd.shuffle(items)
    return items


def run_pipeline(config_dir: Path | str = "config", mode: str = "manual") -> Tuple[str, Path]:
    config_path = Path(config_dir)
    settings: Settings = load_settings(config_path)
    sources_config: SourcesConfig = load_sources(config_path)

    run_id = generate_run_id()
    started_at = datetime.now(timezone.utc)
    items = _generate_stub_items(
        run_id,
        [
            {"name": s.name, "category": s.category, "enabled": s.enabled}
            for s in sources_config.sources
        ],
    )
    alerts: list = []

    conn = get_connection(settings)
    ensure_schema(conn)

    # Idempotency for testing: clear any existing run rows
    conn.execute("DELETE FROM runs WHERE run_id = ?", [run_id])
    conn.execute("DELETE FROM sources WHERE run_id = ?", [run_id])
    conn.execute("DELETE FROM items WHERE run_id = ?", [run_id])
    conn.execute("DELETE FROM alerts WHERE run_id = ?", [run_id])

    for src in sources_config.sources:
        conn.execute(
            """
            INSERT INTO sources (run_id, source_name, category, enabled)
            VALUES (?, ?, ?, ?)
            """,
            [run_id, src.name, src.category, src.enabled],
        )

    for item in items:
        conn.execute(
            """
            INSERT INTO items (run_id, source_name, title, summary, url, published_at, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                item["run_id"],
                item["source_name"],
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
        [run_id, started_at, finished_at, "success", len(items), len(sources_config.sources)],
    )
    conn.commit()
    conn.close()

    output_dir = settings.output_root / run_id
    stats = {
        "run_id": run_id,
        "mode": mode,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "status": "success",
        "item_count": len(items),
        "source_count": len(sources_config.sources),
    }
    _write_exports(output_dir, items, alerts, stats)
    logger.info(
        "Run %s completed; items=%s sources=%s", run_id, len(items), len(sources_config.sources)
    )
    return run_id, output_dir
