from __future__ import annotations

from pathlib import Path

import duckdb

from src.app.config import Settings

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT,
    item_count INTEGER,
    source_count INTEGER
);

CREATE TABLE IF NOT EXISTS sources (
    run_id TEXT,
    source_name TEXT,
    category TEXT,
    enabled BOOLEAN,
    PRIMARY KEY (run_id, source_name)
);

CREATE TABLE IF NOT EXISTS items (
    run_id TEXT,
    source_name TEXT,
    title TEXT,
    summary TEXT,
    url TEXT,
    published_at TIMESTAMP,
    fetched_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alerts (
    run_id TEXT,
    alert_type TEXT,
    message TEXT
);
"""


def get_connection(settings: Settings):
    db_path: Path = settings.db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def ensure_schema(conn) -> None:
    conn.execute(SCHEMA_SQL)
    conn.commit()


def init_db(settings: Settings) -> Path:
    conn = get_connection(settings)
    ensure_schema(conn)
    conn.close()
    return settings.db_path
