from pathlib import Path

import duckdb
import yaml

from src.app.pipeline import run_pipeline
from src.storage.migrate import init_db


def test_indicator_series_ingest_fact_and_dim(tmp_path: Path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    series_yaml = {
        "series": [
            {"key": "ok_passthrough", "resolver": {"type": "passthrough", "value": "headline"}},
            {"key": "bad_type", "resolver": {"type": "nope", "value": "x"}},
            {"key": "missing_value", "resolver": {"type": "passthrough"}},
        ]
    }
    (config_dir / "series.yml").write_text(yaml.safe_dump(series_yaml), encoding="utf-8")

    sources_yaml = {
        "sources": [
            {
                "id": "rss1",
                "name": "RSS Source",
                "category": "jp",
                "kind": "rss",
                "url": "https://example.com/rss",
                "enabled": False,
            }
        ]
    }
    (config_dir / "sources.yml").write_text(yaml.safe_dump(sources_yaml), encoding="utf-8")

    db_path = tmp_path / "app.duckdb"
    monkeypatch.setenv("APP_DB_PATH", str(db_path))
    monkeypatch.setenv("APP_OUTPUT_ROOT", str(tmp_path / "output"))
    monkeypatch.setenv("RUN_ID", "run-series-1")

    def fake_fetch(url: str, allowed_urls):
        return ""

    init_db(db_path=db_path)
    run_id, _ = run_pipeline(
        config_dir=config_dir,
        mode="manual",
        fetcher=fake_fetch,
        overwrite_run=True,
    )

    conn = duckdb.connect(str(db_path))
    latest_run = conn.execute(
        "SELECT run_id FROM runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()[0]
    assert latest_run == run_id

    fact_count = conn.execute(
        "SELECT COUNT(*) FROM fact_indicator_series_run WHERE run_id = ?", [run_id]
    ).fetchone()[0]
    assert fact_count == 3

    dim_rows = conn.execute(
        "SELECT series_key, resolved_id FROM dim_indicator_series ORDER BY series_key"
    ).fetchall()
    assert dim_rows == [("ok_passthrough", "headline")]
    conn.close()


def test_series_ingest_skips_when_missing_file(tmp_path: Path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    sources_yaml = {
        "sources": [
            {
                "id": "rss1",
                "name": "RSS Source",
                "category": "jp",
                "kind": "rss",
                "url": "https://example.com/rss",
                "enabled": False,
            }
        ]
    }
    (config_dir / "sources.yml").write_text(yaml.safe_dump(sources_yaml), encoding="utf-8")

    db_path = tmp_path / "app.duckdb"
    monkeypatch.setenv("APP_DB_PATH", str(db_path))
    monkeypatch.setenv("APP_OUTPUT_ROOT", str(tmp_path / "output"))
    monkeypatch.setenv("RUN_ID", "run-series-2")

    def fake_fetch(url: str, allowed_urls):
        return ""

    init_db(db_path=db_path)
    run_id, _ = run_pipeline(
        config_dir=config_dir,
        mode="manual",
        fetcher=fake_fetch,
        overwrite_run=True,
    )

    conn = duckdb.connect(str(db_path))
    count = conn.execute(
        "SELECT COUNT(*) FROM fact_indicator_series_run WHERE run_id = ?", [run_id]
    ).fetchone()[0]
    assert count == 0
    conn.close()
