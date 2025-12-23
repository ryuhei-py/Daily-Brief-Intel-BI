import importlib
from pathlib import Path

import duckdb
import yaml


def test_pipeline_ingests_and_exports(tmp_path: Path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    sources = {
        "sources": [
            {
                "id": "rss1",
                "name": "RSS Source",
                "category": "jp",
                "kind": "rss",
                "url": "https://example.com/rss",
                "enabled": True,
            },
            {
                "id": "estat1",
                "name": "eStat",
                "category": "jp",
                "kind": "estat_api",
                "url": "https://api.example.com/estat",
                "params": {"dataset_id": "123", "table_id": "1"},
                "enabled": True,
            },
        ]
    }
    (config_dir / "sources.yml").write_text(yaml.safe_dump(sources), encoding="utf-8")

    # Fixtures
    rss_text = (Path("tests/fixtures/rss_sample.xml")).read_text(encoding="utf-8")
    estat_text = (Path("tests/fixtures/estat_sample.json")).read_text(encoding="utf-8")

    def fake_fetch(url: str, allowed_urls):
        assert any(str(url).startswith(prefix) for prefix in allowed_urls)
        if "rss" in url:
            return rss_text
        return estat_text

    db_path = tmp_path / "app.duckdb"
    monkeypatch.setenv("APP_DB_PATH", str(db_path))
    monkeypatch.setenv("APP_OUTPUT_ROOT", str(tmp_path / "output/runs"))
    monkeypatch.setenv("RUN_ID", "test-run-123")

    import src.app.pipeline as pipeline

    importlib.reload(pipeline)

    run_id, output_dir = pipeline.run_pipeline(
        config_dir=config_dir, mode="manual", fetcher=fake_fetch
    )
    assert run_id == "test-run-123"
    assert output_dir.exists()

    # DB assertions
    conn = duckdb.connect(str(db_path))
    item_count = conn.execute("SELECT COUNT(*) FROM items WHERE run_id = ?", [run_id]).fetchone()[0]
    assert item_count >= 2

    fact_run = conn.execute(
        "SELECT run_id, started_at, ended_at, status FROM fact_run WHERE run_id = ?", [run_id]
    ).fetchone()
    assert fact_run is not None
    assert fact_run[1] is not None and fact_run[2] is not None

    source_rows = conn.execute(
        "SELECT source_id, started_at, ended_at FROM fact_source_run WHERE run_id = ?", [run_id]
    ).fetchall()
    assert source_rows
    for _, start, end in source_rows:
        assert start is not None and end is not None
        assert start <= end

    # Export assertions
    assert (output_dir / "brief_items.csv").exists()
    assert (output_dir / "run_stats.json").exists()
    assert (output_dir / "alerts.json").exists()
    csv_content = (output_dir / "brief_items.csv").read_text(encoding="utf-8").strip()
    assert "Item One" in csv_content


def test_pipeline_dedupes_items_across_runs_by_url(tmp_path: Path, monkeypatch):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    sources = {
        "sources": [
            {
                "id": "rss1",
                "name": "RSS Source",
                "category": "jp",
                "kind": "rss",
                "url": "https://example.com/rss",
                "enabled": True,
            }
        ]
    }
    (config_dir / "sources.yml").write_text(yaml.safe_dump(sources), encoding="utf-8")

    rss_text = (Path("tests/fixtures/rss_sample.xml")).read_text(encoding="utf-8")

    def fake_fetch(url: str, allowed_urls):
        assert any(str(url).startswith(prefix) for prefix in allowed_urls)
        return rss_text

    db_path = tmp_path / "app.duckdb"
    monkeypatch.setenv("APP_DB_PATH", str(db_path))
    monkeypatch.setenv("APP_OUTPUT_ROOT", str(tmp_path / "output/runs"))

    import src.app.pipeline as pipeline

    importlib.reload(pipeline)

    monkeypatch.setenv("RUN_ID", "run-1")
    pipeline.run_pipeline(config_dir=config_dir, mode="manual", fetcher=fake_fetch)
    conn = duckdb.connect(str(db_path))
    count1 = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]

    monkeypatch.setenv("RUN_ID", "run-2")
    pipeline.run_pipeline(config_dir=config_dir, mode="manual", fetcher=fake_fetch)
    count2 = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]

    assert count2 == count1
    dup_count = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT source_id, url, COUNT(*) c
            FROM items
            GROUP BY source_id, url
            HAVING c > 1
        )
        """
    ).fetchone()[0]
    assert dup_count == 0
