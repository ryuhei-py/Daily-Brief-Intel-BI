import importlib
from pathlib import Path

import duckdb
import yaml

import src.app.pipeline as pipeline


def _write_sources(tmp_path: Path, source_url: str) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    sources = {
        "sources": [
            {
                "id": "rss1",
                "name": "RSS Source",
                "category": "jp",
                "kind": "rss",
                "url": source_url,
                "enabled": True,
            }
        ]
    }
    (config_dir / "sources.yml").write_text(yaml.safe_dump(sources), encoding="utf-8")
    return config_dir


def _fake_fetch_factory(content: str):
    def _fake_fetch(url: str, allowed_urls):
        assert any(str(url).startswith(prefix) for prefix in allowed_urls)
        return content

    return _fake_fetch


def test_run_id_collision_suffix(tmp_path: Path, monkeypatch):
    config_dir = _write_sources(tmp_path, "https://example.com/rss")
    rss_text = Path("tests/fixtures/rss_sample.xml").read_text(encoding="utf-8")
    fake_fetch = _fake_fetch_factory(rss_text)

    db_path = tmp_path / "app.duckdb"
    monkeypatch.setenv("APP_DB_PATH", str(db_path))
    monkeypatch.setenv("APP_OUTPUT_ROOT", str(tmp_path / "output/runs"))

    importlib.reload(pipeline)

    run_id1, _ = pipeline.run_pipeline(
        config_dir=config_dir,
        mode="manual",
        fetcher=fake_fetch,
        run_id="demo-run",
        overwrite_run=False,
    )
    run_id2, _ = pipeline.run_pipeline(
        config_dir=config_dir,
        mode="manual",
        fetcher=fake_fetch,
        run_id="demo-run",
        overwrite_run=False,
    )

    assert run_id1 == "demo-run"
    assert run_id2 == "demo-run-02"

    conn = duckdb.connect(str(db_path))
    run_ids = {row[0] for row in conn.execute("SELECT run_id FROM runs").fetchall()}
    assert run_ids == {"demo-run", "demo-run-02"}


def test_run_id_overwrite(tmp_path: Path, monkeypatch):
    config_dir = _write_sources(tmp_path, "https://example.com/rss")
    rss_text = Path("tests/fixtures/rss_sample.xml").read_text(encoding="utf-8")
    fake_fetch = _fake_fetch_factory(rss_text)

    db_path = tmp_path / "app.duckdb"
    monkeypatch.setenv("APP_DB_PATH", str(db_path))
    monkeypatch.setenv("APP_OUTPUT_ROOT", str(tmp_path / "output/runs"))

    importlib.reload(pipeline)

    run_id1, _ = pipeline.run_pipeline(
        config_dir=config_dir,
        mode="manual",
        fetcher=fake_fetch,
        run_id="demo-overwrite",
        overwrite_run=False,
    )
    run_id2, _ = pipeline.run_pipeline(
        config_dir=config_dir,
        mode="manual",
        fetcher=fake_fetch,
        run_id="demo-overwrite",
        overwrite_run=True,
    )

    assert run_id1 == "demo-overwrite"
    assert run_id2 == "demo-overwrite"

    conn = duckdb.connect(str(db_path))
    rows = conn.execute("SELECT run_id, COUNT(*) FROM runs GROUP BY run_id").fetchall()
    assert rows == [("demo-overwrite", 1)]

    # Items should have been rewritten for the same run_id
    item_count = conn.execute(
        "SELECT COUNT(*) FROM items WHERE run_id = 'demo-overwrite'"
    ).fetchone()[0]
    assert item_count >= 2
