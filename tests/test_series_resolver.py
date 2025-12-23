from pathlib import Path

import duckdb

from src.core.series_resolver import resolve_series_config
from src.storage.migrate import init_db


def test_series_resolver_persistence(tmp_path: Path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    series_yaml = {
        "series": [
            {"key": "ok_passthrough", "resolver": {"type": "passthrough", "value": "headline"}},
            {"key": "bad_type", "resolver": {"type": "nope", "value": "x"}},
            {"key": "missing_value", "resolver": {"type": "passthrough"}},
        ]
    }
    (config_dir / "series.yml").write_text(
        __import__("yaml").safe_dump(series_yaml), encoding="utf-8"
    )

    db_path = tmp_path / "app.duckdb"
    init_db(db_path=db_path)

    conn = duckdb.connect(str(db_path))
    results = resolve_series_config(config_dir, conn)
    assert results["ok_passthrough"]["status"] == "resolved"
    assert results["ok_passthrough"]["resolved_id"] == "headline"
    assert results["bad_type"]["status"] == "unresolved"
    assert results["bad_type"]["resolved_id"] is None
    assert results["missing_value"]["status"] == "unresolved"
    assert results["missing_value"]["resolved_id"] is None
    conn.close()

    # Reopen and ensure rows persisted
    conn = duckdb.connect(str(db_path))
    rows = conn.execute(
        "SELECT series_key, status, resolved_id FROM dim_series_resolution"
    ).fetchall()
    assert len(rows) == 3
    keys = {r[0] for r in rows}
    assert {"ok_passthrough", "bad_type", "missing_value"} == keys
    conn.close()
