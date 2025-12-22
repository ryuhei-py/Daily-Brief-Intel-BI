from pathlib import Path

import duckdb

from src.app.config import Settings
from src.app.db import init_db


def test_init_db_creates_tables(tmp_path: Path):
    db_path = tmp_path / "app.duckdb"
    settings = Settings(db_path=db_path, output_root=tmp_path / "output")
    init_db(settings)

    conn = duckdb.connect(str(db_path))
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert "runs" in tables
    assert "items" in tables
    assert "sources" in tables
