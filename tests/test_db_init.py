from pathlib import Path

import duckdb

from src.storage.migrate import init_db


def test_init_db_creates_tables(tmp_path: Path):
    db_path = tmp_path / "app.duckdb"
    init_db(db_path=db_path)

    conn = duckdb.connect(str(db_path))
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert {"fact_run", "fact_source_run", "runs", "sources", "items"}.issubset(tables)
