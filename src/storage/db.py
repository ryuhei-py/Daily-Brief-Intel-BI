import os
from pathlib import Path
from typing import Optional

import duckdb

DEFAULT_DB_PATH = Path("output/db/app.duckdb")


def get_db_path() -> Path:
    override = os.getenv("APP_DB_PATH")
    if override:
        path = Path(override)
    else:
        path = DEFAULT_DB_PATH
    return path


def connect(db_path: Optional[Path] = None) -> duckdb.DuckDBPyConnection:
    path = Path(db_path) if db_path else get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))
