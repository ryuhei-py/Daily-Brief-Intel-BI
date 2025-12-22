from pathlib import Path
from typing import Optional

from .db import connect, get_db_path

SCHEMA_FILE = Path(__file__).with_name("schema.sql")


def apply_schema(conn, schema_path: Path) -> None:
    with schema_path.open("r", encoding="utf-8") as file:
        sql = file.read()
    conn.execute(sql)
    conn.commit()


def init_db(db_path: Optional[Path] = None, schema_path: Optional[Path] = None) -> Path:
    target_path = Path(db_path) if db_path else get_db_path()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    schema = Path(schema_path) if schema_path else SCHEMA_FILE
    conn = connect(target_path)
    apply_schema(conn, schema)
    conn.close()
    return target_path
