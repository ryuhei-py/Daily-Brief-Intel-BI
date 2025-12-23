from pathlib import Path
from typing import Optional

from .db import connect, get_db_path

SCHEMA_FILE = Path(__file__).with_name("schema.sql")


def _table_exists(conn, table_name: str) -> bool:
    result = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return bool(result)


def _dedupe_items_by_source_url(conn) -> None:
    if not _table_exists(conn, "items"):
        return
    conn.execute(
        """
        CREATE TEMP TABLE _items_rank AS
        SELECT
            run_id,
            source_id,
            url,
            ROW_NUMBER() OVER (
                PARTITION BY source_id, url
                ORDER BY fetched_at DESC NULLS LAST, published_at DESC NULLS LAST, run_id DESC
            ) AS rn
        FROM items
        """
    )
    conn.execute(
        """
        DELETE FROM items
        WHERE (run_id, source_id, url) IN (
            SELECT run_id, source_id, url
            FROM _items_rank
            WHERE rn > 1
        )
        """
    )
    conn.execute("DROP TABLE _items_rank")
    conn.commit()


def _ensure_unique_items_index(conn) -> None:
    if not _table_exists(conn, "items"):
        return
    try:
        _dedupe_items_by_source_url(conn)
        conn.execute(
            """
            CREATE UNIQUE INDEX idx_items_source_url
            ON items(source_id, url)
            """
        )
        conn.commit()
    except Exception:
        try:
            _dedupe_items_by_source_url(conn)
            conn.execute(
                """
                CREATE UNIQUE INDEX idx_items_source_url
                ON items(source_id, url)
                """
            )
            conn.commit()
        except Exception:
            pass


def apply_schema(conn, schema_path: Path) -> None:
    with schema_path.open("r", encoding="utf-8") as file:
        sql = file.read()
    conn.execute(sql)
    conn.commit()
    _dedupe_items_by_source_url(conn)
    _ensure_unique_items_index(conn)


def init_db(db_path: Optional[Path] = None, schema_path: Optional[Path] = None) -> Path:
    target_path = Path(db_path) if db_path else get_db_path()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    schema = Path(schema_path) if schema_path else SCHEMA_FILE
    conn = connect(target_path)
    apply_schema(conn, schema)
    conn.close()
    return target_path
