from typing import Optional

from duckdb import DuckDBPyConnection


def get_latest_success_run(conn: DuckDBPyConnection) -> Optional[dict]:
    result = conn.execute(
        """
        SELECT run_id, started_at, ended_at, status, run_mode, params_json
        FROM fact_run
        WHERE status = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()
    if result:
        return {
            "run_id": result[0],
            "started_at": result[1],
            "ended_at": result[2],
            "status": result[3],
            "run_mode": result[4],
            "params_json": result[5],
        }
    return None


def get_latest_run(conn: DuckDBPyConnection) -> Optional[dict]:
    result = conn.execute(
        """
        SELECT run_id, started_at, ended_at, status, run_mode, params_json
        FROM fact_run
        WHERE status IN ('success', 'partial')
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()
    if result:
        return {
            "run_id": result[0],
            "started_at": result[1],
            "ended_at": result[2],
            "status": result[3],
            "run_mode": result[4],
            "params_json": result[5],
        }
    return None


def get_items_for_run(conn: DuckDBPyConnection, run_id: str, limit: int = 200) -> list[dict]:
    rows = conn.execute(
        """
        SELECT source_id, source_name, title, summary, url, published_at, fetched_at
        FROM items
        WHERE run_id = ?
        ORDER BY published_at DESC NULLS LAST, fetched_at DESC
        LIMIT ?
        """,
        [run_id, limit],
    ).fetchall()
    return [
        {
            "source_id": row[0],
            "source_name": row[1],
            "title": row[2],
            "summary": row[3],
            "url": row[4],
            "published_at": row[5],
            "fetched_at": row[6],
        }
        for row in rows
    ]


def get_item_counts_by_source(conn: DuckDBPyConnection, run_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT source_name, COUNT(*) as count
        FROM items
        WHERE run_id = ?
        GROUP BY source_name
        ORDER BY count DESC
        """,
        [run_id],
    ).fetchall()
    return [{"source_name": row[0], "count": row[1]} for row in rows]
