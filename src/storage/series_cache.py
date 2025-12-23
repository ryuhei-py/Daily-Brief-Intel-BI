from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from duckdb import DuckDBPyConnection


def upsert_series_resolution(
    conn: DuckDBPyConnection,
    series_key: str,
    resolver_type: str,
    resolver_value: Optional[str],
    resolved_id: Optional[str],
    status: str,
    message: Optional[str],
) -> None:
    conn.execute("DELETE FROM dim_series_resolution WHERE series_key = ?", [series_key])
    conn.execute(
        """
        INSERT INTO dim_series_resolution (
            series_key, resolver_type, resolver_value, resolved_id, status, message, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            series_key,
            resolver_type,
            resolver_value,
            resolved_id,
            status,
            message,
            datetime.now(timezone.utc),
        ],
    )
    conn.commit()


def get_series_resolution(conn: DuckDBPyConnection, series_key: str) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT series_key, resolver_type, resolver_value, resolved_id, status, message, updated_at
        FROM dim_series_resolution
        WHERE series_key = ?
        """,
        [series_key],
    ).fetchone()
    if not row:
        return None
    return {
        "series_key": row[0],
        "resolver_type": row[1],
        "resolver_value": row[2],
        "resolved_id": row[3],
        "status": row[4],
        "message": row[5],
        "updated_at": row[6],
    }


def list_series_resolutions(conn: DuckDBPyConnection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT series_key, resolver_type, resolver_value, resolved_id, status, message, updated_at
        FROM dim_series_resolution
        ORDER BY series_key
        """
    ).fetchall()
    return [
        {
            "series_key": row[0],
            "resolver_type": row[1],
            "resolver_value": row[2],
            "resolved_id": row[3],
            "status": row[4],
            "message": row[5],
            "updated_at": row[6],
        }
        for row in rows
    ]
