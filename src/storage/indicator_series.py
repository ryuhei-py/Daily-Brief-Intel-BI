from __future__ import annotations

from typing import Iterable

from duckdb import DuckDBPyConnection


def upsert_dim_indicator_series(conn: DuckDBPyConnection, rows: Iterable[dict]) -> None:
    payload = [
        (
            row["series_key"],
            row["resolved_id"],
            row.get("resolver_type"),
            row.get("resolver_value"),
            row.get("message"),
        )
        for row in rows
    ]
    if not payload:
        return
    conn.executemany(
        """
        INSERT INTO dim_indicator_series (
            series_key, resolved_id, resolver_type, resolver_value, message
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(series_key) DO UPDATE SET
            resolved_id = excluded.resolved_id,
            resolver_type = excluded.resolver_type,
            resolver_value = excluded.resolver_value,
            message = excluded.message,
        """,
        payload,
    )
    conn.commit()


def upsert_fact_indicator_series_run(
    conn: DuckDBPyConnection, run_id: str, rows: Iterable[dict]
) -> None:
    payload = [
        (
            run_id,
            row["series_key"],
            row.get("resolved_id"),
            row.get("status"),
            row.get("message"),
        )
        for row in rows
    ]
    if not payload:
        return
    conn.executemany(
        """
        INSERT INTO fact_indicator_series_run (
            run_id, series_key, resolved_id, status, message
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(run_id, series_key) DO UPDATE SET
            resolved_id = excluded.resolved_id,
            status = excluded.status,
            message = excluded.message,
        """,
        payload,
    )
    conn.commit()


def delete_indicator_series_for_run(conn: DuckDBPyConnection, run_id: str) -> None:
    conn.execute("DELETE FROM fact_indicator_series_run WHERE run_id = ?", [run_id])
    conn.commit()
