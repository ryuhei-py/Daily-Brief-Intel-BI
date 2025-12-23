from __future__ import annotations

from typing import Any, Optional

from duckdb import DuckDBPyConnection

RunDict = dict[str, Any]
ItemDict = dict[str, Any]
CountDict = dict[str, Any]
HealthDict = dict[str, Any]


def get_latest_success_run(conn: DuckDBPyConnection) -> Optional[RunDict]:
    row = conn.execute(
        """
        SELECT
            run_id,
            started_at,
            ended_at,
            status,
            run_mode,
            params_json
        FROM fact_run
        WHERE status = 'success'
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()

    if row is None:
        return None

    return _run_row_to_dict(row)


def get_latest_run(conn: DuckDBPyConnection) -> Optional[RunDict]:
    row = conn.execute(
        """
        SELECT
            run_id,
            started_at,
            ended_at,
            status,
            run_mode,
            params_json
        FROM fact_run
        WHERE status IN ('success', 'partial')
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()

    if row is None:
        return None

    return _run_row_to_dict(row)


def _run_row_to_dict(row: tuple[Any, ...]) -> RunDict:
    return {
        "run_id": row[0],
        "started_at": row[1],
        "ended_at": row[2],
        "status": row[3],
        "run_mode": row[4],
        "params_json": row[5],
    }


def get_items_for_run(
    conn: DuckDBPyConnection,
    run_id: str,
    limit: int = 200,
) -> list[ItemDict]:
    rows = conn.execute(
        """
        SELECT
            source_id,
            source_name,
            title,
            summary,
            url,
            published_at,
            fetched_at
        FROM items
        WHERE run_id = ?
        ORDER BY published_at DESC NULLS LAST, fetched_at DESC
        LIMIT ?
        """,
        [run_id, limit],
    ).fetchall()

    return [
        {
            "source_id": r[0],
            "source_name": r[1],
            "title": r[2],
            "summary": r[3],
            "url": r[4],
            "published_at": r[5],
            "fetched_at": r[6],
        }
        for r in rows
    ]


def get_item_counts_by_source(conn: DuckDBPyConnection, run_id: str) -> list[CountDict]:
    rows = conn.execute(
        """
        SELECT
            source_name,
            COUNT(*) AS count
        FROM items
        WHERE run_id = ?
        GROUP BY source_name
        ORDER BY count DESC
        """,
        [run_id],
    ).fetchall()

    return [{"source_name": r[0], "count": r[1]} for r in rows]


def get_source_health(
    conn: DuckDBPyConnection,
    latest_run_id: str,
    lookback_runs: int = 20,
) -> list[HealthDict]:
    """
    Per-source health metrics across the last N runs (fact_run.started_at DESC).

    Notes:
    - "consecutive_failures" counts failures from the most recent backwards until the first success.
    - source_name is taken from the sources table for latest_run_id (so UI uses current names).
    """
    rows = conn.execute(
        """
        WITH last_runs AS (
            SELECT run_id
            FROM fact_run
            WHERE started_at IS NOT NULL
            ORDER BY started_at DESC
            LIMIT ?
        ),
        sr AS (
            SELECT
                fr.run_id,
                fsr.source_id,
                fsr.started_at,
                fsr.ended_at,
                fsr.status,
                fsr.item_count,
                fsr.error_class,
                fsr.error_message,
                fsr.http_status
            FROM fact_source_run AS fsr
            JOIN last_runs AS fr
                ON fsr.run_id = fr.run_id
        ),
        ordered AS (
            SELECT
                run_id,
                source_id,
                started_at,
                ended_at,
                status,
                item_count,
                error_class,
                error_message,
                http_status,
                ROW_NUMBER() OVER (
                    PARTITION BY source_id
                    ORDER BY started_at DESC
                ) AS rn,
                SUM(
                    CASE WHEN status = 'success' THEN 1 ELSE 0 END
                ) OVER (
                    PARTITION BY source_id
                    ORDER BY started_at DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS success_seen,
                CASE
                    WHEN started_at IS NOT NULL AND ended_at IS NOT NULL
                        THEN DATEDIFF('second', started_at, ended_at)
                    ELSE NULL
                END AS duration_seconds
            FROM sr
        ),
        agg AS (
            SELECT
                source_id,
                COUNT(*) AS runs,
                SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN status != 'success' THEN 1 ELSE 0 END) AS fail_count,
                AVG(duration_seconds) AS avg_duration_seconds,
                MAX(CASE WHEN status = 'success' THEN ended_at END) AS last_success_at,
                MAX(CASE WHEN status != 'success' THEN ended_at END) AS last_failure_at
            FROM ordered
            GROUP BY source_id
        ),
        consec AS (
            SELECT
                source_id,
                SUM(
                    CASE
                        WHEN status != 'success' AND success_seen = 0
                            THEN 1
                        ELSE 0
                    END
                ) AS consecutive_failures
            FROM ordered
            GROUP BY source_id
        ),
        last AS (
            SELECT
                source_id,
                status AS last_status,
                ended_at AS last_ended_at,
                item_count AS last_item_count,
                http_status AS last_http_status,
                error_class AS last_error_class,
                error_message AS last_error_message
            FROM ordered
            WHERE rn = 1
        )
        SELECT
            COALESCE(s.source_name, l.source_id) AS source_name,
            l.source_id,
            a.runs,
            a.success_count,
            a.fail_count,
            CASE
                WHEN a.runs > 0 THEN (a.success_count * 100.0) / a.runs
                ELSE NULL
            END AS success_rate,
            c.consecutive_failures,
            a.avg_duration_seconds,
            l.last_status,
            l.last_ended_at,
            l.last_item_count,
            l.last_http_status,
            l.last_error_class,
            l.last_error_message,
            a.last_success_at,
            a.last_failure_at
        FROM agg AS a
        JOIN last AS l
            ON a.source_id = l.source_id
        LEFT JOIN consec AS c
            ON a.source_id = c.source_id
        LEFT JOIN sources AS s
            ON s.run_id = ? AND s.source_id = a.source_id
        ORDER BY
            c.consecutive_failures DESC,
            a.fail_count DESC,
            a.source_id
        """,
        [lookback_runs, latest_run_id],
    ).fetchall()

    return [
        {
            "source_name": r[0],
            "source_id": r[1],
            "runs": r[2],
            "success_count": r[3],
            "fail_count": r[4],
            "success_rate": r[5],
            "consecutive_failures": r[6],
            "avg_duration_seconds": r[7],
            "last_status": r[8],
            "last_ended_at": r[9],
            "last_item_count": r[10],
            "last_http_status": r[11],
            "last_error_class": r[12],
            "last_error_message": r[13],
            "last_success_at": r[14],
            "last_failure_at": r[15],
        }
        for r in rows
    ]
