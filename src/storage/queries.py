from typing import Optional

from duckdb import DuckDBPyConnection


def get_latest_success_run(conn: DuckDBPyConnection) -> Optional[dict]:
    try:
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
    except Exception:
        result = None

    if result:
        return result

    try:
        newer = conn.execute(
            """
            SELECT run_id, started_at, finished_at as ended_at, status
            FROM runs
            WHERE status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
            """
        ).fetchone()
        if newer:
            return {
                "run_id": newer[0],
                "started_at": newer[1],
                "ended_at": newer[2],
                "status": newer[3],
                "run_mode": None,
                "params_json": None,
            }
    except Exception:
        return None
    return None
