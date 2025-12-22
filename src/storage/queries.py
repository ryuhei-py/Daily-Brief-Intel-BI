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
