from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from duckdb import DuckDBPyConnection

from src.core.logging import get_logger
from src.storage.db import connect

logger = get_logger(__name__)


def create_run(
    run_mode: str,
    params_json: Optional[str] = None,
    conn: Optional[DuckDBPyConnection] = None,
) -> tuple[str, datetime]:
    connection = conn or connect()
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc)
    connection.execute(
        """
        INSERT INTO fact_run (run_id, started_at, ended_at, status, run_mode, params_json)
        VALUES (?, ?, NULL, ?, ?, ?)
        """,
        [run_id, started_at, "running", run_mode, params_json or "{}"],
    )
    connection.commit()
    logger.info("Created run %s mode=%s", run_id, run_mode)
    return run_id, started_at


def finish_run(
    run_id: str, status: str, conn: Optional[DuckDBPyConnection] = None
) -> datetime:
    connection = conn or connect()
    ended_at = datetime.now(timezone.utc)
    connection.execute(
        """
        UPDATE fact_run SET ended_at = ?, status = ?
        WHERE run_id = ?
        """,
        [ended_at, status, run_id],
    )
    connection.commit()
    logger.info("Finished run %s status=%s", run_id, status)
    return ended_at


def record_source_run(
    run_id: str,
    source_id: str,
    started_at: datetime,
    ended_at: datetime,
    status: str,
    item_count: int,
    error_class: Optional[str] = None,
    error_message: Optional[str] = None,
    http_status: Optional[int] = None,
    conn: Optional[DuckDBPyConnection] = None,
) -> None:
    connection = conn or connect()
    connection.execute(
        """
        INSERT INTO fact_source_run (
            run_id, source_id, started_at, ended_at, status, item_count,
            error_class, error_message, http_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            source_id,
            started_at,
            ended_at,
            status,
            item_count,
            error_class,
            error_message,
            http_status,
        ],
    )
    connection.commit()
    logger.info("Recorded source run %s for %s status=%s", run_id, source_id, status)
