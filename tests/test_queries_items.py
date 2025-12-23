from pathlib import Path

import duckdb

from src.storage import queries
from src.storage.migrate import apply_schema


def _apply_schema(conn):
    schema_path = Path(__file__).resolve().parents[1] / "src" / "storage" / "schema.sql"
    apply_schema(conn, schema_path)


def test_latest_run_and_items_ordering(tmp_path):
    conn = duckdb.connect(":memory:")
    _apply_schema(conn)
    conn.execute(
        """
        INSERT INTO fact_run (run_id, started_at, ended_at, status, run_mode, params_json)
        VALUES ('r1', '2024-01-01', '2024-01-01', 'success', 'manual', '{}')
        """
    )
    conn.execute(
        """
        INSERT INTO items (
            run_id,
            source_id,
            source_name,
            category,
            kind,
            title,
            summary,
            url,
            published_at,
            fetched_at
        )
        VALUES
            (
                'r1','s1','Source A','jp','rss','Old','Old summary',
                'https://a', '2024-01-01', '2024-01-01T00:00:00Z'
            ),
            (
                'r1','s1','Source A','jp','rss','New','New summary',
                'https://b', '2024-01-02', '2024-01-02T00:00:00Z'
            )
        """
    )

    latest = queries.get_latest_run(conn)
    assert latest["run_id"] == "r1"

    items = queries.get_items_for_run(conn, "r1", limit=200)
    assert len(items) == 2
    assert items[0]["title"] == "New"
    assert items[1]["title"] == "Old"

    counts = queries.get_item_counts_by_source(conn, "r1")
    assert counts == [{"source_name": "Source A", "count": 2}]
