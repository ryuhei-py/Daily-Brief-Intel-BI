CREATE TABLE IF NOT EXISTS fact_run (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status TEXT,
    run_mode TEXT,
    params_json TEXT
);

CREATE TABLE IF NOT EXISTS fact_source_run (
    run_id TEXT,
    source_id TEXT,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status TEXT,
    item_count INTEGER,
    error_class TEXT,
    error_message TEXT,
    http_status INTEGER,
    PRIMARY KEY (run_id, source_id)
);
