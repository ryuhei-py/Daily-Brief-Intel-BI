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

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT,
    item_count INTEGER,
    source_count INTEGER
);

CREATE TABLE IF NOT EXISTS sources (
    run_id TEXT,
    source_id TEXT,
    source_name TEXT,
    category TEXT,
    kind TEXT,
    enabled BOOLEAN,
    PRIMARY KEY (run_id, source_id)
);

CREATE TABLE IF NOT EXISTS items (
    run_id TEXT,
    source_id TEXT,
    source_name TEXT,
    category TEXT,
    kind TEXT,
    title TEXT,
    summary TEXT,
    url TEXT,
    published_at TIMESTAMP,
    fetched_at TIMESTAMP,
    PRIMARY KEY (run_id, source_id, url)
);

CREATE TABLE IF NOT EXISTS alerts (
    run_id TEXT,
    alert_type TEXT,
    message TEXT
);

CREATE TABLE IF NOT EXISTS dim_series_resolution (
    series_key TEXT PRIMARY KEY,
    resolver_type TEXT,
    resolver_value TEXT,
    resolved_id TEXT,
    status TEXT,
    message TEXT,
    updated_at TIMESTAMP
);

-- Indicator series registry (materialized resolved series)
CREATE TABLE IF NOT EXISTS dim_indicator_series (
    series_key TEXT PRIMARY KEY,
    resolved_id TEXT NOT NULL,
    resolver_type TEXT,
    resolver_value TEXT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Per-run audit snapshot of series resolution
CREATE TABLE IF NOT EXISTS fact_indicator_series_run (
    run_id TEXT NOT NULL,
    series_key TEXT NOT NULL,
    resolved_id TEXT,
    status TEXT NOT NULL,
    message TEXT,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, series_key)
);
