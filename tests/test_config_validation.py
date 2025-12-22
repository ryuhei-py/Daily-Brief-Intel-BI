from pathlib import Path

import yaml

from src.core.config_loader import validate_config_dir


def write_yaml(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def build_valid_configs(tmp_path: Path) -> Path:
    write_yaml(
        tmp_path / "sources.yml",
        {
            "sources": [
                {
                    "source_id": "source_a",
                    "kind": "rss",
                    "enabled": True,
                    "content_level": "summary",
                    "rate_limit": {"requests_per_minute": 10},
                    "compliance": {"gdpr": True},
                }
            ]
        },
    )
    write_yaml(
        tmp_path / "watchlist.yml",
        {
            "watchlist_policy": {"limit_enabled": False},
            "matching_policy": {"match_order": "linear"},
            "watch_entities": [],
        },
    )
    write_yaml(
        tmp_path / "geo.yml",
        {
            "geo_rollups": {
                "tokyo_metro": ["Tokyo", "Kanagawa", "Chiba", "Saitama"],
            }
        },
    )
    write_yaml(tmp_path / "schedule.yml", {"daily_time_jst": "07:00"})
    write_yaml(
        tmp_path / "series.yml",
        {
            "series": [
                {"key": "headline_series", "resolver": {"type": "passthrough", "value": "title"}}
            ]
        },
    )
    write_yaml(
        tmp_path / "scoring.yml",
        {"default_score": 0.0, "rules": [{"name": "base", "weight": 1.0}]},
    )
    write_yaml(
        tmp_path / "alerts.yml",
        {"enabled": True, "channels": [{"channel": "email", "target": "ops@example.com"}]},
    )
    return tmp_path


def test_validate_config_success(tmp_path: Path):
    config_dir = build_valid_configs(tmp_path)
    ok, messages = validate_config_dir(config_dir)
    assert ok
    assert all("ok" in msg for msg in messages)


def test_validate_config_failure(tmp_path: Path):
    config_dir = build_valid_configs(tmp_path)
    # Break one required field
    write_yaml(
        config_dir / "sources.yml",
        {
            "sources": [
                {
                    "kind": "rss",
                    "enabled": True,
                    "content_level": "summary",
                    "rate_limit": {"requests_per_minute": 10},
                    "compliance": {"gdpr": True},
                }
            ]
        },
    )
    ok, messages = validate_config_dir(config_dir)
    assert not ok
    assert any("sources.yml" in msg for msg in messages)
