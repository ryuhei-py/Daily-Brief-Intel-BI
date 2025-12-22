from pathlib import Path

import yaml

from src.app.config import validate_config


def write_yaml(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def build_valid_configs(tmp_path: Path) -> Path:
    write_yaml(
        tmp_path / "settings.yml",
        {
            "db_path": str(tmp_path / "data/app.duckdb"),
            "output_root": str(tmp_path / "output"),
            "run_time": "07:00",
        },
    )
    write_yaml(
        tmp_path / "sources.yml",
        {
            "sources": [
                {"name": "tokyo_daily", "category": "jp", "enabled": True},
                {"name": "global_wire", "category": "global", "enabled": True},
            ]
        },
    )
    return tmp_path


def test_validate_config_success(tmp_path: Path):
    config_dir = build_valid_configs(tmp_path)
    ok, messages = validate_config(config_dir)
    assert ok
    assert all("ok" in msg for msg in messages)


def test_validate_config_failure(tmp_path: Path):
    config_dir = build_valid_configs(tmp_path)
    # Break one required field
    write_yaml(
        config_dir / "sources.yml",
        {
            "sources": [
                {"name": "tokyo_daily", "category": "jp", "enabled": True},
            ]
        },
    )
    ok, messages = validate_config(config_dir)
    assert not ok
    assert any("sources.yml" in msg for msg in messages)
