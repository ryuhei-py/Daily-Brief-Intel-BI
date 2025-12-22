from __future__ import annotations

from datetime import time
from pathlib import Path
from typing import Any, Tuple

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator


class Settings(BaseModel):
    db_path: Path = Field(default=Path("data/app.duckdb"))
    output_root: Path = Field(default=Path("output/runs"))
    run_time: time = Field(default=time(hour=7, minute=0))

    @model_validator(mode="after")
    def normalize_paths(self) -> "Settings":
        self.db_path = Path(self.db_path)
        self.output_root = Path(self.output_root)
        return self


class Source(BaseModel):
    name: str
    category: str
    enabled: bool = True


class SourcesConfig(BaseModel):
    sources: list[Source]

    @model_validator(mode="after")
    def ensure_sources(self) -> "SourcesConfig":
        if len(self.sources) < 2:
            raise ValueError("At least two sources are required")
        return self


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def load_settings(config_dir: Path) -> Settings:
    path = config_dir / "settings.yml"
    data = _load_yaml(path)
    return Settings(**data)


def load_sources(config_dir: Path) -> SourcesConfig:
    path = config_dir / "sources.yml"
    data = _load_yaml(path)
    return SourcesConfig(**data)


def validate_config(config_dir: Path) -> Tuple[bool, list[str]]:
    results: list[str] = []
    ok = True
    try:
        load_settings(config_dir)
        results.append("settings.yml: ok")
    except FileNotFoundError:
        results.append("settings.yml: missing")
        ok = False
    except ValidationError as exc:
        results.append(f"settings.yml: {exc}")
        ok = False

    try:
        load_sources(config_dir)
        results.append("sources.yml: ok")
    except FileNotFoundError:
        results.append("sources.yml: missing")
        ok = False
    except ValidationError as exc:
        results.append(f"sources.yml: {exc}")
        ok = False
    return ok, results


__all__ = [
    "Settings",
    "Source",
    "SourcesConfig",
    "load_settings",
    "load_sources",
    "validate_config",
]
