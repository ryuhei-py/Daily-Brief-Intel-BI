from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml
from pydantic import ValidationError

from .config_schema import CONFIG_MODEL_MAP, SourcesConfig
from .logging import get_logger

logger = get_logger(__name__)


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        content = yaml.safe_load(file) or {}
    return content


def validate_config_file(path: Path, model_cls) -> Tuple[bool, str]:
    try:
        data = load_yaml(path)
        model_cls(**data)
        return True, f"{path.name}: ok"
    except FileNotFoundError:
        return False, f"{path.name}: missing"
    except ValidationError as exc:
        return False, f"{path.name}: {exc}"
    except yaml.YAMLError as exc:
        return False, f"{path.name}: invalid yaml ({exc})"


def validate_config_dir(config_dir: Path) -> Tuple[bool, list[str]]:
    results: list[str] = []
    success = True
    for filename, model_cls in CONFIG_MODEL_MAP.items():
        path = config_dir / filename
        ok, message = validate_config_file(path, model_cls)
        results.append(message)
        if not ok:
            success = False
    return success, results


def load_configs(config_dir: Path) -> Dict[str, Any]:
    configs: Dict[str, Any] = {}
    for filename, model_cls in CONFIG_MODEL_MAP.items():
        path = config_dir / filename
        data = load_yaml(path)
        configs[filename] = model_cls(**data)
    return configs


def load_sources_config(config_dir: Path) -> SourcesConfig:
    data = load_yaml(config_dir / "sources.yml")
    return SourcesConfig(**data)


def print_validation_report(config_dir: Path) -> int:
    ok, messages = validate_config_dir(config_dir)
    for message in messages:
        print(message)
    if not ok:
        logger.error("Config validation failed.")
        return 1
    logger.info("Config validation succeeded.")
    return 0


if __name__ == "__main__":
    exit_code = print_validation_report(Path("config"))
    sys.exit(exit_code)
