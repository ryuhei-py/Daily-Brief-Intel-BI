from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.app.config import validate_config
from src.app.db import init_db as app_init_db
from src.app.pipeline import run_pipeline
from src.core.logging import get_logger

logger = get_logger(__name__)


def cmd_validate_config(args: argparse.Namespace) -> int:
    config_dir = Path(args.config_dir).resolve()
    ok, messages = validate_config(config_dir)
    for message in messages:
        print(message)
    if not ok:
        logger.error("Config validation failed.")
        return 1
    logger.info("Config validation succeeded.")
    return 0


def cmd_init_db(args: argparse.Namespace) -> int:
    config_dir = Path(args.config_dir).resolve()
    from src.app.config import load_settings

    settings = load_settings(config_dir)
    target = app_init_db(settings)
    print(f"Initialized database at {target}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    config_dir = Path(args.config_dir).resolve()
    run_pipeline(config_dir=config_dir, mode=args.mode)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tool", description="Utility commands")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate-config", help="Validate YAML configs")
    validate_parser.add_argument("--config-dir", default="config", help="Path to config directory")
    validate_parser.set_defaults(func=cmd_validate_config)

    init_parser = subparsers.add_parser("init-db", help="Bootstrap DuckDB schema")
    init_parser.add_argument("--config-dir", default="config", help="Path to config directory")
    init_parser.set_defaults(func=cmd_init_db)

    run_parser = subparsers.add_parser("run", help="Manage runs")
    run_parser.add_argument("mode", choices=["manual", "scheduled"], help="Run mode")
    run_parser.add_argument("--config-dir", default="config", help="Path to config directory")
    run_parser.set_defaults(func=cmd_run)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if not func:
        parser.print_help()
        return 1
    return func(args)


if __name__ == "__main__":
    sys.exit(main())
