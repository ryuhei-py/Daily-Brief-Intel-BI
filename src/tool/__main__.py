from __future__ import annotations

import argparse
import sys
from pathlib import Path

from src.app.pipeline import run_pipeline
from src.core.config_loader import print_validation_report
from src.core.logging import get_logger
from src.pipeline.run_lock import RunLock, RunLockedError
from src.storage.migrate import init_db

logger = get_logger(__name__)


def cmd_validate_config(args: argparse.Namespace) -> int:
    config_dir = Path(args.config_dir).resolve()
    return print_validation_report(config_dir)


def cmd_init_db(args: argparse.Namespace) -> int:
    db_path = Path(args.db_path) if args.db_path else None
    target = init_db(db_path=db_path)
    print(f"Initialized database at {target}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    lock = RunLock()
    try:
        lock.acquire()
    except RunLockedError:
        logger.error("Another run appears to be in progress; try again later.")
        return 1

    try:
        init_db()
        run_id, _ = run_pipeline(
            config_dir=args.config_dir,
            mode=args.mode,
            run_id=args.run_id,
            overwrite_run=args.overwrite_run,
        )
        logger.info("Run %s finished in mode=%s", run_id, args.mode)
        return 0
    except Exception as exc:  # pragma: no cover
        logger.error("Run failed: %s", exc)
        return 1
    finally:
        lock.release()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tool", description="Utility commands")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate-config", help="Validate YAML configs")
    validate_parser.add_argument("--config-dir", default="config", help="Path to config directory")
    validate_parser.set_defaults(func=cmd_validate_config)

    init_parser = subparsers.add_parser("init-db", help="Bootstrap DuckDB schema")
    init_parser.add_argument("--db-path", help="Override database path")
    init_parser.set_defaults(func=cmd_init_db)

    run_parser = subparsers.add_parser("run", help="Manage runs")
    run_parser.add_argument("mode", choices=["manual", "scheduled"], help="Run mode")
    run_parser.add_argument("--config-dir", default="config", help="Path to config directory")
    run_parser.add_argument("--run-id", help="Optional run identifier to use")
    run_parser.add_argument(
        "--overwrite-run",
        action="store_true",
        help="Overwrite existing data for the provided run id if it exists",
    )
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
