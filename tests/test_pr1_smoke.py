import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb


def write_config(tmp_dir: Path) -> Path:
    config_dir = tmp_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    settings = {
        "db_path": str(tmp_dir / "data/app.duckdb"),
        "output_root": str(tmp_dir / "output/runs"),
        "run_time": "07:00",
    }
    sources = {
        "sources": [
            {"name": "tokyo_daily", "category": "jp", "enabled": True},
            {"name": "global_wire", "category": "global", "enabled": True},
        ]
    }
    (config_dir / "settings.yml").write_text(json.dumps(settings), encoding="utf-8")
    (config_dir / "sources.yml").write_text(json.dumps(sources), encoding="utf-8")
    return config_dir


def run_cmd(cmd: list[str], env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, env=env, cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True
    )


def test_cli_smoke(tmp_path: Path):
    config_dir = write_config(tmp_path)
    env = os.environ.copy()
    env["RUN_ID"] = "test-run-id"
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")

    validate = run_cmd(
        [sys.executable, "-m", "tool", "validate-config", "--config-dir", str(config_dir)], env
    )
    assert validate.returncode == 0, validate.stderr

    init_db = run_cmd(
        [sys.executable, "-m", "tool", "init-db", "--config-dir", str(config_dir)], env
    )
    assert init_db.returncode == 0, init_db.stderr

    run = run_cmd(
        [sys.executable, "-m", "tool", "run", "manual", "--config-dir", str(config_dir)], env
    )
    assert run.returncode == 0, run.stderr

    db_path = tmp_path / "data/app.duckdb"
    assert db_path.exists()
    conn = duckdb.connect(str(db_path))
    runs = conn.execute("SELECT count(*) FROM runs").fetchone()[0]
    items = conn.execute("SELECT count(*) FROM items").fetchone()[0]
    assert runs >= 1
    assert items >= 1

    output_dir = tmp_path / "output/runs/test-run-id"
    assert (output_dir / "brief_items.csv").exists()
    assert (output_dir / "alerts.json").exists()
    assert (output_dir / "run_stats.json").exists()
    assert (output_dir / "brief_items.csv").read_text(encoding="utf-8").strip() != ""
