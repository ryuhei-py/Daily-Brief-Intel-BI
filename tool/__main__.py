from __future__ import annotations

import sys
from pathlib import Path


def _run() -> int:
    src_dir = Path(__file__).resolve().parents[1] / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    from src.tool.__main__ import main as _main

    return _main()


if __name__ == "__main__":
    raise SystemExit(_run())
