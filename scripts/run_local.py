from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = str(ROOT / ".venv312" / "bin" / "python")


def run_module(module: str) -> None:
    cmd = [PYTHON, "-m", module]
    print("+", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and optionally serve EASYGE dashboard locally.")
    parser.add_argument("--fetch", action="store_true", help="Fetch latest holdings before rebuild")
    parser.add_argument("--serve", action="store_true", help="Start local HTTP server after rebuild")
    parser.add_argument("--port", type=int, default=8000, help="Port for local server (default: 8000)")
    args = parser.parse_args()

    if not Path(PYTHON).exists():
        raise FileNotFoundError(f"Missing venv python: {PYTHON}")

    run_module("scripts.analyse_holdings")
    run_module("scripts.build_charts")
    run_module("scripts.build_dashboard")
    run_module("scripts.build_readme")

    if args.fetch:
        run_module("scripts.fetch_holdings")

    if args.serve:
        print(f"\nServing site on http://localhost:{args.port}/\n")
        subprocess.run([PYTHON, "-m", "http.server", str(args.port), "-d", "site"], cwd=ROOT, check=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
