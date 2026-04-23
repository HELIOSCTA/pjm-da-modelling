"""
Top-level runner for every ICE Python scrape.

Discovers scripts across every subfolder (next_day_gas, intraday_quotes,
contract_dates, ticker_data) plus top-level one-shots like
install_ice_python.py. Excludes __init__, runs, and *_utils modules.

Usage:
    python runs.py                    # interactive menu
    python runs.py --list             # list all available scripts
    python runs.py all                # run every script sequentially
    python runs.py <number>           # run script by menu number
    python runs.py <number> <number>  # run multiple scripts
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils.runner_utils import RunnerConfig, runner_main, run_script_main_only

EXCLUDED_NAMES = {"__init__.py", "runs.py", "flows.py", "install_ice_python.py"}
EXCLUDED_DIRS = {"__pycache__", "logs", "symbols"}


def _is_runnable(path: Path) -> bool:
    if path.name in EXCLUDED_NAMES:
        return False
    if path.name.startswith("_"):
        return False
    if path.name.endswith("_utils.py") or path.name == "utils.py":
        return False
    if any(part in EXCLUDED_DIRS for part in path.parts):
        return False
    return True


def discover_scripts() -> list[Path]:
    return sorted(p for p in SCRIPT_DIR.rglob("*.py") if _is_runnable(p))


def _display_name(path: Path) -> str:
    return str(path.relative_to(SCRIPT_DIR).with_suffix("")).replace("\\", "/")


def display_menu(scripts: list[Path]) -> None:
    print("\n=== Available ICE Python Scripts ===\n")
    for index, script in enumerate(scripts, 1):
        print(f"  [{index:>2}] {_display_name(script)}")
    print()


def main() -> None:
    config = RunnerConfig(
        name="ICE Python",
        project_root=PROJECT_ROOT,
        discover=discover_scripts,
        display=display_menu,
        display_name=_display_name,
        adapter=run_script_main_only,
    )
    runner_main(config)


if __name__ == "__main__":
    main()
