"""
Runner for manually executing ICE contract dates scripts.

Usage:
    python runs.py                    # interactive menu
    python runs.py --list             # list available scripts
    python runs.py all                # run all scripts
    python runs.py <number>           # run script by menu number
    python runs.py <number> <number>  # run multiple scripts
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils.runner_utils import RunnerConfig, runner_main, run_script_main_only


def discover_scripts() -> list[Path]:
    return sorted(path for path in SCRIPT_DIR.glob("runner_*.py"))


def display_menu(scripts: list[Path]) -> None:
    print("\n=== Available ICE Contract Dates Scripts ===\n")
    for index, script in enumerate(scripts, 1):
        print(f"  [{index:>2}] {script.stem}")
    print()


def main() -> None:
    config = RunnerConfig(
        name="ICE Contract Dates",
        project_root=PROJECT_ROOT,
        discover=discover_scripts,
        display=display_menu,
        display_name=lambda path: path.stem,
        adapter=run_script_main_only,
    )
    runner_main(config)


if __name__ == "__main__":
    main()
