"""
Runner for manually executing EIA scrapes.

Usage:
    python runs.py                    # interactive menu
    python runs.py --list             # list all available scripts
    python runs.py <number>           # run script by menu number
    python runs.py <number> <number>  # run multiple scripts sequentially
    python runs.py all                # run all scripts
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.utils.runner_utils import (  # noqa: E402
    RunnerConfig,
    run_script_main_only,
    runner_main,
)


def discover_scripts() -> list[Path]:
    """Find all .py scripts in the EIA directory (excluding __init__, run,
    flows, and any underscore-prefixed shared modules / scratch).
    """
    return sorted(
        p
        for p in SCRIPT_DIR.glob("*.py")
        if p.name not in ("__init__.py", "run.py", "runs.py", "flows.py")
        and not p.name.startswith("_")
    )


def display_menu(scripts: list[Path]) -> None:
    """Print a numbered list of available EIA scripts."""
    print("\n=== Available EIA Scripts ===\n")
    for i, script in enumerate(scripts, 1):
        print(f"  [{i}] {script.name}")
    print()


def main():
    config = RunnerConfig(
        name="EIA",
        project_root=PROJECT_ROOT,
        discover=discover_scripts,
        display=display_menu,
        display_name=lambda p: p.name,
        adapter=run_script_main_only,
    )
    runner_main(config)


if __name__ == "__main__":
    main()
