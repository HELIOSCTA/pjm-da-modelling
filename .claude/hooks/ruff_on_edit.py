"""PostToolUse hook: format and lint Python files after Claude edits them.

Reads the tool input from stdin (Claude Code passes JSON), checks the
edited file is `.py`, and runs ``ruff format`` then ``ruff check --fix``.
Surviving lint output goes to stderr with exit code 2 so Claude sees it
as additional context and can self-correct on the next turn.

Fails soft: exits 0 silently when ``ruff`` is not on PATH or when the
edited file isn't Python. Install with ``pip install ruff`` to activate.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys


def main() -> int:
    if shutil.which("ruff") is None:
        return 0

    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0

    tool_input = data.get("tool_input") or {}
    path = tool_input.get("file_path") or ""
    if not path.endswith(".py"):
        return 0

    subprocess.run(
        ["ruff", "format", path],
        capture_output=True, text=True, check=False,
    )
    result = subprocess.run(
        ["ruff", "check", "--fix", path],
        capture_output=True, text=True, check=False,
    )

    remaining = (result.stdout or "").strip()
    if remaining and "All checks passed" not in remaining:
        print(f"ruff issues in {path}:\n{remaining}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
