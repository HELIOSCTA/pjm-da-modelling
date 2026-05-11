"""PreToolUse hook: ensure the local MCP server is healthy.

Fires before any `mcp__pjm-views__*` tool call (matcher scoped in
.claude/settings.json). Fast localhost health check; only restarts if
the server is unreachable, so the steady-state cost is one quick HTTP
GET (~10-50ms on a warm server).

Reads the standard Claude Code hook JSON payload from stdin but
doesn't need to inspect it — the matcher already filtered us to the
right tool calls.

Exit codes:
    0  = MCP healthy (no-op, or restart succeeded). Tool call continues.
    2  = MCP unhealthy and restart failed. Tool call blocked; stderr
         is shown to the model so it can surface the failure cleanly.
"""

from __future__ import annotations

import subprocess
import sys
import urllib.error
import urllib.request

HEALTH_URL = "http://localhost:8000/openapi.json"
HEALTH_TIMEOUT_SEC = 2.0
RESTART_TIMEOUT_SEC = 60


def _is_healthy() -> bool:
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=HEALTH_TIMEOUT_SEC) as r:
            return r.status == 200
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
        return False


def main() -> int:
    # Drain stdin so Claude Code doesn't see a broken pipe; we don't
    # actually need the payload.
    try:
        sys.stdin.read()
    except Exception:
        pass

    if _is_healthy():
        return 0

    print(
        f"[mcp-health] MCP unreachable at {HEALTH_URL} — restarting...",
        file=sys.stderr,
    )
    try:
        result = subprocess.run(
            [sys.executable, "-m", "backend.mcp_server.ensure_running"],
            capture_output=True,
            text=True,
            timeout=RESTART_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        print(
            f"[mcp-health] ensure_running timed out after {RESTART_TIMEOUT_SEC}s.\n"
            "  See backend/mcp_server/logs/server.log",
            file=sys.stderr,
        )
        return 2

    if result.returncode == 0 and _is_healthy():
        return 0

    print(
        "[mcp-health] Restart failed.\n"
        f"  ensure_running exit: {result.returncode}\n"
        f"  stdout:\n{result.stdout or '    <empty>'}\n"
        f"  stderr:\n{result.stderr or '    <empty>'}\n"
        "  Server log: backend/mcp_server/logs/server.log",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
