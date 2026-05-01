"""Always-fresh MCP server pre-flight.

Used by slash commands as the first step. Always-fresh semantics:

  1. Kill any process listening on port 8000 (yours, a stale uvicorn,
     anything — no diff between "healthy MCP" and "stale process").
  2. Spawn a detached uvicorn (logs go to backend/mcp_server/logs/server.log).
  3. Poll /openapi.json until healthy or timeout.
  4. Exit 0 on healthy, 1 on timeout.

This guarantees each slash-command invocation runs against the current
code on disk. Code changes to data layer / view builders / formatters
are picked up automatically — no stale in-memory state.

Exit codes:
    0  = Fresh MCP server is up and healthy.
    1  = Failed to come up within timeout. See log for traceback.

Usage:
    python -m backend.mcp_server.ensure_running
"""
from __future__ import annotations

import platform
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PORT = 8000
HEALTH_URL = f"http://localhost:{PORT}/openapi.json"
READY_TIMEOUT_SEC = 30

# Repo root: this file is at backend/mcp_server/ensure_running.py
ROOT = Path(__file__).resolve().parents[2]
LOG_PATH = ROOT / "backend" / "mcp_server" / "logs" / "server.log"


def _is_healthy() -> bool:
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=2) as r:
            return r.status == 200
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError):
        return False


def _port_bound(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _pids_on_port(port: int) -> list[int]:
    if platform.system() == "Windows":
        try:
            r = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True, text=True, check=False, timeout=10,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return []
        target = f":{port}"
        pids: set[int] = set()
        for line in r.stdout.splitlines():
            if target in line and "LISTENING" in line:
                parts = line.split()
                if parts and parts[-1].isdigit():
                    pids.add(int(parts[-1]))
        return sorted(pids)
    for cmd in (["lsof", "-ti", f":{port}"], ["fuser", f"{port}/tcp"]):
        try:
            r = subprocess.run(
                cmd, capture_output=True, text=True, check=False, timeout=10,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
        pids = [int(p) for p in r.stdout.split() if p.strip().isdigit()]
        if pids:
            return pids
    return []


def _kill(pid: int) -> bool:
    try:
        if platform.system() == "Windows":
            # /T also kills child processes (uvicorn workers)
            r = subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, check=False, timeout=10,
            )
        else:
            r = subprocess.run(
                ["kill", "-9", str(pid)],
                capture_output=True, check=False, timeout=10,
            )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return r.returncode == 0


def _kill_port(port: int) -> int:
    """Kill all listeners on port. Returns count killed. Waits briefly for OS to release."""
    pids = _pids_on_port(port)
    killed = 0
    for pid in pids:
        if _kill(pid):
            killed += 1
            print(f"  killed pid={pid}", file=sys.stderr)
        else:
            print(f"  failed to kill pid={pid}", file=sys.stderr)
    # Wait up to 2s for the OS to fully release the port before bind retry.
    for _ in range(10):
        if not _port_bound(port):
            break
        time.sleep(0.2)
    return killed


def _start_uvicorn() -> subprocess.Popen:
    """Spawn detached uvicorn. Stdout/stderr → log file."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Append; rotation can be added later if log grows large.
    log = open(LOG_PATH, "ab")
    cmd = [
        sys.executable, "-m", "uvicorn",
        "backend.mcp_server.main:app",
        "--host", "127.0.0.1",
        "--port", str(PORT),
    ]
    kwargs: dict = {
        "stdout": log,
        "stderr": subprocess.STDOUT,
        "stdin": subprocess.DEVNULL,
        "cwd": str(ROOT),
    }
    if platform.system() == "Windows":
        kwargs["creationflags"] = (
            subprocess.DETACHED_PROCESS
            | subprocess.CREATE_NEW_PROCESS_GROUP
        )
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(cmd, **kwargs)


def _wait_ready(timeout: int = READY_TIMEOUT_SEC) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _is_healthy():
            return True
        time.sleep(0.5)
    return False


def main() -> int:
    if _port_bound(PORT):
        print(f"Port {PORT} bound — killing existing listener(s):", file=sys.stderr)
        _kill_port(PORT)

    print(f"Starting MCP server (uvicorn) on port {PORT}...", file=sys.stderr)
    print(f"  log: {LOG_PATH}", file=sys.stderr)
    proc = _start_uvicorn()
    print(f"  pid: {proc.pid}", file=sys.stderr)

    if _wait_ready():
        print(f"MCP server healthy on port {PORT} (pid {proc.pid})")
        return 0

    print(
        f"\nERROR: MCP server failed to come up within {READY_TIMEOUT_SEC}s.\n"
        f"Check the log:\n  {LOG_PATH}",
        file=sys.stderr,
    )
    # Best-effort: don't leave a dangling failed process.
    try:
        proc.terminate()
    except Exception:
        pass
    return 1


if __name__ == "__main__":
    sys.exit(main())
