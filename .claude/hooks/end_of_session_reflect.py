"""Stop hook: end-of-session reflection prompt.

Surfaces a short reminder asking Claude to consider whether project-
scoped facts established this session belong in ``CLAUDE.md`` or new
``settings.json`` hooks. Exits with code 2 so the message reaches Claude
as additional context and gets a chance to act on it before truly
stopping.

Reads the Stop event JSON from stdin but doesn't actually need it -
this is a fixed reminder, not a per-event computation.
"""
from __future__ import annotations

import sys


REMINDER = (
    "End-of-task check (per CLAUDE.md routing rule):\n"
    "  - Did this session establish a project-scoped fact (layout, "
    "convention, named standard)? If so, propose a CLAUDE.md edit.\n"
    "  - Did a deterministic rule emerge that should run every tool "
    "call? If so, propose a settings.json hook.\n"
    "  - Personal preferences belong in MEMORY.md (auto-maintained); "
    "no action needed here.\n"
    "If nothing applies, ack briefly and stop."
)


def main() -> int:
    try:
        sys.stdin.read()
    except Exception:
        pass

    print(REMINDER, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
