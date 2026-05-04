"""Use icepython's get_search to find the PJM WH DA Off-Peak Weekend symbol.

Round-3 discovery: round 1 found PDO is the DA Off-Peak root, round 2 found
no weekend tenor on PDO. icepython exposes get_search / get_search_facets /
get_search_filters — let's ask ICE directly instead of guessing tenors.
"""

from __future__ import annotations

import sys
from inspect import signature
from pathlib import Path
from pprint import pprint

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.scrapes.ice_python import utils  # noqa: E402


def _try(label: str, fn, *args, **kwargs) -> None:
    print()
    print("-" * 70)
    print(f">>> {label}")
    print("-" * 70)
    try:
        result = fn(*args, **kwargs)
    except Exception as exc:
        print(f"  ERROR: {exc}")
        return
    if hasattr(result, "__len__"):
        try:
            n = len(result)
            print(f"  (len={n})")
            preview = result[:30] if n > 30 else result
            pprint(preview)
            return
        except Exception:
            pass
    pprint(result)


def run() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    ice = utils.get_icepython_module()

    for name in ("get_search", "get_search_facets", "get_search_filters"):
        fn = getattr(ice, name, None)
        if fn is None:
            continue
        try:
            sig = signature(fn)
        except (TypeError, ValueError):
            sig = "(introspection unavailable)"
        print(f"{name}{sig}")
        doc = (fn.__doc__ or "").strip()
        if doc:
            print(f"  doc: {doc[:400]}")

    # Try get_search_facets / filters with no args first to learn the shape.
    _try("get_search_facets()", ice.get_search_facets)
    _try("get_search_filters()", ice.get_search_filters)

    # Now try a few search queries.
    queries = [
        "PJM Off-Peak Weekend",
        "PJM Western Hub Off-Peak Weekend",
        "PJM WH DA Off-Peak Weekend",
        "PJM Off-Peak Wknd",
        "PJM Weekend",
        "PJM 2x16",
        "PJM Off-Peak",
        "PDO",
    ]
    for q in queries:
        _try(f"get_search({q!r})", ice.get_search, q)


if __name__ == "__main__":
    run()
