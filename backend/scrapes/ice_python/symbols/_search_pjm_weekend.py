"""Verify 3 user-supplied ICE next-day gas symbol candidates.

Tennessee Z5 (`Z28 D1-IPG`) is already added to `next_day_gas_symbols.py`.
These 3 still need a real timeseries pull to confirm they exist and return
prices in the same shape as the existing gas hubs:

    YVQ D1-IPG  -> REX East-Midwest
    XZL D1-IPG  -> ANR Southwest
    XIH D1-IPG  -> Panhandle Oklahoma

Also tries the search-symbol lookup so we can see the canonical description.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from pprint import pprint

_PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.scrapes.ice_python import utils  # noqa: E402


CANDIDATES: list[tuple[str, str]] = [
    ("YVQ D1-IPG", "REX East-Midwest"),
    ("XZL D1-IPG", "ANR Southwest"),
    ("XIH D1-IPG", "Panhandle Oklahoma"),
]


def _describe(ice, symbol: str) -> None:
    """Look the symbol up in get_search so we see ICE's own description."""
    try:
        result = ice.get_search(symbol)
    except Exception as exc:
        print(f"  search ERROR: {exc}")
        return
    if not result:
        print("  search: no hits")
        return
    # exact match first
    exact = [row for row in result if row[0] == symbol]
    if exact:
        print("  search exact:")
        for row in exact:
            print(f"    {row}")
        return
    print(f"  search (no exact match, top 3 of {len(result)}):")
    for row in result[:3]:
        print(f"    {row}")


def _pull(symbol: str) -> None:
    start = datetime.now() - timedelta(days=30)
    end = datetime.now()
    try:
        df = utils.get_timeseries(
            symbol=symbol,
            data_type="VWAP Close",
            granularity="D",
            start_date=start,
            end_date=end,
            date_col=utils.DEFAULT_DATE_COLUMN,
            date_format=utils.DEFAULT_DATE_FORMAT,
        )
    except Exception as exc:
        print(f"  pull ERROR: {exc}")
        return
    if df is None or df.empty:
        print("  pull: empty frame")
        return
    print(f"  pull: rows={len(df)}, columns={list(df.columns)}")
    print("  head:")
    print(df.head(3).to_string(index=False))
    print("  tail:")
    print(df.tail(3).to_string(index=False))


def run() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    ice = utils.get_icepython_module()

    for symbol, label in CANDIDATES:
        print()
        print("=" * 70)
        print(f"### {symbol}  ({label})")
        print("=" * 70)
        _describe(ice, symbol)
        _pull(symbol)


if __name__ == "__main__":
    run()
