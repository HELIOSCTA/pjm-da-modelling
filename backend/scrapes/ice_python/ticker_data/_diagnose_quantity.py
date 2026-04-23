"""
Diagnostic: figure out why Quantity is coming back NULL from ice.get_timesales().

Run on the ICE XL host:
    python backend/scrapes/ice_python/ticker_data/_diagnose_quantity.py

Prints:
    1. get_timesales_fields() for each PJM symbol (the actual valid field names
       ICE reports for this instrument type).
    2. A raw get_timesales() response for a ~1h window on PDA D1-IUS — so we can
       see the exact column headers and whether Quantity is blank vs absent.
    3. get_quotes_fields() for cross-reference (quote-side field catalog).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pprint import pprint

from backend.scrapes.ice_python import utils
from backend.scrapes.ice_python.symbols.pjm_short_term_symbols import (
    get_pjm_symbol_codes,
)

DIAG_SYMBOL = "PDA D1-IUS"  # liquid daily — should have ticks + qty if any symbol does
DIAG_LOOKBACK_HOURS = 4


def main() -> None:
    ice = utils.get_icepython_module()

    print("=" * 70)
    print("1. get_timesales_fields() per PJM symbol")
    print("=" * 70)
    for symbol in get_pjm_symbol_codes():
        try:
            fields = ice.get_timesales_fields(symbol)
            print(f"\n  {symbol}:")
            pprint(fields, indent=4)
        except Exception as exc:
            print(f"\n  {symbol}: ERROR — {exc}")

    print("\n" + "=" * 70)
    print(f"2. Raw get_timesales() response for {DIAG_SYMBOL}")
    print("=" * 70)
    end = datetime.utcnow()
    start = end - timedelta(hours=DIAG_LOOKBACK_HOURS)
    fmt = "%Y-%m-%d %H:%M:%S"
    print(f"  Window: {start.strftime(fmt)} -> {end.strftime(fmt)} (UTC)")

    for fields_to_try in (
        ["Price", "Size"],
        ["Price", "Size", "Type", "Conditions", "Value"],
    ):
        print(f"\n  Trying fields={fields_to_try!r} ...")
        try:
            data = ice.get_timesales(
                [DIAG_SYMBOL],
                fields_to_try,
                start_date=start.strftime(fmt),
                end_date=end.strftime(fmt),
            )
            if not data:
                print("    (empty response)")
                continue
            print(f"    header   : {data[0]!r}")
            print(f"    row count: {len(data) - 1}")
            for row in data[1:6]:
                print(f"    row      : {row!r}")
        except Exception as exc:
            print(f"    ERROR — {exc}")

    print("\n" + "=" * 70)
    print("3. get_quotes_fields() (catalog — for reference)")
    print("=" * 70)
    try:
        quote_fields = ice.get_quotes_fields()
        pprint(quote_fields, indent=4)
    except Exception as exc:
        print(f"  ERROR — {exc}")


if __name__ == "__main__":
    main()
