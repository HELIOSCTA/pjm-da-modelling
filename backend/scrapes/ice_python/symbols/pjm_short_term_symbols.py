"""
PJM ICE short-term symbol registry.

Add or remove PJM symbols here.  The tick-data pipeline reads this list
at runtime, so changes take effect on the next scheduled run with no
pipeline code changes required.

Each entry must have at minimum:
    symbol       – the ICE symbol code passed to get_quotes / get_timesales
    description  – human-readable label for logging and audits
    product_type – "power" or "gas"
    contract_type – "daily", "weekly", "monthly", "balmo", etc.

Usage:
    from backend.scrapes.ice_python.symbols.pjm_short_term_symbols import get_pjm_symbols
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Symbol registry
# ---------------------------------------------------------------------------
# Populate this list with the exact ICE symbol codes you want to ingest.
# The pipeline iterates over this list; nothing else needs to change.
#
# Placeholder entries below are derived from the ICE_PYTHON_TICK_LEVEL_DATA
# screenshot.  Replace / extend once final symbols are confirmed.
# ---------------------------------------------------------------------------

PJM_SYMBOLS: list[dict] = [
    # -- Daily power products ------------------------------------------------
    {
        "symbol": "PDP D0-IUS",
        "description": "PJM Balance of Day",
        "product_type": "power",
        "contract_type": "daily",
    },
    {
        "symbol": "PDP D1-IUS",
        "description": "PJM RT Next Day",
        "product_type": "power",
        "contract_type": "daily",
    },
    {
        "symbol": "PDA D1-IUS",
        "description": "PJM DA Next Day",
        "product_type": "power",
        "contract_type": "daily",
    },
    # -- Weekly power products -----------------------------------------------
    {
        "symbol": "PDP W0-IUS",
        "description": "PJM Balance of Week",
        "product_type": "power",
        "contract_type": "weekly",
    },
    {
        "symbol": "PDP W1-IUS",
        "description": "PJM Week 1",
        "product_type": "power",
        "contract_type": "weekly",
    },
    {
        "symbol": "PDP W2-IUS",
        "description": "PJM Week 2",
        "product_type": "power",
        "contract_type": "weekly",
    },
    {
        "symbol": "PDP W3-IUS",
        "description": "PJM Week 3",
        "product_type": "power",
        "contract_type": "weekly",
    },
    {
        "symbol": "PDP W4-IUS",
        "description": "PJM Week 4",
        "product_type": "power",
        "contract_type": "weekly",
    },
    # -- Monthly power products (add strips as needed) -----------------------
    # Example: PMI H26-IUS = PJM Western Hub RT Peak, March 2026
    # Uncomment / add entries when you confirm the exact strips to track.
    # {
    #     "symbol": "PMI H26-IUS",
    #     "description": "PJM Western Hub RT Peak Mar 2026",
    #     "product_type": "power",
    #     "contract_type": "monthly",
    # },
]


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------

def get_pjm_symbols() -> list[dict]:
    """Return all active PJM symbol entries."""
    return PJM_SYMBOLS


def get_pjm_symbol_codes(symbol_entries: list[dict] | None = None) -> list[str]:
    """Return just the symbol strings for API calls."""
    entries = symbol_entries or PJM_SYMBOLS
    return [entry["symbol"] for entry in entries]


def get_pjm_symbol_map() -> dict[str, dict]:
    """Return PJM symbols keyed by ICE symbol code."""
    return {entry["symbol"]: entry for entry in PJM_SYMBOLS}


def resolve_pjm_symbol_entries(symbols: list[str] | None = None) -> list[dict]:
    """Resolve an optional user-selected symbol list against the registry.

    Args:
        symbols: Optional list of ICE symbol codes. If omitted, all configured
            PJM symbols are returned.

    Returns:
        PJM symbol registry entries in the requested order.

    Raises:
        ValueError: If an explicit symbol list contains blanks or unknown codes.
    """
    if symbols is None:
        return list(PJM_SYMBOLS)

    normalized_symbols = [symbol.strip() for symbol in symbols if symbol and symbol.strip()]
    if not normalized_symbols:
        raise ValueError("No valid PJM symbol codes were provided.")

    symbol_map = get_pjm_symbol_map()
    unknown_symbols = sorted(set(normalized_symbols) - set(symbol_map))
    if unknown_symbols:
        raise ValueError(
            "Unknown PJM ICE symbols: "
            f"{unknown_symbols}. Valid symbols must come from "
            "pjm_short_term_symbols.py."
        )

    unique_symbols = list(dict.fromkeys(normalized_symbols))
    return [symbol_map[symbol] for symbol in unique_symbols]


def log_all_symbols(symbol_entries: list[dict] | None = None) -> None:
    """Log all configured PJM short-term symbols."""
    entries = symbol_entries or get_pjm_symbols()
    logger.info("Configured %s PJM short-term symbols", len(entries))
    for entry in entries:
        logger.info(
            "%s | %s | %s | %s",
            entry["symbol"],
            entry["description"],
            entry["product_type"],
            entry["contract_type"],
        )


def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    log_all_symbols()


if __name__ == "__main__":
    _main()
