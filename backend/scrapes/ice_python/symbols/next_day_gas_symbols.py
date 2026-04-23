"""
ICE next-day gas symbol registry.

Each entry has:
    symbol        – the ICE symbol code passed to get_timeseries
    description   – human-readable label for logging and audits
    product_type  – "gas"
    contract_type – "next_day"
    region        – geographic region grouping

Usage:
    from backend.scrapes.ice_python.symbols.next_day_gas_symbols import get_next_day_gas_symbols
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Symbol registry
# ---------------------------------------------------------------------------
# Source: backend/scrapes/ice_python/next_day_gas/next_day_gas_v1_2025_dec_16.py
# NOTE: NG Firm Phys, FP
# ---------------------------------------------------------------------------

NEXT_DAY_GAS_SYMBOLS: list[dict] = [
    # -- Louisiana ---------------------------------------------------------------
    {
        "symbol": "XGF D1-IPG",
        "description": "Henry Hub",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "louisiana",
    },
    # -- Southeast ---------------------------------------------------------------
    {
        "symbol": "XVA D1-IPG",
        "description": "Transco Station 85",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "southeast",
    },
    {
        "symbol": "XLM D1-IPG",
        "description": "TGP-500L",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "southeast",
    },
    {
        "symbol": "YHV D1-IPG",
        "description": "FGT Zone 3",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "southeast",
    },
    {
        "symbol": "XLA D1-IPG",
        "description": "Columbia Gulf (Mainline)",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "southeast",
    },
    {
        "symbol": "XTA D1-IPG",
        "description": "ANR SE-T",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "southeast",
    },
    {
        "symbol": "YV7 D1-IPG",
        "description": "Pine Prairie",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "southeast",
    },
    {
        "symbol": "XVM D1-IPG",
        "description": "Tetco WLA",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "southeast",
    },
    # -- East Texas --------------------------------------------------------------
    {
        "symbol": "XYZ D1-IPG",
        "description": "Houston Ship Channel",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "east_texas",
    },
    {
        "symbol": "XT6 D1-IPG",
        "description": "Waha",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "east_texas",
    },
    {
        "symbol": "XIT D1-IPG",
        "description": "NGPL TX/OK",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "east_texas",
    },
    # -- Northeast ---------------------------------------------------------------
    {
        "symbol": "X7F D1-IPG",
        "description": "Algonquin Citygates (non-G)",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "XZR D1-IPG",
        "description": "Tetco M3",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "YFF D1-IPG",
        "description": "Transco Zone 5 South",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "Z2Y D1-IPG",
        "description": "Transco Zone 5 North",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "YP8 D1-IPG",
        "description": "Iroquois Zone 2",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "XWK D1-IPG",
        "description": "Transco Zone 6 NY",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "XJL D1-IPG",
        "description": "Dominion South (Eastern Gas-South)",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "XIZ D1-IPG",
        "description": "Columbia TCO Pool",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "YAG D1-IPG",
        "description": "Tetco M2 (Receipt)",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "Z1Q D1-IPG",
        "description": "Tennessee Z4 (Marcellus)",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "YQE D1-IPG",
        "description": "Transco Leidy",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "northeast",
    },
    {
        "symbol": "XTG D1-IPG",
        "description": "Northern Ventura (NNG)",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "midwest",
    },
    {
        "symbol": "YHF D1-IPG",
        "description": "Chicago CityGate (NGPL-Nicor)",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "midwest",
    },
    # -- Southwest ---------------------------------------------------------------
    {
        "symbol": "XKF D1-IPG",
        "description": "SoCal Citygate",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "southwest",
    },
    {
        "symbol": "XGV D1-IPG",
        "description": "PG&E Citygate",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "southwest",
    },
    # -- Rockies/Northwest -------------------------------------------------------
    {
        "symbol": "YKL D1-IPG",
        "description": "CIG Mainline",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "rockies_northwest",
    },
    # -- Midwest -----------------------------------------------------------------
    {
        "symbol": "XJR D1-IPG",
        "description": "NGPL Midcontinent",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "midwest",
    },
    {
        "symbol": "XJZ D1-IPG",
        "description": "MichCon",
        "product_type": "gas",
        "contract_type": "next_day",
        "region": "midwest",
    },
]


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------

def get_next_day_gas_symbols() -> list[dict]:
    """Return all active next-day gas symbol entries."""
    return NEXT_DAY_GAS_SYMBOLS


def get_next_day_gas_symbol_codes(symbol_entries: list[dict] | None = None) -> list[str]:
    """Return just the symbol strings for API calls."""
    entries = symbol_entries or NEXT_DAY_GAS_SYMBOLS
    return [entry["symbol"] for entry in entries]


def get_next_day_gas_symbol_map() -> dict[str, dict]:
    """Return next-day gas symbols keyed by ICE symbol code."""
    return {entry["symbol"]: entry for entry in NEXT_DAY_GAS_SYMBOLS}


def log_all_symbols(symbol_entries: list[dict] | None = None) -> None:
    """Log all configured next-day gas symbols."""
    entries = symbol_entries or get_next_day_gas_symbols()
    logger.info("Configured %s next-day gas symbols", len(entries))
    for entry in entries:
        logger.info(
            "%s | %s | %s | %s",
            entry["symbol"],
            entry["description"],
            entry["region"],
            entry["contract_type"],
        )


def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    log_all_symbols()


if __name__ == "__main__":
    _main()
