"""
ICE RGGI futures symbol registry.

Two venues list RGGI futures and ICE Connect exposes both:

- **ICE Futures U.S. (exchange code ``IUSE``)** — the primary RGGI
  venue. Vintage-segmented monthly futures, one root per vintage:
  ``RJ3``=V2023, ``RJ4``=V2024, ``RJ5``=V2025, ``RJ6``=V2026,
  ``RJ7``=V2027, ``RJ0``=V2030. Symbol format
  ``<root> <month_code><yy>-IUS`` (e.g. ``RJ6 Z26-IUS`` = V2026
  contract expiring Dec 2026). Plus the Auction Clearing Price
  futures (``RCP``) listed Mar/Jun/Sep/Dec and the continuous
  auction-index symbol ``RCP_P A0-IUS``.
- **NYMEX Globex (exchange code ``NYMG``)** — CME's competing listing
  with root ``RGI``. Not vintage-segmented; far thinner liquidity.
  Kept for cross-venue comparison only.

Entitlement note (2026-05-13): the icepython search resolves these
symbols and ``get_timeseries_fields`` returns the full field list,
but ``get_timeseries`` and ``get_quotes`` return empty for every RGGI
contract on this subscription -- both IUSE and NYMG appear
unentitled. The runner will upsert zero rows in that state; once the
RGGI add-on is enabled the same code starts producing data without
changes.

Each entry has:
    symbol        – the ICE symbol code passed to get_timeseries
    description   – human-readable label for logging and audits
    product_type  – "carbon"
    contract_type – "vintage_future", "acp_future", or "index"
    vintage       – calendar year the allowance is valid for, or None
                    (for the auction-index continuous symbol)
    exchange      – "IUSE" or "NYMG"
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


RGGI_FUTURES_SYMBOLS: list[dict] = [
    # -- IFED auction-clearing-price continuous index --------------------------
    # Single best symbol for a "live RGGI mark" -- tracks the published
    # quarterly auction clearing price between auctions.
    {
        "symbol": "RCP_P A0-IUS",
        "description": "RGGI Futures Auction Index (continuous)",
        "product_type": "carbon",
        "contract_type": "index",
        "vintage": None,
        "exchange": "IUSE",
    },
    # -- IFED ACP futures (Mar/Jun/Sep/Dec cycle) ------------------------------
    {
        "symbol": "RCP H27-IUS",
        "description": "RGGI ACP Future - Mar 2027",
        "product_type": "carbon",
        "contract_type": "acp_future",
        "vintage": None,
        "exchange": "IUSE",
    },
    {
        "symbol": "RCP M27-IUS",
        "description": "RGGI ACP Future - Jun 2027",
        "product_type": "carbon",
        "contract_type": "acp_future",
        "vintage": None,
        "exchange": "IUSE",
    },
    {
        "symbol": "RCP U27-IUS",
        "description": "RGGI ACP Future - Sep 2027",
        "product_type": "carbon",
        "contract_type": "acp_future",
        "vintage": None,
        "exchange": "IUSE",
    },
    {
        "symbol": "RCP Z27-IUS",
        "description": "RGGI ACP Future - Dec 2027",
        "product_type": "carbon",
        "contract_type": "acp_future",
        "vintage": None,
        "exchange": "IUSE",
    },
    # -- IFED vintage monthly futures (most-liquid forwards) -------------------
    # Liquid trade tends to concentrate in the Dec contract of each
    # vintage year; seed with the prompt + 1y forward for active
    # vintages. Add more months by extending this list.
    {
        "symbol": "RJ5 Z26-IUS",
        "description": "RGGI V2025 Future - Dec 2026",
        "product_type": "carbon",
        "contract_type": "vintage_future",
        "vintage": 2025,
        "exchange": "IUSE",
    },
    {
        "symbol": "RJ6 Z26-IUS",
        "description": "RGGI V2026 Future - Dec 2026",
        "product_type": "carbon",
        "contract_type": "vintage_future",
        "vintage": 2026,
        "exchange": "IUSE",
    },
    {
        "symbol": "RJ6 Z27-IUS",
        "description": "RGGI V2026 Future - Dec 2027",
        "product_type": "carbon",
        "contract_type": "vintage_future",
        "vintage": 2026,
        "exchange": "IUSE",
    },
    {
        "symbol": "RJ7 Z27-IUS",
        "description": "RGGI V2027 Future - Dec 2027",
        "product_type": "carbon",
        "contract_type": "vintage_future",
        "vintage": 2027,
        "exchange": "IUSE",
    },
    # -- NYMG (CME competing listing) -- kept for venue comparison -------------
    {
        "symbol": "RGI Z26",
        "description": "CME CARBON RGGI EMISSIONS - Dec 2026",
        "product_type": "carbon",
        "contract_type": "vintage_future",
        "vintage": None,  # NYMG listing is not vintage-segmented
        "exchange": "NYMG",
    },
    {
        "symbol": "RGI Z27",
        "description": "CME CARBON RGGI EMISSIONS - Dec 2027",
        "product_type": "carbon",
        "contract_type": "vintage_future",
        "vintage": None,
        "exchange": "NYMG",
    },
]


def get_rggi_futures_symbols() -> list[dict]:
    """Return all configured RGGI futures symbol entries."""
    return RGGI_FUTURES_SYMBOLS


def get_rggi_futures_symbol_codes(
    symbol_entries: list[dict] | None = None,
) -> list[str]:
    """Return just the symbol strings for API calls."""
    entries = symbol_entries or RGGI_FUTURES_SYMBOLS
    return [entry["symbol"] for entry in entries]


def get_rggi_futures_symbol_map() -> dict[str, dict]:
    """Return RGGI futures symbols keyed by ICE symbol code."""
    return {entry["symbol"]: entry for entry in RGGI_FUTURES_SYMBOLS}


def log_all_symbols(symbol_entries: list[dict] | None = None) -> None:
    entries = symbol_entries or get_rggi_futures_symbols()
    logger.info("Configured %s RGGI futures symbols", len(entries))
    for entry in entries:
        logger.info(
            "%s | %s | vintage=%s | %s",
            entry["symbol"],
            entry["description"],
            entry.get("vintage"),
            entry.get("exchange", "?"),
        )


def _main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    log_all_symbols()


if __name__ == "__main__":
    _main()
