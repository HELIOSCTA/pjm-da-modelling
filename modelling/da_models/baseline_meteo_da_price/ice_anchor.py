"""ICE PJM DA Next Day ticker → VWAP → multiplicative anchor.

Pure helpers for pulling tick-level fills of ``PDA D1-IUS`` from
``pjm_da_modelling_cleaned.ice_python_ticker_data``, computing a
qty-weighted VWAP on principal trades only (Lift + Hit; Leg / null /
Spread excluded), and rescaling a Meteologica hourly profile so that
its OnPeak (HE8-23) mean matches the ICE VWAP.

This module is I/O-light by design: ``fetch_ice_ticker_trades`` is the
only function that hits the database; the rest are pure transforms
exercised by both the live anchored pipeline and the backtest.
"""

from __future__ import annotations

import sys
import warnings
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg2

# modelling/credentials.py is a flat module (not part of the da_models
# editable install). Load it lazily inside _connect() so this module
# stays importable in unit-test / IDE contexts that don't have
# modelling/ on sys.path.
_MODELLING_ROOT_FOR_CREDENTIALS = Path(__file__).resolve().parents[2]


# Trade-direction filter for VWAP construction. Matches the dbt mapping
# in source_v1_ticker_data.sql: SetByBid -> Lift, SetByAsk -> Hit, Leg
# stays as 'Leg', everything else (including outright spread fills and
# unmapped rows) is null/'nan'. Only Lift + Hit are principal fills
# that move the market level — Leg's are spread legs whose absolute
# price is not the level signal.
LIVE_TRADE_DIRECTIONS: frozenset[str] = frozenset({"Lift", "Hit"})

# OnPeak block per the desk convention (HE8..HE23).
ONPEAK_HOURS: tuple[int, ...] = tuple(range(8, 24))

DEFAULT_SYMBOL: str = "PDA D1-IUS"

_BASE_QUERY = """
SELECT
    exec_time_local,
    trade_date,
    start_date,
    end_date,
    symbol,
    description,
    product_type,
    contract_type,
    strip,
    price,
    quantity,
    trade_direction
FROM pjm_da_modelling_cleaned.ice_python_ticker_data
WHERE
    start_date = %(start_date)s
    AND symbol = %(symbol)s
"""


@dataclass(frozen=True)
class VwapResult:
    """Outcome of a VWAP computation, including book-keeping."""

    vwap: float | None
    volume: float
    n_trades: int
    n_excluded: int
    last_price: float | None
    last_time: pd.Timestamp | None


def _connect():
    if str(_MODELLING_ROOT_FOR_CREDENTIALS) not in sys.path:
        sys.path.insert(0, str(_MODELLING_ROOT_FOR_CREDENTIALS))
    import credentials  # noqa: PLC0415  modelling/credentials.py (flat module)

    return psycopg2.connect(
        user=credentials.AZURE_POSTGRESQL_DB_USER,
        password=credentials.AZURE_POSTGRESQL_DB_PASSWORD,
        host=credentials.AZURE_POSTGRESQL_DB_HOST,
        port=credentials.AZURE_POSTGRESQL_DB_PORT,
        dbname=credentials.AZURE_POSTGRESQL_DB_NAME,
    )


def fetch_ice_ticker_trades(
    delivery_date: date,
    symbol: str = DEFAULT_SYMBOL,
    cutoff_local: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Pull tick-level trades whose contract delivers on ``delivery_date``.

    Filters by ``start_date = delivery_date`` (contract delivery day),
    so this transparently handles weekend/holiday gaps where the trade
    happened more than one calendar day before delivery. Returns
    columns: ``exec_time_local``, ``trade_date``, ``start_date``,
    ``symbol``, ``price``, ``quantity``, ``trade_direction``. Returns
    an empty frame (with the expected columns) when no rows match.
    """
    params: dict = {"start_date": delivery_date, "symbol": symbol}
    query = _BASE_QUERY
    if cutoff_local is not None:
        query = query + "\n    AND exec_time_local <= %(cutoff_local)s"
        params["cutoff_local"] = cutoff_local
    query = query + "\nORDER BY exec_time_local ASC"

    with warnings.catch_warnings():
        # pandas warns when read_sql is given a DB-API connection.
        warnings.simplefilter("ignore", UserWarning)
        with _connect() as conn:
            return pd.read_sql(query, conn, params=params)


def compute_vwap(
    trades: pd.DataFrame,
    directions: Iterable[str] = LIVE_TRADE_DIRECTIONS,
) -> VwapResult:
    """Qty-weighted VWAP over rows whose ``trade_direction`` is in ``directions``.

    All other rows (including null / NaN direction) are counted as
    ``n_excluded`` and not contributed to numerator or denominator.
    Returns ``vwap=None`` when no eligible trades — the caller's signal
    to skip anchoring.
    """
    if trades.empty:
        return VwapResult(None, 0.0, 0, 0, None, None)

    direction_set = set(directions)
    mask = trades["trade_direction"].isin(direction_set)
    eligible = trades.loc[mask].copy()
    n_excluded = int(len(trades) - len(eligible))

    if eligible.empty:
        return VwapResult(None, 0.0, 0, n_excluded, None, None)

    eligible["price"] = pd.to_numeric(eligible["price"], errors="coerce")
    eligible["quantity"] = pd.to_numeric(eligible["quantity"], errors="coerce")
    eligible = eligible.dropna(subset=["price", "quantity"])
    eligible = eligible[eligible["quantity"] > 0]
    if eligible.empty:
        return VwapResult(None, 0.0, 0, n_excluded, None, None)

    notional = float((eligible["price"] * eligible["quantity"]).sum())
    volume = float(eligible["quantity"].sum())
    vwap = notional / volume if volume > 0 else None
    last_row = eligible.sort_values("exec_time_local").iloc[-1]
    return VwapResult(
        vwap=vwap,
        volume=volume,
        n_trades=int(len(eligible)),
        n_excluded=n_excluded,
        last_price=float(last_row["price"]),
        last_time=pd.Timestamp(last_row["exec_time_local"]),
    )


def onpeak_mean(hourly: dict[int, float]) -> float | None:
    """Mean over the OnPeak hours (HE8..HE23). Returns None if any HE
    is missing or non-finite. Strict on purpose — silent NaN propagation
    would produce a meaningless multiplier."""
    vals: list[float] = []
    for h in ONPEAK_HOURS:
        v = hourly.get(h)
        if v is None or pd.isna(v):
            return None
        vals.append(float(v))
    return sum(vals) / len(vals) if vals else None


def onpeak_multiplier(meteo_hourly: dict[int, float], ice_vwap: float) -> float:
    """``scale = ice_vwap / mean(meteo_hourly[h] for h in OnPeak)``.

    Raises ``ValueError`` when the OnPeak mean is missing, non-positive,
    or otherwise unsuitable as a denominator.
    """
    onpk = onpeak_mean(meteo_hourly)
    if onpk is None:
        raise ValueError("Meteo OnPeak mean is missing — cannot compute multiplier.")
    if onpk <= 0:
        raise ValueError(
            f"Meteo OnPeak mean {onpk:.4f} <= 0 — multiplicative anchor undefined."
        )
    return float(ice_vwap) / onpk


def apply_multiplier(hourly: dict[int, float], scale: float) -> dict[int, float]:
    """Multiply every HE value by ``scale``. NaN inputs propagate as NaN."""
    return {h: float(v) * float(scale) if pd.notna(v) else v for h, v in hourly.items()}


__all__ = [
    "LIVE_TRADE_DIRECTIONS",
    "ONPEAK_HOURS",
    "DEFAULT_SYMBOL",
    "VwapResult",
    "fetch_ice_ticker_trades",
    "compute_vwap",
    "onpeak_mean",
    "onpeak_multiplier",
    "apply_multiplier",
]
