"""Build the run-JSON payload for the ICE-anchored Meteologica DA-price
baseline and extract its OnPeak forecast for the shared publisher.

See ``modelling/da_models/common/publish.py`` for the upsert. This
module exposes ``build_payload`` (model-specific payload construction
including the ICE anchor block + ENS bands + trade list) and
``extract_onpeak_forecast`` (pulls the deterministic OnPeak HE 8-23
forecast out of the payload's ``blocks[]`` array).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from backend.modelling.da_models.baseline_meteo_da_price import ice_anchor
from backend.modelling.da_models.common.configs import HOURS

logger = logging.getLogger(__name__)


# Member-column predicate matches printers._member_columns: the 51
# numeric ECMWF members are da_price_ens_NN; ENS Avg / Bottom / Top
# share the prefix and must be excluded.
def _member_columns(df: pd.DataFrame) -> list[str]:
    prefix = "da_price_ens_"
    return [
        c for c in df.columns if c.startswith(prefix) and c[len(prefix) :].isdigit()
    ]


def _round(value: Any, ndigits: int = 3) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    if pd.isna(value):
        return None
    return round(float(value), ndigits)


def _iso_or_none(ts: Any) -> str | None:
    if ts is None:
        return None
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts).isoformat()


def _hourly_dict(df: pd.DataFrame, col: str) -> dict[int, float]:
    if col not in df.columns or df.empty:
        return {}
    out: dict[int, float] = {}
    for _, r in df.iterrows():
        h = int(r["hour_ending"])
        v = r.get(col)
        if pd.isna(v):
            continue
        out[h] = float(v)
    return out


def _members_quantile_by_he(
    df: pd.DataFrame,
    quantile: float,
) -> dict[int, float]:
    """Per-HE quantile across the 51 ECMWF members in ``df``.

    Uses linear interpolation (numpy default) to match printers'
    members-fan view. Returns {} when df is empty or member columns
    are missing — caller emits None for those entries.
    """
    if df.empty:
        return {}
    cols = _member_columns(df)
    if not cols:
        return {}
    out: dict[int, float] = {}
    for _, r in df.iterrows():
        h = int(r["hour_ending"])
        vals = pd.to_numeric(r[cols], errors="coerce").dropna().to_numpy(dtype=float)
        if len(vals) == 0:
            continue
        out[h] = float(np.quantile(vals, quantile))
    return out


def _build_hourly(
    df_for_fan: pd.DataFrame,
    actuals_hourly: dict[int, float] | None,
) -> list[dict[str, Any]]:
    """Reshape the (already scaled) forecast frame into 24 HE entries.

    Always emits HE1..HE24, filling missing values with None. ``df_for_fan``
    is the SCALED frame from the runner (or the raw frame when no anchor
    was applied) — this publisher does not re-scale.
    """
    point = _hourly_dict(df_for_fan, "da_price_deterministic")
    ens_avg = _hourly_dict(df_for_fan, "da_price_ens_average")
    ens_bot = _hourly_dict(df_for_fan, "da_price_ens_bottom")
    ens_top = _hourly_dict(df_for_fan, "da_price_ens_top")
    members_p25 = _members_quantile_by_he(df_for_fan, 0.25)
    members_p75 = _members_quantile_by_he(df_for_fan, 0.75)

    out: list[dict[str, Any]] = []
    for h in HOURS:
        out.append(
            {
                "hour_ending": h,
                "point_forecast": _round(point.get(h)),
                "ens_avg": _round(ens_avg.get(h)),
                "ens_bottom": _round(ens_bot.get(h)),
                "ens_top": _round(ens_top.get(h)),
                "members_p25": _round(members_p25.get(h)),
                "members_p75": _round(members_p75.get(h)),
                "actual_lmp": (
                    _round(actuals_hourly.get(h)) if actuals_hourly else None
                ),
            }
        )
    return out


_BLOCK_NAMES: tuple[str, ...] = ("OnPeak", "OffPeak", "Flat")
_SERIES_LABELS: tuple[str, ...] = ("Det", "ENS Avg", "ENS Bottom", "ENS Top")


def _build_blocks(bands_table: pd.DataFrame) -> list[dict[str, Any]]:
    """Extract block summaries (OnPeak / OffPeak / Flat) for each named series."""
    out: list[dict[str, Any]] = []
    if bands_table is None or len(bands_table) == 0:
        return out
    if "Type" not in bands_table.columns:
        return out
    for label in _SERIES_LABELS:
        rows = bands_table[bands_table["Type"] == label]
        if len(rows) == 0:
            continue
        rec = rows.iloc[0]
        for block in _BLOCK_NAMES:
            out.append(
                {
                    "series": label,
                    "block": block,
                    "value": _round(rec.get(block)),
                }
            )
    return out


def _build_ice_anchor(
    symbol: str,
    vwap_result: ice_anchor.VwapResult | None,
    cutoff: pd.Timestamp | None,
    shared_scale: float | None,
    anchor_label: str | None,
    implied_multipliers: dict[str, float] | None,
) -> dict[str, Any]:
    """ICE anchor metadata block. ``applied`` says whether the bands
    above are scaled (True) or fell back to raw Meteo (False)."""
    block: dict[str, Any] = {
        "symbol": symbol,
        "cutoff_local": _iso_or_none(cutoff),
        "applied": shared_scale is not None,
        "shared_scale": _round(shared_scale, ndigits=6) if shared_scale else None,
        "anchor_label": anchor_label,
        "vwap": None,
        "volume": None,
        "n_trades": 0,
        "n_excluded": 0,
        "last_price": None,
        "last_time_local": None,
        "implied_multipliers": (
            {k: _round(v, ndigits=6) for k, v in implied_multipliers.items()}
            if implied_multipliers
            else {}
        ),
    }
    if vwap_result is not None:
        block.update(
            {
                "vwap": _round(vwap_result.vwap),
                "volume": _round(vwap_result.volume, ndigits=2),
                "n_trades": int(vwap_result.n_trades),
                "n_excluded": int(vwap_result.n_excluded),
                "last_price": _round(vwap_result.last_price),
                "last_time_local": _iso_or_none(vwap_result.last_time),
            }
        )
    return block


_TRADE_COLUMNS: tuple[str, ...] = (
    "exec_time_local",
    "price",
    "quantity",
    "trade_direction",
)


def _build_trades(trades: pd.DataFrame) -> list[dict[str, Any]]:
    """Per-trade list, sorted ascending by execution time. Empty when no trades."""
    if trades is None or trades.empty:
        return []
    cols = [c for c in _TRADE_COLUMNS if c in trades.columns]
    df = trades[cols].copy().sort_values("exec_time_local")
    out: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        out.append(
            {
                "exec_time_local": _iso_or_none(r.get("exec_time_local")),
                "price": _round(r.get("price")),
                "quantity": _round(r.get("quantity"), ndigits=2),
                "trade_direction": (
                    None
                    if pd.isna(r.get("trade_direction"))
                    else str(r.get("trade_direction"))
                ),
            }
        )
    return out


def build_payload(
    *,
    df_for_fan: pd.DataFrame,
    bands_table_scaled: pd.DataFrame,
    bands_table_raw: pd.DataFrame,
    actuals_hourly: dict[int, float] | None,
    trades: pd.DataFrame,
    vwap_result: ice_anchor.VwapResult | None,
    target_date: date,
    run_date: date,
    model_name: str,
    model_family: str,
    run_id: str,
    hub: str,
    lead_days: int | None,
    det_exec: pd.Timestamp | None,
    ens_exec: pd.Timestamp | None,
    ice_symbol: str,
    ice_cutoff: pd.Timestamp | None,
    shared_scale: float | None,
    anchor_label: str | None,
    implied_multipliers: dict[str, float] | None,
    created_at_utc: datetime | None = None,
    created_at_local: datetime | None = None,
) -> dict[str, Any]:
    """Build the run-JSON payload (dict). Pure function; no IO.

    When ``shared_scale`` is None (no eligible ICE trades, or neither
    Det nor ENS Avg has a usable OnPeak), the published bands fall back
    to ``bands_table_raw`` and ``ice_anchor.applied`` is False.

    ``created_at_utc`` / ``created_at_local`` default to "now" — pass
    explicit values when you want the payload's timestamps to match the
    row's columns.
    """
    bands_for_publish = (
        bands_table_scaled if shared_scale is not None else bands_table_raw
    )
    if created_at_utc is None:
        created_at_utc = datetime.now(timezone.utc).replace(microsecond=0)
    if created_at_local is None:
        # Naive local-machine wall-clock time. Stored in Postgres as
        # `timestamp without time zone` so the literal MST value is
        # preserved without conversion.
        created_at_local = datetime.now().replace(microsecond=0)

    return {
        "target_date": str(target_date),
        "run_date": str(run_date),
        "model_name": model_name,
        "model_family": model_family,
        "run_id": run_id,
        "hub": hub,
        "lead_days": lead_days,
        "det_executed_local": _iso_or_none(det_exec),
        "ens_executed_local": _iso_or_none(ens_exec),
        "created_at_utc": created_at_utc.isoformat().replace("+00:00", "Z"),
        "created_at_local": created_at_local.isoformat(),
        "ice_anchor": _build_ice_anchor(
            symbol=ice_symbol,
            vwap_result=vwap_result,
            cutoff=ice_cutoff,
            shared_scale=shared_scale,
            anchor_label=anchor_label,
            implied_multipliers=implied_multipliers,
        ),
        "hourly": _build_hourly(df_for_fan, actuals_hourly),
        "blocks": _build_blocks(bands_for_publish),
        "ice_trades": _build_trades(trades),
    }


def extract_onpeak_forecast(payload: dict) -> float | None:
    """Pull the OnPeak HE 8-23 deterministic forecast out of ``payload['blocks']``.

    Baseline payloads encode blocks as ``{series, block, value}`` (note
    the divergence from the KNN families' ``quantile_label`` shape). The
    headline number is ``block='OnPeak' AND series='Det'`` — the
    deterministic ICE-anchored point. ``ENS Avg`` would be the natural
    fallback but isn't used here to keep the comparand pinned to the
    point forecast. Returns None when the block is missing or the value
    is None.
    """
    blocks = payload.get("blocks") or []
    for entry in blocks:
        if entry.get("block") == "OnPeak" and entry.get("series") == "Det":
            value = entry.get("value")
            return float(value) if value is not None else None
    return None
