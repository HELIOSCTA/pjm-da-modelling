"""Build the run-JSON payload for the like_day_model_knn_sunny forecaster
(model_name ``pjm_rto_hourly_sunny``) and extract its OnPeak forecast for the
shared publisher.

See ``modelling/da_models/common/publish.py`` for the upsert. The sunny variant
is a KNN like-day forecaster (single hub, per-HE point + quantiles, weighted
analog days), so this ``publish.py`` mirrors
``like_day_model_knn/pjm_rto_hourly/publish.py``. ``build_payload`` builds the
model-specific payload; ``extract_onpeak_forecast`` pulls the OnPeak HE 8-23
point forecast out of the payload's ``blocks[]`` array. (It lives at the family
root rather than under ``pjm_rto_hourly/`` so a future sunny sibling can reuse
it without crossing the forward-import boundary into ``like_day_model_knn``.)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from da_models.common.configs import HOURS
from da_models.common.forecast.output import actuals_from_pool

logger = logging.getLogger(__name__)

# OnPeak hours for analog DA OnPeak LMP averaging. Matches the convention
# in common/forecast/output.py (HE8..HE23) used everywhere else on the desk.
_ONPEAK_HOURS: list[int] = list(range(8, 24))


def _round(value: Any, ndigits: int = 3) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    if pd.isna(value):
        return None
    return round(float(value), ndigits)


def _build_hourly(
    df_forecast: pd.DataFrame,
    actuals_hourly: dict[int, float] | None,
    quantiles: list[float],
) -> list[dict[str, Any]]:
    """Reshape df_forecast (24 rows, q_0.10..q_0.90) into the JSON hourly array.

    Always emits 24 entries (HE1..HE24), filling missing values with None.
    """
    fc_by_he: dict[int, dict[str, float]] = {}
    point_col = (
        "point_forecast" if "point_forecast" in df_forecast.columns else "q_0.50"
    )
    for _, r in df_forecast.iterrows():
        h = int(r["hour_ending"])
        row: dict[str, float] = {"point_forecast": r.get(point_col)}
        for q in quantiles:
            row[f"q{int(round(q * 100)):02d}"] = r.get(f"q_{q:.2f}")
        fc_by_he[h] = row

    out: list[dict[str, Any]] = []
    for h in HOURS:
        row = fc_by_he.get(h, {})
        entry: dict[str, Any] = {
            "hour_ending": h,
            "point_forecast": _round(row.get("point_forecast")),
        }
        for q in quantiles:
            key = f"q{int(round(q * 100)):02d}"
            entry[key] = _round(row.get(key))
        entry["actual_lmp"] = _round(actuals_hourly.get(h)) if actuals_hourly else None
        out.append(entry)
    return out


_BLOCK_NAMES: tuple[str, ...] = ("OnPeak", "OffPeak", "Flat")
_QUANTILE_LABELS: tuple[str, ...] = ("P10", "P25", "P50", "Forecast", "P75", "P90")


def _build_blocks(quantiles_table: pd.DataFrame) -> list[dict[str, Any]]:
    """Extract OnPeak / OffPeak / Flat x P10..P90+Forecast (18 rows)."""
    out: list[dict[str, Any]] = []
    if quantiles_table is None or len(quantiles_table) == 0:
        return out
    type_col = "Type" if "Type" in quantiles_table.columns else None
    if type_col is None:
        return out
    for label in _QUANTILE_LABELS:
        rows = quantiles_table[quantiles_table[type_col] == label]
        if len(rows) == 0:
            continue
        rec = rows.iloc[0]
        for block in _BLOCK_NAMES:
            value = rec.get(block)
            out.append(
                {
                    "block": block,
                    "quantile_label": label,
                    "value": _round(value),
                }
            )
    return out


def _build_analogs(
    analogs: pd.DataFrame,
    pool: pd.DataFrame,
    target_date: date,
) -> list[dict[str, Any]]:
    """Aggregate the 24xK analog picks into one row per unique analog date.

    weight_share is normalised so the array sums to 1.0 across all unique
    analog dates (matches the printer's ``w`` column). da_onpk_lmp is the
    realised OnPeak DA LMP (HE8..HE23 mean) for the analog date, looked
    up via ``actuals_from_pool``.
    """
    if analogs is None or len(analogs) == 0:
        return []

    grp = (
        analogs.groupby("date", dropna=True)
        .agg(
            sum_weight=("weight", "sum"),
            mean_distance=("distance", "mean"),
            hes_contributed=("hour_ending", "nunique"),
        )
        .reset_index()
    )
    total_weight = float(grp["sum_weight"].sum())
    if total_weight <= 0:
        grp["weight_share"] = 0.0
    else:
        grp["weight_share"] = grp["sum_weight"] / total_weight

    grp = grp.sort_values("weight_share", ascending=False).reset_index(drop=True)
    grp.insert(0, "rank", grp.index + 1)

    target_ts = pd.Timestamp(target_date)

    out: list[dict[str, Any]] = []
    for _, r in grp.iterrows():
        analog_date = r["date"]
        # `date` from the analogs frame is already a python date or pandas
        # Timestamp; coerce to date for downstream consistency.
        if isinstance(analog_date, pd.Timestamp):
            analog_date_obj = analog_date.date()
        else:
            analog_date_obj = pd.Timestamp(analog_date).date()

        actuals = actuals_from_pool(pool, analog_date_obj)
        da_onpk: float | None = None
        if actuals is not None:
            onpk_vals = [actuals[h] for h in _ONPEAK_HOURS if h in actuals]
            if onpk_vals:
                da_onpk = float(np.mean(onpk_vals))

        analog_ts = pd.Timestamp(analog_date_obj)
        day_diff = int((target_ts - analog_ts).days)
        out.append(
            {
                "rank": int(r["rank"]),
                "analog_date": analog_date_obj.isoformat(),
                "day_of_week": analog_ts.strftime("%a"),
                "day_diff": day_diff,
                "weight_share": _round(r["weight_share"], ndigits=4),
                "hes_contributed": int(r["hes_contributed"]),
                "da_onpk_lmp": _round(da_onpk),
            }
        )
    return out


def build_payload(
    *,
    df_forecast: pd.DataFrame,
    quantiles_table: pd.DataFrame,
    analogs: pd.DataFrame,
    pool: pd.DataFrame,
    output_table: pd.DataFrame,
    target_date: date,
    run_date: date,
    model_name: str,
    model_family: str,
    run_id: str,
    hub: str,
    day_type: str,
    n_analogs: int,
    quantiles: list[float],
    created_at_utc: datetime | None = None,
    created_at_local: datetime | None = None,
) -> dict[str, Any]:
    """Build the run-JSON payload (dict). Pure function; no IO.

    ``created_at_utc`` / ``created_at_local`` default to "now" -- pass
    explicit values when you want the payload's timestamps to match
    the row's columns.
    """
    del output_table  # actuals are pulled from the pool below

    actuals_hourly = actuals_from_pool(pool, target_date)
    n_unique_analog_dates = (
        int(analogs["date"].nunique()) if analogs is not None and len(analogs) else 0
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
        "day_type": day_type,
        "n_analogs": int(n_analogs),
        "n_unique_analog_dates": n_unique_analog_dates,
        "created_at_utc": created_at_utc.isoformat().replace("+00:00", "Z"),
        "created_at_local": created_at_local.isoformat(),
        "hourly": _build_hourly(df_forecast, actuals_hourly, quantiles),
        "blocks": _build_blocks(quantiles_table),
        "analogs": _build_analogs(analogs, pool, target_date),
    }


def extract_onpeak_forecast(payload: dict) -> float | None:
    """Pull the OnPeak HE 8-23 point forecast out of ``payload['blocks']``.

    KNN payloads encode blocks as ``{block, quantile_label, value}``;
    the headline number is the row with ``block='OnPeak'`` and
    ``quantile_label='Forecast'`` (the deterministic point -- see
    ``_QUANTILE_LABELS``). Returns None when the block is missing or
    the value is None.
    """
    blocks = payload.get("blocks") or []
    for entry in blocks:
        if entry.get("block") == "OnPeak" and entry.get("quantile_label") == "Forecast":
            value = entry.get("value")
            return float(value) if value is not None else None
    return None
