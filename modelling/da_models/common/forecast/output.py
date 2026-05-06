"""Display-table helpers shared across model families.

Pivots per-hour forecast frames into the canonical
Date | Type | HE1..HE24 | OnPeak | OffPeak | Flat layout, with HE6-22
treated as on-peak per the PJM convention used throughout the desk's
deliverables.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from da_models.common.configs import HOURS


_ONPEAK_HOURS: list[int] = list(range(8, 24))  # HE8..HE23
_OFFPEAK_HOURS: list[int] = list(range(1, 8)) + [24]  # HE1..HE7, HE24


def actuals_from_pool(pool: pd.DataFrame, target_date: date) -> dict[int, float] | None:
    """Lookup the target date's hourly DA LMPs from a wide OR long pool.

    Wide pool detection: row keyed by ``date``, with ``lmp_h{1..24}`` cols.
    Long pool detection: rows keyed by (``date``, ``hour_ending``) with a
    single scalar ``lmp`` col (post-T4 like_day_model_knn convention).

    Returns ``None`` when the date is missing from the pool or any HE has
    NaN actuals. Callers treat ``None`` as "no actuals available yet".
    """
    rows = pool[pool["date"] == target_date]
    if len(rows) == 0:
        return None

    # Long pool: one row per HE with scalar ``lmp`` col.
    if "hour_ending" in pool.columns and "lmp" in pool.columns:
        out: dict[int, float] = {}
        seen: set[int] = set()
        for _, r in rows.iterrows():
            h = int(r["hour_ending"])
            v = r.get("lmp")
            if v is None or pd.isna(v):
                return None
            out[h] = float(v)
            seen.add(h)
        # Require all 24 HEs.
        if not set(HOURS).issubset(seen):
            return None
        return out

    # Wide pool: one row per date with lmp_h1..lmp_h24 cols.
    rec = rows.iloc[0]
    out_wide: dict[int, float] = {}
    for h in HOURS:
        v = rec.get(f"lmp_h{h}")
        if v is None or pd.isna(v):
            return None
        out_wide[h] = float(v)
    return out_wide


def add_summary_cols(row_dict: dict) -> dict:
    """Append OnPeak / OffPeak / Flat means to an HE-keyed row dict."""
    onpeak = [
        row_dict[f"HE{h}"] for h in _ONPEAK_HOURS if pd.notna(row_dict.get(f"HE{h}"))
    ]
    offpeak = [
        row_dict[f"HE{h}"] for h in _OFFPEAK_HOURS if pd.notna(row_dict.get(f"HE{h}"))
    ]
    allv = [
        row_dict[f"HE{h}"] for h in range(1, 25) if pd.notna(row_dict.get(f"HE{h}"))
    ]
    row_dict["OnPeak"] = float(np.mean(onpeak)) if onpeak else float("nan")
    row_dict["OffPeak"] = float(np.mean(offpeak)) if offpeak else float("nan")
    row_dict["Flat"] = float(np.mean(allv)) if allv else float("nan")
    return row_dict


def build_output_table(
    target_date: date,
    df_forecast: pd.DataFrame,
    actuals_hourly: dict[int, float] | None = None,
) -> pd.DataFrame:
    """Pivot per-HE forecast into the Date | Type | HE1..HE24 | OnPeak | OffPeak | Flat shape.

    Rows: Actual (if actuals supplied) / Forecast / Error (if actuals supplied).
    """
    forecast_hourly = {
        int(r["hour_ending"]): float(r["point_forecast"])
        for _, r in df_forecast.iterrows()
        if pd.notna(r.get("point_forecast"))
    }

    rows: list[dict] = []
    if actuals_hourly is not None:
        actual_row = {"Date": target_date, "Type": "Actual"}
        for h in range(1, 25):
            actual_row[f"HE{h}"] = actuals_hourly.get(h)
        rows.append(add_summary_cols(actual_row))

    forecast_row = {"Date": target_date, "Type": "Forecast"}
    for h in range(1, 25):
        forecast_row[f"HE{h}"] = forecast_hourly.get(h)
    rows.append(add_summary_cols(forecast_row))

    if actuals_hourly is not None:
        error_row = {"Date": target_date, "Type": "Error"}
        for h in range(1, 25):
            a = actuals_hourly.get(h)
            f = forecast_hourly.get(h)
            error_row[f"HE{h}"] = (f - a) if (a is not None and f is not None) else None
        rows.append(add_summary_cols(error_row))

    cols = (
        ["Date", "Type"]
        + [f"HE{h}" for h in range(1, 25)]
        + ["OnPeak", "OffPeak", "Flat"]
    )
    return pd.DataFrame(rows, columns=cols)
