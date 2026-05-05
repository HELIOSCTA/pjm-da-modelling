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


_ONPEAK_HOURS: list[int] = list(range(8, 24))                       # HE8..HE23
_OFFPEAK_HOURS: list[int] = list(range(1, 8)) + [24]                # HE1..HE7, HE24


def actuals_from_pool(pool: pd.DataFrame, target_date: date) -> dict[int, float] | None:
    """Lookup the target date's hourly DA LMPs in the wide pool, if available.

    Returns ``None`` when the date is missing from the pool or when any
    hour value is NaN. Callers treat ``None`` as "no actuals available
    yet" (e.g. forward-looking forecasts before delivery).
    """
    row = pool[pool["date"] == target_date]
    if len(row) == 0:
        return None
    rec = row.iloc[0]
    out: dict[int, float] = {}
    for h in HOURS:
        v = rec.get(f"lmp_h{h}")
        if v is None or pd.isna(v):
            return None
        out[h] = float(v)
    return out


def add_summary_cols(row_dict: dict) -> dict:
    """Append OnPeak / OffPeak / Flat means to an HE-keyed row dict."""
    onpeak = [row_dict[f"HE{h}"] for h in _ONPEAK_HOURS
              if pd.notna(row_dict.get(f"HE{h}"))]
    offpeak = [row_dict[f"HE{h}"] for h in _OFFPEAK_HOURS
               if pd.notna(row_dict.get(f"HE{h}"))]
    allv = [row_dict[f"HE{h}"] for h in range(1, 25)
            if pd.notna(row_dict.get(f"HE{h}"))]
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

    cols = ["Date", "Type"] + [f"HE{h}" for h in range(1, 25)] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)
