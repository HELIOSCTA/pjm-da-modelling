"""Hourly forecast aggregation for per_hour - per-hour analogs.

Differs from the per_day_* day-analog aggregation: per_hour's analogs are
per-(date, hour) tuples, so this module groups by hour_ending and computes
weighted averages within each hour's own ensemble.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from da_models.knn_model_only_load import configs


def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    idx = np.argsort(values)
    v = values[idx]
    w = weights[idx]
    cdf = np.cumsum(w)
    cdf = cdf / cdf[-1]
    return float(np.interp(q, cdf, v))


def hourly_forecast_from_hour_analogs(
    analogs: pd.DataFrame,
    quantiles: list[float],
) -> pd.DataFrame:
    """Aggregate per-(hour, rank) analog tuples into a 24-hour forecast.

    Expects ``analogs`` with columns: hour_ending, weight, lmp.
    Group by hour_ending and produce a weighted point + quantiles per HE.
    """
    if len(analogs) == 0 or not {"hour_ending", "weight", "lmp"}.issubset(analogs.columns):
        return pd.DataFrame()

    rows: list[dict] = []
    for h in configs.HOURS:
        sub = analogs[analogs["hour_ending"] == h].dropna(subset=["lmp"])
        if len(sub) == 0:
            continue
        values = sub["lmp"].to_numpy(dtype=float)
        w = sub["weight"].to_numpy(dtype=float)
        if w.sum() <= 0:
            continue
        w = w / w.sum()
        row = {"hour_ending": h, "point_forecast": float(np.average(values, weights=w))}
        for q in quantiles:
            row[f"q_{q:.2f}"] = weighted_quantile(values, w, q)
        rows.append(row)
    return pd.DataFrame(rows)


def actuals_from_pool(pool: pd.DataFrame, target_date: date) -> dict[int, float] | None:
    """Lookup the target date's hourly DA LMPs in the pool, if available."""
    row = pool[pool["date"] == target_date]
    if len(row) == 0:
        return None
    rec = row.iloc[0]
    out: dict[int, float] = {}
    for h in configs.HOURS:
        v = rec.get(f"lmp_h{h}")
        if v is None or pd.isna(v):
            return None
        out[h] = float(v)
    return out
