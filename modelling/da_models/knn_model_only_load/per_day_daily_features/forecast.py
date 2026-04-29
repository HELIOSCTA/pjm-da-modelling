"""Hourly forecast aggregation for per_day_daily_features - per-day analogs."""
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


def hourly_forecast_from_day_analogs(
    analogs: pd.DataFrame,
    quantiles: list[float],
) -> pd.DataFrame:
    """Aggregate top-N day-level analogs into a 24-hour forecast.

    For each HE, compute the inverse-distance-weighted average and weighted
    empirical quantiles across the analogs' ``lmp_h{HE}`` columns.
    """
    rows: list[dict] = []
    for h in configs.HOURS:
        col = f"lmp_h{h}"
        if col not in analogs.columns:
            continue
        hour = analogs[["weight", col]].dropna(subset=[col]).copy()
        if len(hour) == 0:
            continue
        values = hour[col].to_numpy(dtype=float)
        w = hour["weight"].to_numpy(dtype=float)
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
