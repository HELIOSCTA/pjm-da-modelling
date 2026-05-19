"""Quantile-band scoring across the tall-schema replay frame.

For each ``(model_name, group)``: coverage of the P10-P90 interval
(target: 80%), average band width (sharpness), and the pinball-loss
average across the available quantile levels. Models that don't expose
all five quantile knots (e.g. ``baseline_meteo`` only has Bottom / Avg /
Top -> 0.10 / 0.50 / 0.90, with 0.25 / 0.75 NaN) score on whatever knots
they do expose; the comparison is honest-by-subset rather than
penalising NaN.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

_DEFAULT_QUANTILES: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)


def _pinball_loss(actual: np.ndarray, q_pred: np.ndarray, q: float) -> float:
    """Mean asymmetric quantile loss ``max(q * e, (q-1) * e)`` where
    ``e = actual - q_pred``. NaN rows skipped."""
    mask = ~(np.isnan(actual) | np.isnan(q_pred))
    if mask.sum() == 0:
        return float("nan")
    e = actual[mask] - q_pred[mask]
    return float(np.mean(np.maximum(q * e, (q - 1.0) * e)))


def _coverage(actual: np.ndarray, lo: np.ndarray, hi: np.ndarray) -> float:
    mask = ~(np.isnan(actual) | np.isnan(lo) | np.isnan(hi))
    if mask.sum() == 0:
        return float("nan")
    a, lo_m, hi_m = actual[mask], lo[mask], hi[mask]
    return float(np.mean((a >= lo_m) & (a <= hi_m)))


def _band_metrics_array(
    actual: np.ndarray,
    q_arrays: dict[float, np.ndarray],
) -> dict[str, float]:
    out: dict[str, float] = {}
    # 80% PI coverage + sharpness (P10..P90).
    lo = q_arrays.get(0.10)
    hi = q_arrays.get(0.90)
    if lo is not None and hi is not None:
        out["coverage_p10_p90"] = _coverage(actual, lo, hi)
        mask = ~(np.isnan(lo) | np.isnan(hi))
        out["sharpness_p10_p90_mw"] = (
            float(np.mean(hi[mask] - lo[mask])) if mask.any() else float("nan")
        )
    else:
        out["coverage_p10_p90"] = float("nan")
        out["sharpness_p10_p90_mw"] = float("nan")
    # Pinball loss averaged across the levels the model exposes.
    per_q: dict[str, float] = {}
    for q, arr in q_arrays.items():
        per_q[f"pinball_q_{q:.2f}"] = _pinball_loss(actual, arr, q)
    finite = [v for v in per_q.values() if np.isfinite(v)]
    out["pinball_mean"] = float(np.mean(finite)) if finite else float("nan")
    out.update(per_q)
    return out


def quantile_metrics_by_model(
    df: pd.DataFrame,
    *,
    group_by: list[str] | None = None,
    quantiles: tuple[float, ...] = _DEFAULT_QUANTILES,
) -> pd.DataFrame:
    """One row per ``(model_name, *group_by)`` with coverage, sharpness,
    and per-quantile pinball columns."""
    if df.empty:
        return pd.DataFrame()
    group_cols = ["model_name"] + list(group_by or [])
    rows: list[dict] = []
    for key, grp in df.groupby(group_cols, dropna=False, sort=False):
        if not isinstance(key, tuple):
            key = (key,)
        row: dict = dict(zip(group_cols, key))
        actual = grp["actual_lmp"].to_numpy(dtype=float)
        q_arrays = {
            q: grp[f"q_{q:.2f}"].to_numpy(dtype=float)
            for q in quantiles
            if f"q_{q:.2f}" in grp.columns
        }
        row.update(_band_metrics_array(actual, q_arrays))
        row["n"] = int((~np.isnan(actual)).sum())
        rows.append(row)
    return pd.DataFrame(rows).reset_index(drop=True)
