"""Probabilistic forecast evaluation metrics.

Ported from helioscta-pjm-da/backend/src/like_day_forecast/evaluation/metrics.py
to keep the terminal report numbers comparable. GEFCom2014 conventions.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def rmae(y_true: np.ndarray, y_pred: np.ndarray, y_naive: np.ndarray) -> float:
    """Relative MAE: MAE(model) / MAE(naive). <1 means model beats naive."""
    mae_model = float(np.mean(np.abs(y_true - y_pred)))
    mae_naive = float(np.mean(np.abs(y_true - y_naive)))
    if mae_naive == 0:
        return float("inf")
    return mae_model / mae_naive


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y_true - y_pred)))


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    mask = y_true != 0
    if not np.any(mask):
        return float("nan")
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)


def coverage(y_true: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> float:
    """% of actuals within [lower, upper]."""
    return float(np.mean((y_true >= lower) & (y_true <= upper)))


def sharpness(lower: np.ndarray, upper: np.ndarray) -> float:
    """Average prediction interval width."""
    return float(np.mean(upper - lower))


def crps(y_true: np.ndarray, y_pred_df: pd.DataFrame, quantiles: list[float]) -> float:
    """CRPS approximated via trapezoidal integration of the asymmetric
    quantile loss ρ_q(y, p) = max(q·(y-p), (q-1)·(y-p))."""
    losses: list[tuple[float, float]] = []
    for q in quantiles:
        col = f"q_{q:.2f}"
        if col in y_pred_df.columns:
            delta = y_true - y_pred_df[col].to_numpy(dtype=float)
            ql = float(np.mean(np.maximum(q * delta, (q - 1) * delta)))
            losses.append((q, ql))
    if len(losses) < 2:
        return float("nan")
    losses.sort(key=lambda x: x[0])
    qs = [item[0] for item in losses]
    pls = [item[1] for item in losses]
    return float(np.trapz(pls, qs))


def evaluate_forecast(
    y_true: np.ndarray,
    y_pred_df: pd.DataFrame,
    quantiles: list[float],
    y_naive: np.ndarray | None = None,
) -> dict:
    """Compute all metrics for a quantile forecast.

    ``y_pred_df`` must have a ``point_forecast`` column (or ``q_0.50``)
    and ``q_{q:.2f}`` columns for each quantile in ``quantiles``.
    """
    results: dict[str, float] = {}

    if "point_forecast" in y_pred_df.columns:
        y_point = y_pred_df["point_forecast"].to_numpy(dtype=float)
    elif "q_0.50" in y_pred_df.columns:
        y_point = y_pred_df["q_0.50"].to_numpy(dtype=float)
    else:
        y_point = None

    if y_point is not None:
        results["mae"] = mae(y_true, y_point)
        results["rmse"] = rmse(y_true, y_point)
        results["mape"] = mape(y_true, y_point)
        if y_naive is not None:
            results["rmae"] = rmae(y_true, y_point, y_naive)

    results["crps"] = crps(y_true, y_pred_df, quantiles)

    for name, q_lo, q_hi in (
        ("80pct", 0.10, 0.90),
        ("90pct", 0.05, 0.95),
        ("98pct", 0.01, 0.99),
    ):
        col_lo = f"q_{q_lo:.2f}"
        col_hi = f"q_{q_hi:.2f}"
        if col_lo in y_pred_df.columns and col_hi in y_pred_df.columns:
            results[f"coverage_{name}"] = coverage(
                y_true,
                y_pred_df[col_lo].to_numpy(dtype=float),
                y_pred_df[col_hi].to_numpy(dtype=float),
            )
            results[f"sharpness_{name}"] = sharpness(
                y_pred_df[col_lo].to_numpy(dtype=float),
                y_pred_df[col_hi].to_numpy(dtype=float),
            )

    return results
