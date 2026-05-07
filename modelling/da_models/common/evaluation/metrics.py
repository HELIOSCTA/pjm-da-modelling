"""Evaluation metrics for probabilistic and point forecasts."""

from __future__ import annotations

from typing import Mapping, Sequence

import numpy as np


def _to_numpy(values: Sequence[float] | np.ndarray) -> np.ndarray:
    return np.asarray(values, dtype=float)


def crps_from_quantiles(
    y_true: Sequence[float] | np.ndarray,
    quantile_predictions: Mapping[float, Sequence[float] | np.ndarray],
) -> float:
    """Approximate CRPS from a set of quantile forecasts.

    Discretizes the integral CRPS = 2·∫₀¹ ρ_q(y, F⁻¹(q)) dq across the
    provided quantile set, where ρ_q is the asymmetric quantile loss
    max(q·e, (q-1)·e). Calibrated so a deterministic point forecast
    (degenerate distribution) recovers MAE.
    """
    if not quantile_predictions:
        raise ValueError("quantile_predictions must not be empty.")

    actual = _to_numpy(y_true)
    losses: list[float] = []
    for quantile, predictions in sorted(quantile_predictions.items()):
        q = float(quantile)
        if not 0.0 < q < 1.0:
            raise ValueError("quantile must be between 0 and 1.")
        pred = _to_numpy(predictions)
        if actual.shape != pred.shape:
            raise ValueError(
                "y_true and quantile predictions must have the same shape."
            )
        error = actual - pred
        losses.append(float(np.mean(np.maximum(q * error, (q - 1.0) * error))))
    return float(2.0 * np.mean(losses))


def coverage(
    y_true: Sequence[float] | np.ndarray,
    lower: Sequence[float] | np.ndarray,
    upper: Sequence[float] | np.ndarray,
) -> float:
    """Empirical interval coverage."""
    actual = _to_numpy(y_true)
    lo = _to_numpy(lower)
    hi = _to_numpy(upper)
    if not (actual.shape == lo.shape == hi.shape):
        raise ValueError("y_true, lower, and upper must have the same shape.")

    return float(np.mean((actual >= lo) & (actual <= hi)))


def point_errors(
    y_true: Sequence[float] | np.ndarray,
    y_pred: Sequence[float] | np.ndarray,
) -> dict[str, float]:
    """Point forecast error summary."""
    actual = _to_numpy(y_true)
    pred = _to_numpy(y_pred)
    if actual.shape != pred.shape:
        raise ValueError("y_true and y_pred must have the same shape.")

    error = actual - pred
    abs_error = np.abs(error)
    squared_error = error**2
    denom = np.where(np.abs(actual) < 1e-12, np.nan, np.abs(actual))
    ape = abs_error / denom

    return {
        "mae": float(np.nanmean(abs_error)),
        "rmse": float(np.sqrt(np.nanmean(squared_error))),
        "mape": float(np.nanmean(ape) * 100.0),
        "bias": float(np.nanmean(error)),
    }
