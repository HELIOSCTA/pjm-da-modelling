"""Point-forecast scoring across the tall-schema replay frame.

For each ``(model_name, group)``: MAE / RMSE / bias / MAPE on the
``(actual_lmp, point)`` pairs, dropping rows where either side is NaN
(unforecasted hours, unsettled dates). Plus rMAE vs the configured
baseline model -- ``rMAE = MAE_model / MAE_baseline``, lower is better;
< 1 means the model beats the baseline on average over the slice.

``group_by`` lets the caller slice by HE / DOW / day-type / scarcity
flag / season / load tier -- whatever columns they've attached to the
frame before calling. Returns one row per (model_name, group_key, ...).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from da_models.backtest import configs as C


def _safe_mape(actual: np.ndarray, forecast: np.ndarray) -> float:
    with np.errstate(divide="ignore", invalid="ignore"):
        ape = np.abs(forecast - actual) / np.where(
            np.abs(actual) < 1e-9, np.nan, np.abs(actual)
        )
    if not np.isfinite(ape).any():
        return float("nan")
    return float(np.nanmean(ape) * 100.0)


def _point_metrics_array(actual: np.ndarray, forecast: np.ndarray) -> dict[str, float]:
    mask = ~(np.isnan(actual) | np.isnan(forecast))
    if mask.sum() == 0:
        return {
            "n": 0,
            "mae": float("nan"),
            "rmse": float("nan"),
            "bias": float("nan"),
            "mape_pct": float("nan"),
        }
    a = actual[mask]
    f = forecast[mask]
    err = f - a
    return {
        "n": int(mask.sum()),
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(np.sqrt(np.mean(err**2))),
        "bias": float(np.mean(err)),
        "mape_pct": _safe_mape(a, f),
    }


def point_metrics_by_model(
    df: pd.DataFrame,
    *,
    group_by: list[str] | None = None,
    baseline_model: str = C.BASELINE_MODEL_NAME,
) -> pd.DataFrame:
    """Aggregate point metrics. ``df`` is the canonical tall schema; the
    output frame has one row per ``(model_name, *group_by)`` with columns
    ``n, mae, rmse, bias, mape_pct, rmae`` where ``rmae`` is the model's
    MAE divided by the baseline model's MAE on the same slice."""
    if df.empty:
        return pd.DataFrame()
    group_cols = ["model_name"] + list(group_by or [])
    rows: list[dict] = []
    for key, grp in df.groupby(group_cols, dropna=False, sort=False):
        if not isinstance(key, tuple):
            key = (key,)
        row: dict = dict(zip(group_cols, key))
        row.update(
            _point_metrics_array(
                grp["actual_lmp"].to_numpy(dtype=float),
                grp["point"].to_numpy(dtype=float),
            )
        )
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    # Attach rMAE vs baseline_model on each slice.
    if baseline_model in set(out["model_name"]):
        slice_cols = [c for c in group_cols if c != "model_name"]
        if slice_cols:
            base = (
                out[out["model_name"] == baseline_model]
                .set_index(slice_cols)["mae"]
                .rename("mae_baseline")
            )
            out = out.merge(base, left_on=slice_cols, right_index=True, how="left")
        else:
            base_mae = float(
                out.loc[out["model_name"] == baseline_model, "mae"].iloc[0]
            )
            out["mae_baseline"] = base_mae
        out["rmae"] = out["mae"] / out["mae_baseline"]
        out.loc[~np.isfinite(out["rmae"]), "rmae"] = float("nan")
        out = out.drop(columns=["mae_baseline"])
    else:
        out["rmae"] = float("nan")
    return out.reset_index(drop=True)
