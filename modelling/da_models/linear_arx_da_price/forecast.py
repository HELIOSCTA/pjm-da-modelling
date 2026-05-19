"""Turn trained per-hour models + the target-date feature row into a forecast.

For each hour-ending the point forecast is ``sinh`` of the ridge
prediction in asinh space, nudged for the asinh skew (the EV row sits
above P50 on right-skewed peak hours -- see mean_vs_median.md). Quantile
bands add an empirical residual quantile (taken over the recent in-sample
residuals, in asinh space) before inverting the transform:
``band_q = sinh(pred_asinh + Q_q(resid))``.

``forecast_target_date`` returns a per-HE frame with ``point_forecast``,
``p50``, and ``q_0.10`` / ``q_0.25`` / ... columns (the ``q_`` naming
matches the like-day ``pjm_rto_hourly`` frame so the shared display
helpers consume it unchanged). ``build_quantiles_table`` pivots that into
the ``Date | Type | HE1..HE24 | OnPeak | OffPeak | Flat`` band table used
by the terminal report.
"""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from da_models.common.forecast.output import add_summary_cols
from da_models.linear_arx_da_price import configs as C
from da_models.linear_arx_da_price.trainer import TrainedModels, sinh

logger = logging.getLogger(__name__)

_FINE_GRID = np.round(np.arange(0.05, 1.0, 0.05), 2)
_HE_COLS = [f"HE{h}" for h in range(1, 25)]
_SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]


def _q_col(q: float) -> str:
    return f"q_{q:.2f}"


def quantile_label(q: float) -> str:
    """P25, P37.5, P90, ... -- matches the like-day printer convention."""
    q_pct = q * 100
    if float(q_pct).is_integer():
        return f"P{int(q_pct):02d}"
    return f"P{q_pct:.1f}".rstrip("0").rstrip(".")


def _empty_record(h: int) -> dict:
    rec: dict[str, object] = {"hour_ending": h, "point_forecast": np.nan, "p50": np.nan}
    for q in C.QUANTILES:
        rec[_q_col(q)] = np.nan
    return rec


def forecast_target_date(
    models: TrainedModels,
    panel: pd.DataFrame,
    target_date: date,
) -> pd.DataFrame:
    target_rows = panel[panel["date"] == target_date].set_index("hour_ending")
    records: list[dict] = []
    skipped: dict[int, list[str]] = {}  # hour -> missing feature names

    for h in C.HOURS:
        model = models.by_hour.get(h)
        if model is None or h not in target_rows.index:
            records.append(_empty_record(h))
            continue
        x_row = target_rows.loc[[h]]
        missing = [c for c in model.feature_cols if pd.isna(x_row.iloc[0].get(c))]
        if missing:
            skipped[h] = missing
            records.append(_empty_record(h))
            continue

        pred_asinh = model.predict_asinh(x_row)
        resid = model.residuals_asinh
        rec: dict[str, object] = {"hour_ending": h}
        for q in C.QUANTILES:
            rec[_q_col(q)] = float(sinh(pred_asinh + np.quantile(resid, q)))
        rec["p50"] = float(sinh(pred_asinh + np.quantile(resid, 0.5)))
        rec["point_forecast"] = float(
            np.mean(sinh(pred_asinh + np.quantile(resid, _FINE_GRID)))
        )
        records.append(rec)

    if skipped:
        all_missing = sorted({c for cols in skipped.values() for c in cols})
        if len(skipped) == len(C.HOURS):
            logger.warning(
                "%s: all 24 HEs skipped -- target features unavailable (%s ...)",
                target_date,
                ", ".join(all_missing[:4]),
            )
        else:
            logger.warning(
                "%s: %d HEs skipped (%s) -- missing target features (%s ...)",
                target_date,
                len(skipped),
                sorted(skipped),
                ", ".join(all_missing[:4]),
            )

    cols = ["hour_ending", "point_forecast", "p50"] + [_q_col(q) for q in C.QUANTILES]
    return pd.DataFrame(records)[cols]


def build_quantiles_table(
    target_date: date,
    df_forecast: pd.DataFrame,
    display_quantiles: tuple[float, ...] | list[float],
) -> pd.DataFrame:
    """One row per displayed quantile (``P10`` ... ``P90``), HE1..HE24 +
    OnPeak / OffPeak / Flat means."""
    by_he = {int(r["hour_ending"]): r for _, r in df_forecast.iterrows()}
    rows: list[dict] = []
    for q in display_quantiles:
        col = _q_col(q)
        if col not in df_forecast.columns:
            continue
        d: dict[str, object] = {"Date": target_date, "Type": quantile_label(q)}
        for h in range(1, 25):
            v = by_he.get(h, {})
            d[f"HE{h}"] = (
                float(v[col]) if (h in by_he and pd.notna(v.get(col))) else np.nan
            )
        rows.append(add_summary_cols(d))
    cols = ["Date", "Type"] + _HE_COLS + _SUMMARY_COLS
    return pd.DataFrame(rows, columns=cols)
