"""Per-hour ridge fit in asinh space, with recency weighting.

24 independent models (univariate framework, per Ziel & Weron 2018). For
each hour-ending: standardize features, fit ``Ridge`` with alpha selected
by expanding-window time-series CV, on ``arcsinh(lmp)`` targets weighted
by ``gamma ** age_in_days``. Holds onto the recent in-sample residuals
(in asinh space) for the band construction in ``forecast.py``.

Asinh is the standard variance-stabilizing transform for EPF targets
(Uniejewski/Weron/Ziel 2018) — it compresses heavy price tails so the
ridge fit and the residual quantiles are not dominated by the bulk of
"normal" hours, and it handles negative LMPs without special-casing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from da_models.linear_arx_da_price import configs as C

logger = logging.getLogger(__name__)


def asinh(x: np.ndarray | pd.Series) -> np.ndarray:
    return np.arcsinh(np.asarray(x, dtype=float))


def sinh(x: np.ndarray) -> np.ndarray:
    return np.sinh(np.asarray(x, dtype=float))


@dataclass
class HourModel:
    hour_ending: int
    pipeline: Pipeline
    feature_cols: list[str]
    alpha: float
    n_train: int
    residuals_asinh: np.ndarray  # recent in-sample residuals (asinh space)
    coef: np.ndarray = field(repr=False)

    def predict_asinh(self, x_row: pd.DataFrame) -> float:
        return float(self.pipeline.predict(x_row[self.feature_cols].to_numpy())[0])


@dataclass
class TrainedModels:
    by_hour: dict[int, HourModel]
    feature_cols: list[str]
    skipped_hours: list[int]
    # Backward-vs-forward coefficient mass diagnostic, averaged over hours.
    backward_coef_share: float


def _recency_weights(train_dates: pd.Series, target_date: date) -> np.ndarray:
    age_days = np.array([(target_date - d).days for d in train_dates], dtype=float)
    age_days = np.clip(age_days, 0.0, None)
    return C.RECENCY_GAMMA**age_days


def _select_alpha(x: np.ndarray, y: np.ndarray, w: np.ndarray) -> float:
    n = len(y)
    n_splits = min(C.CV_SPLITS, max(2, n // 60))
    if n < 60 or n_splits < 2:
        return float(np.median(C.RIDGE_ALPHAS))
    tscv = TimeSeriesSplit(n_splits=n_splits)
    best_alpha, best_score = C.RIDGE_ALPHAS[0], np.inf
    for alpha in C.RIDGE_ALPHAS:
        fold_losses: list[float] = []
        for tr_idx, va_idx in tscv.split(x):
            pipe = Pipeline(
                [("scaler", StandardScaler()), ("ridge", Ridge(alpha=alpha))]
            )
            pipe.fit(x[tr_idx], y[tr_idx], ridge__sample_weight=w[tr_idx])
            pred = pipe.predict(x[va_idx])
            fold_losses.append(float(np.mean(np.abs(pred - y[va_idx]))))
        score = float(np.mean(fold_losses))
        if score < best_score:
            best_alpha, best_score = alpha, score
    return best_alpha


def _backward_share(coef: np.ndarray, feature_cols: list[str]) -> float:
    abs_coef = np.abs(coef)
    total = abs_coef.sum()
    if total <= 0:
        return float("nan")
    bwd_mask = np.array([c.startswith("bwd_lmp_") for c in feature_cols])
    return float(abs_coef[bwd_mask].sum() / total)


def train(
    panel: pd.DataFrame, feature_cols: list[str], target_date: date
) -> TrainedModels:
    by_hour: dict[int, HourModel] = {}
    skipped: list[int] = []
    bwd_shares: list[float] = []

    for h in C.HOURS:
        rows = panel[
            (panel["hour_ending"] == h)
            & (panel["date"] < target_date)
            & panel["lmp"].notna()
        ].copy()
        rows = rows.dropna(subset=feature_cols)
        if len(rows) < C.MIN_TRAIN_ROWS_PER_HOUR:
            logger.warning(
                "HE%d: only %d usable training rows (<%d) -- skipping hour",
                h,
                len(rows),
                C.MIN_TRAIN_ROWS_PER_HOUR,
            )
            skipped.append(h)
            continue
        rows = rows.sort_values("date")
        x = rows[feature_cols].to_numpy(dtype=float)
        y = asinh(rows["lmp"].to_numpy())
        w = _recency_weights(rows["date"], target_date)

        alpha = _select_alpha(x, y, w)
        pipe = Pipeline([("scaler", StandardScaler()), ("ridge", Ridge(alpha=alpha))])
        pipe.fit(x, y, ridge__sample_weight=w)

        resid = y - pipe.predict(x)
        # One training row per date at this hour, so trailing-N rows ~= trailing-N days.
        window = max(min(len(resid), C.RESIDUAL_HOLDOUT_DAYS), 30)
        recent = resid[-min(len(resid), window) :]

        coef = pipe.named_steps["ridge"].coef_.astype(float)
        by_hour[h] = HourModel(
            hour_ending=h,
            pipeline=pipe,
            feature_cols=feature_cols,
            alpha=alpha,
            n_train=len(rows),
            residuals_asinh=recent,
            coef=coef,
        )
        bwd_shares.append(_backward_share(coef, feature_cols))

    backward_coef_share = float(np.nanmean(bwd_shares)) if bwd_shares else float("nan")
    return TrainedModels(
        by_hour=by_hour,
        feature_cols=feature_cols,
        skipped_hours=skipped,
        backward_coef_share=backward_coef_share,
    )
