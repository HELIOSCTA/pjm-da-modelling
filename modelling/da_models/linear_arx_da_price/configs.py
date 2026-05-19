"""Shared configuration for the linear ARX DA-price family.

Estimator / calibration-window / band constants common to every variant
(``pjm_hourly``, ``meteo_hourly``, ...). Feature-source-specific knobs
(which feeds, which regions, the backward-LMP toggle, the model name)
live in each variant's ``config.py``. Defaults follow the design memo
``modelling/@TODO/pjm-research-for-modelling/linear_regression_model.md``.
"""

from __future__ import annotations

import math

MODEL_FAMILY: str = "linear_arx_da_price"

# ── Target ─────────────────────────────────────────────────────────────────
HUB: str = "WESTERN HUB"
HOURS: tuple[int, ...] = tuple(range(1, 25))
# 80% PI (P10/P90) + IQR (P25/P75) + median -- mirrors the like-day
# pjm_rto_hourly band set so the two terminal reports line up.
QUANTILES: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)
DISPLAY_QUANTILES: tuple[float, ...] = QUANTILES

# ── Calibration window / weighting ─────────────────────────────────────────
# Expanding window: keep at most this many trailing days of training rows.
TRAIN_WINDOW_DAYS: int = 728
MIN_TRAIN_ROWS_PER_HOUR: int = 120
# Exponential recency weighting on training rows (per the EPF literature --
# Marcjasz/Serafin/Weron, lasso_model.md Tier-1 item 2). gamma ** age_in_days.
RECENCY_HALFLIFE_DAYS: int = 231
RECENCY_GAMMA: float = math.exp(-math.log(2.0) / RECENCY_HALFLIFE_DAYS)  # ~0.997

# ── Estimator ──────────────────────────────────────────────────────────────
# Ridge alpha grid, selected per hour by expanding-window time-series CV.
# Capped at 30: when the grid included 100/300 the CV pinned midday hours to
# the max ("prefer even more shrinkage"), which minimizes average MAE but
# leaves the model nearly flat on extreme-input days (e.g. a 60-GW-outage,
# 117-GW-load heat event). Trading a little ordinary-day MAE for tail
# responsiveness -- see the regularization discussion in lasso_model.md.
RIDGE_ALPHAS: tuple[float, ...] = (0.01, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0)
CV_SPLITS: int = 4

# ── Bands ──────────────────────────────────────────────────────────────────
# Residual quantiles taken (in asinh space) over the most recent N training
# rows per hour. v1 simplification -- in-sample residuals; Tier-2 swaps in
# genuine quantile regression / conformalized quantile regression.
RESIDUAL_HOLDOUT_DAYS: int = 90

# ── Scarcity / convexity features ──────────────────────────────────────────
# Net-load hinge knots (MW): each adds a feature max(net_load - knot, 0) that
# is exactly 0 on ordinary days and grows only in the extreme region, so the
# fit can put a steep price slope on scarcity days without distorting the
# normal-day fit (piecewise-linear approximation of the convex supply stack).
NET_LOAD_HINGE_KNOTS_MW: tuple[int, ...] = (95_000, 110_000)

# ── Vintage ────────────────────────────────────────────────────────────────
# Lead-days vintage for all DA-cutoff forecast feeds.
LEAD_DAYS: int = 1
