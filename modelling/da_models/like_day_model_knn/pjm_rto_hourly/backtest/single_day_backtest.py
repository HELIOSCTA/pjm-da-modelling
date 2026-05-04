"""Single-day backtest for pjm_rto_hourly.

A backtest runs the forecast pipeline for a date with KNOWN actuals and
surfaces the metrics. Differs from
``pipelines/forecast_single_day.py`` in two ways:

  - default target is YESTERDAY (``date.today() - 1d``), not tomorrow.
  - hard-fails if the target date has no actuals in the pool — a
    backtest without actuals is meaningless, and silent metric-skipping
    masks the cause.

Use this when you want a single-date evaluation. Use
``backtest/param_sweep.py`` when you want to compare scenarios across a
window of dates.

Tunable defaults at the top — edit and re-run, no CLI flags. The
optional ``FEATURE_GROUP_WEIGHTS_OVERRIDE`` constant lets you ask
"how would yesterday have looked under different weights?" without
touching ``domains.py``.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.backtest.single_day_backtest
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/backtest/single_day_backtest.py
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from da_models.like_day_model_knn import configs  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as forecast_run,
)


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = None              # None -> yesterday (date.today() - 1d)
MODEL_NAME: str = configs.PJM_RTO_HOURLY_SPEC.name
FLT_RADIUS: int = configs.PJM_RTO_HOURLY_SPEC.flt_radius
N_ANALOGS: int | None = None                 # None -> configs.DEFAULT_N_ANALOGS
SEASON_WINDOW_DAYS: int | None = None        # None -> configs.SEASON_WINDOW_DAYS
MIN_POOL_SIZE: int | None = None             # None -> configs.MIN_POOL_SIZE

# Optional: override the spec's feature-group weights for this backtest only.
# Useful for "if I'd used these weights yesterday, how would I have done?".
# None -> use spec defaults. See param_sweep.py for valid keys + examples.
FEATURE_GROUP_WEIGHTS_OVERRIDE: dict[str, float] | None = None


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() - timedelta(days=1)


def run(
    target_date: date | None = TARGET_DATE,
    model_name: str = MODEL_NAME,
    flt_radius: int = FLT_RADIUS,
    n_analogs: int | None = N_ANALOGS,
    season_window_days: int | None = SEASON_WINDOW_DAYS,
    min_pool_size: int | None = MIN_POOL_SIZE,
    feature_group_weights_override: dict[str, float] | None = FEATURE_GROUP_WEIGHTS_OVERRIDE,
) -> dict:
    """Run the forecast for a past date and surface the metrics.

    Returns the same dict as ``forecast_single_day.run()``. Raises
    ``ValueError`` if the target date is in the future, or
    ``RuntimeError`` if no actuals are available in the pool.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    resolved_date = _resolve_target_date(target_date)
    if resolved_date >= date.today() + timedelta(days=1):
        raise ValueError(
            f"Backtest needs a past or current date with actuals; got {resolved_date}. "
            f"Use the live forecast entry point instead: "
            f"python -m da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day"
        )

    result = forecast_run(
        target_date=resolved_date,
        model_name=model_name,
        flt_radius=flt_radius,
        n_analogs=n_analogs,
        season_window_days=season_window_days,
        min_pool_size=min_pool_size,
        feature_group_weights_override=feature_group_weights_override,
        write_analog_store=False,        # backtests don't pollute the live run history
        quiet=False,                     # we want the full four-section report
    )

    if not result.get("has_actuals"):
        raise RuntimeError(
            f"No actuals in the pool for target_date={resolved_date}. "
            "The DA LMP cache may not yet cover this date — try an earlier "
            "target_date, or refresh the pjm_lmps_hourly parquet."
        )

    return result


if __name__ == "__main__":
    run()
