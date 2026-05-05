"""Single-day pjm_rto_hourly forecast (Sunny variant) — terminal output.

Mirrors the printed output of Sunny's original
``C:/OneDrive/HELIOSCTA/daily_fundies/pjm_dashboard_handoff/forecast.py``
``__main__`` block, which runs ``run_forecast()`` for tomorrow and prints
``result["output_table"]``. Adds a configuration header and the
quantile-bands table on top — both are already in the result dict, so
exposing them is free.

``run()`` returns the full result dict (``output_table``,
``quantiles_table``, ``analogs``, ``target_features_by_hour``, ...) for
programmatic / notebook callers and prints to stdout. Tunable defaults
live as module-level constants.

Usage::

    python -m da_models.like_day_model_knn_sunny.pjm_rto_hourly.pipelines.forecast_single_day
    python modelling/da_models/like_day_model_knn_sunny/pjm_rto_hourly/pipelines/forecast_single_day.py
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.like_day_model_knn_sunny import configs  # noqa: E402
from da_models.like_day_model_knn_sunny.pjm_rto_hourly import forecast  # noqa: E402


# ── Defaults (edit here instead of using CLI flags) ────────────────────
TARGET_DATE: date | None = None  # None -> tomorrow
MODEL_NAME: str = configs.PJM_RTO_HOURLY_SUNNY_SPEC.name
N_ANALOGS: int | None = None  # None -> configs.DEFAULT_N_ANALOGS
SEASON_WINDOW_DAYS: int | None = None  # None -> configs.SEASON_WINDOW_DAYS
MIN_POOL_SIZE: int | None = None  # None -> configs.MIN_POOL_SIZE
LABEL_SOURCE: str = configs.LABEL_SOURCE
RECENCY_HALF_LIFE_DAYS: float | None = None  # None -> configs.RECENCY_HALF_LIFE_DAYS

DEFAULT_QUANTILES: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _print_config(
    cfg: configs.KnnModelConfig, target_date: date, day_type: str
) -> None:
    print("=" * 80)
    print("  pjm_rto_hourly_sunny  ::  single-day forecast")
    print("=" * 80)
    print(f"  target_date             {target_date}  ({day_type})")
    print(f"  model                   {cfg.model_name}")
    print(f"  hub                     {cfg.hub}")
    print(f"  label_source            {cfg.label_source}")
    print(f"  n_analogs               {cfg.n_analogs}")
    print(f"  season_window_days      {cfg.season_window_days}")
    print(f"  min_pool_size           {cfg.min_pool_size}")
    print(f"  recency_half_life_days  {cfg.recency_half_life_days}")
    print(f"  same_dow_group          {cfg.same_dow_group}")
    print(
        f"  same_weekend_group      {cfg.same_weekend_group} "
        f"(weekends_only={cfg.same_weekend_group_for_weekends})"
    )
    print(f"  exclude_holidays        {cfg.exclude_holidays}")
    print()


def _print_forecast(output_table: pd.DataFrame) -> None:
    print("-" * 80)
    print("  DA LMP LIKE-DAY FORECAST  ($/MWh)")
    print("-" * 80)
    print(output_table.to_string(index=False, float_format=lambda v: f"{v:>8.2f}"))
    print()


def _print_quantiles(quantiles_table: pd.DataFrame) -> None:
    print("-" * 80)
    print("  QUANTILE BANDS  (per-hour weighted; OnPeak/OffPeak/Flat from MC joint)")
    print("-" * 80)
    print(quantiles_table.to_string(index=False, float_format=lambda v: f"{v:>8.2f}"))
    print()


def run(
    target_date: date | None = TARGET_DATE,
    model_name: str = MODEL_NAME,
    n_analogs: int | None = N_ANALOGS,
    season_window_days: int | None = SEASON_WINDOW_DAYS,
    min_pool_size: int | None = MIN_POOL_SIZE,
    label_source: str = LABEL_SOURCE,
    recency_half_life_days: float | None = RECENCY_HALF_LIFE_DAYS,
    quantiles: tuple[float, ...] | list[float] | None = None,
    pool: pd.DataFrame | None = None,
    quiet: bool = False,
) -> dict:
    """Run the Sunny single-day forecast and print the report.

    Returns the same dict as ``forecast.run_forecast``: ``output_table``,
    ``quantiles_table``, ``analogs``, ``target_features_by_hour``,
    ``forecast_date``, ``reference_date``, ``has_actuals``,
    ``n_analogs_used``, ``scenario``, ``feature_weights``, ``day_type``.

    ``pool`` — pre-built pool to skip the ~5-10s build (notebook reuse).
    ``quiet`` — suppress prints, still return the dict.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if model_name not in configs.MODEL_REGISTRY:
        raise ValueError(
            f"model_name='{model_name}' not in MODEL_REGISTRY "
            f"{tuple(configs.MODEL_REGISTRY.keys())}"
        )

    resolved_date = _resolve_target_date(target_date)
    quantiles_list = list(quantiles if quantiles is not None else DEFAULT_QUANTILES)

    cfg = configs.KnnModelConfig(
        forecast_date=str(resolved_date),
        model_name=model_name,
        n_analogs=configs.DEFAULT_N_ANALOGS if n_analogs is None else int(n_analogs),
        season_window_days=(
            configs.SEASON_WINDOW_DAYS
            if season_window_days is None
            else int(season_window_days)
        ),
        min_pool_size=(
            configs.MIN_POOL_SIZE if min_pool_size is None else int(min_pool_size)
        ),
        recency_half_life_days=(
            configs.RECENCY_HALF_LIFE_DAYS
            if recency_half_life_days is None
            else float(recency_half_life_days)
        ),
        label_source=label_source,
        quantiles=quantiles_list,
    )

    result = forecast.run_forecast(
        target_date=resolved_date,
        config=cfg,
        cache_dir=configs.CACHE_DIR,
        pool=pool,
    )

    if not quiet:
        resolved_cfg, day_type = cfg.with_day_type_overrides(resolved_date)
        _print_config(resolved_cfg, resolved_date, day_type)
        _print_forecast(result["output_table"])
        _print_quantiles(result["quantiles_table"])

    return result


if __name__ == "__main__":
    run()
