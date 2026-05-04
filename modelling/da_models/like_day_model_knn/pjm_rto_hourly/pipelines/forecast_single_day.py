"""Single-day pjm_rto_hourly forecast — terminal output.

Mirrors helioscta-pjm-da/backend/src/like_day_forecast/pipelines/forecast.py
in print layout (FORECAST CONFIGURATION block, LIKE-DAY ANALOG DAYS table,
DA LMP LIKE-DAY FORECAST table with metrics, Quantile Bands table).

``run()`` returns a dict (``output_table``, ``quantiles_table``, ``analogs``,
``metrics``, ...) for programmatic / notebook callers and prints the four
sections to stdout. The optional parquet explainability store is the only
on-disk artefact.

Tunable defaults live in module-level constants at the top of this file —
edit them directly or pass overrides to ``run(...)`` from a REPL.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/pipelines/forecast_single_day.py
"""
from __future__ import annotations

import sys
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.like_day_model_knn import _shared, configs  # noqa: E402
from da_models.like_day_model_knn.analog_store import (  # noqa: E402
    DEFAULT_STORE_DIR,
    write_analog_explainability,
)
from da_models.like_day_model_knn.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool, build_query_row,
)
from da_models.like_day_model_knn.pjm_rto_hourly.engine import find_twins  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.forecast import (  # noqa: E402
    actuals_from_pool,
    build_output_table,
    build_quantiles_table,
    hourly_forecast_from_hour_analogs,
)
from da_models.like_day_model_knn.pjm_rto_hourly.metrics import evaluate_forecast  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.printers import (  # noqa: E402
    print_analogs, print_config, print_forecast, print_quantiles,
)


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = None                            # None -> tomorrow (date.today() + 1d)
MODEL_NAME: str = configs.PJM_RTO_HOURLY_SPEC.name
FLT_RADIUS: int = configs.PJM_RTO_HOURLY_SPEC.flt_radius
N_ANALOGS: int | None = None                               # None -> configs.DEFAULT_N_ANALOGS
SEASON_WINDOW_DAYS: int | None = None                      # None -> configs.SEASON_WINDOW_DAYS
MIN_POOL_SIZE: int | None = None                           # None -> configs.MIN_POOL_SIZE
WRITE_ANALOG_STORE: bool = True
ANALOG_STORE_DIR: Path | None = None                       # None -> DEFAULT_STORE_DIR

# Quantiles needed by the printed bands table (P25..P75 inner) PLUS the
# wider levels (P10/P90, P05/P95, P01/P99) that evaluate_forecast uses
# for 80/90/98% prediction-interval coverage.
DEFAULT_QUANTILES: tuple[float, ...] = (
    0.01, 0.05, 0.10,
    0.25, 0.375, 0.50, 0.625, 0.75,
    0.90, 0.95, 0.99,
)
DISPLAY_QUANTILES: tuple[float, ...] = DEFAULT_QUANTILES


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _naive_last_week(pool: pd.DataFrame, target_date: date) -> np.ndarray | None:
    """Naive baseline: same-day-last-week DA LMP profile (24 hours)."""
    actuals = actuals_from_pool(pool, target_date - timedelta(days=7))
    if actuals is None:
        return None
    return np.array([actuals[h] for h in configs.HOURS], dtype=float)


def run(
    target_date: date | None = TARGET_DATE,
    model_name: str = MODEL_NAME,
    flt_radius: int = FLT_RADIUS,
    n_analogs: int | None = N_ANALOGS,
    season_window_days: int | None = SEASON_WINDOW_DAYS,
    min_pool_size: int | None = MIN_POOL_SIZE,
    write_analog_store: bool = WRITE_ANALOG_STORE,
    analog_store_dir: Path | None = ANALOG_STORE_DIR,
    quantiles: tuple[float, ...] | list[float] | None = None,
    display_quantiles: tuple[float, ...] | list[float] | None = None,
    pool: pd.DataFrame | None = None,
    query: pd.Series | None = None,
    dates_meta: pd.DataFrame | None = None,
    feature_group_weights_override: dict[str, float] | None = None,
    quiet: bool = False,
) -> dict:
    """Run the forecast and print the four-section terminal report.

    Returns a dict with: ``output_table``, ``quantiles_table``, ``analogs``,
    ``metrics``, ``forecast_date``, ``day_type``, ``has_actuals``, ``n_pool``,
    ``n_analogs_used``, ``scenario``, ``df_forecast``.

    Reusable artefacts (``pool``, ``query``, ``dates_meta``) — when
    provided, skip the corresponding build step. Used by the param_sweep
    backtest harness to amortize the ~5-10s pool build across many
    scenarios. ``query`` is target-date-specific so callers reusing pool
    across multiple target dates must rebuild it per date.

    ``feature_group_weights_override`` — passed through to ``find_twins``
    to override the spec's default group weights for this run only.
    Validated and renormalized inside the engine.

    ``quiet`` — suppresses the four ``print_*`` calls (used by the sweep
    harness which prints its own cross-scenario summary).
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if model_name not in configs.MODEL_REGISTRY:
        raise ValueError(
            f"model_name='{model_name}' not in MODEL_REGISTRY {tuple(configs.MODEL_REGISTRY.keys())}"
        )

    resolved_date = _resolve_target_date(target_date)
    quantiles = list(quantiles if quantiles is not None else DEFAULT_QUANTILES)
    display_quantiles = list(
        display_quantiles if display_quantiles is not None else DISPLAY_QUANTILES
    )

    base_config = configs.KnnModelConfig(
        forecast_date=str(resolved_date),
        model_name=model_name,
        n_analogs=configs.DEFAULT_N_ANALOGS if n_analogs is None else int(n_analogs),
        season_window_days=(
            configs.SEASON_WINDOW_DAYS if season_window_days is None
            else int(season_window_days)
        ),
        min_pool_size=(
            configs.MIN_POOL_SIZE if min_pool_size is None else int(min_pool_size)
        ),
        quantiles=quantiles,
    )
    config, day_type = base_config.with_day_type_overrides(resolved_date)
    base_spec = config.resolved_spec()
    spec = replace(base_spec, flt_radius=int(flt_radius))

    if pool is None:
        pool = build_pool(
            schema=config.schema, hub=config.hub, cache_dir=configs.CACHE_DIR, spec=spec,
        )
    if query is None:
        query = build_query_row(
            target_date=resolved_date, schema=config.schema,
            cache_dir=configs.CACHE_DIR, spec=spec,
        )
    if dates_meta is None:
        dates_meta = _shared.load_dates_daily(configs.CACHE_DIR)

    analogs = find_twins(
        query=query, pool=pool, target_date=resolved_date, spec=spec,
        n_analogs=config.n_analogs,
        season_window_days=config.season_window_days,
        min_pool_size=config.min_pool_size,
        dates_meta=dates_meta,
        same_dow_group=config.same_dow_group,
        exclude_holidays=config.exclude_holidays,
        exclude_dates=config.exclude_dates,
        max_age_years=config.max_age_years,
        recency_half_life_years=config.recency_half_life_years,
        feature_group_weights_override=feature_group_weights_override,
    )

    if write_analog_store:
        store_dir = analog_store_dir or DEFAULT_STORE_DIR
        write_analog_explainability(
            target_date=resolved_date,
            config=config,
            spec=spec,
            pool=pool,
            query=query,
            analogs=analogs,
            output_dir=store_dir,
        )

    df_forecast = hourly_forecast_from_hour_analogs(analogs, quantiles)

    actuals = actuals_from_pool(pool, resolved_date)
    has_actuals = actuals is not None
    output_table = build_output_table(resolved_date, df_forecast, actuals)
    quantiles_table = build_quantiles_table(resolved_date, df_forecast, display_quantiles)

    metrics: dict = {}
    if has_actuals and len(df_forecast) > 0:
        merged = df_forecast.copy()
        merged["actual_lmp"] = merged["hour_ending"].map(actuals)
        merged = merged.dropna(subset=["actual_lmp"])
        if len(merged) > 0:
            y_true = merged["actual_lmp"].to_numpy(dtype=float)
            y_naive = None
            naive_full = _naive_last_week(pool, resolved_date)
            if naive_full is not None:
                y_naive = naive_full[merged["hour_ending"].astype(int).values - 1]
            metrics = evaluate_forecast(y_true, merged, quantiles, y_naive=y_naive)

    if not quiet:
        print_config(config, spec, resolved_date, day_type)
        print_analogs(analogs, resolved_date, config.hub)
        print_forecast(output_table, metrics if metrics else None)
        print_quantiles(quantiles_table)

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "analogs": analogs,
        "metrics": metrics,
        "forecast_date": str(resolved_date),
        "day_type": day_type,
        "has_actuals": has_actuals,
        "n_pool": len(pool),
        "n_analogs_used": int(analogs["date"].nunique()) if len(analogs) else 0,
        "scenario": spec.name,
        "df_forecast": df_forecast,
    }


if __name__ == "__main__":
    run()
