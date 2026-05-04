"""Single-day pjm_rto_hourly forecast — terminal output.

Mirrors helioscta-pjm-da/backend/src/like_day_forecast/pipelines/forecast.py
in print layout (FORECAST CONFIGURATION block, LIKE-DAY ANALOG DAYS table,
DA LMP LIKE-DAY FORECAST table with metrics, Quantile Bands table).

Returns a dict with output_table / quantiles_table / analogs / metrics
for programmatic callers (Streamlit Run page); writes nothing to disk
beyond the optional parquet explainability store.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day --date 2026-05-05
    python -m da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day --date 2026-05-05 --model pjm_rto_hourly_levels
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta
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


_PJM_RTO_HOURLY_MODELS: tuple[str, ...] = (
    configs.PJM_RTO_HOURLY_SPEC.name,
    configs.PJM_RTO_HOURLY_LEVELS_SPEC.name,
)

# Quantiles needed by the printed bands table (P25..P75 inner) PLUS the
# wider levels (P10/P90, P05/P95, P01/P99) that evaluate_forecast uses
# for 80/90/98% prediction-interval coverage.
DEFAULT_QUANTILES: list[float] = [
    0.01, 0.05, 0.10,
    0.25, 0.375, 0.50, 0.625, 0.75,
    0.90, 0.95, 0.99,
]
DISPLAY_QUANTILES: list[float] = [0.25, 0.375, 0.50, 0.625, 0.75]


def _naive_last_week(pool: pd.DataFrame, target_date: date) -> np.ndarray | None:
    """Naive baseline: same-day-last-week DA LMP profile (24 hours)."""
    seven_back = target_date - timedelta(days=7)
    actuals = actuals_from_pool(pool, seven_back)
    if actuals is None:
        return None
    return np.array([actuals[h] for h in configs.HOURS], dtype=float)


def generate(
    target_date: date,
    flt_radius: int = configs.PJM_RTO_HOURLY_SPEC.flt_radius,
    n_analogs: int | None = None,
    season_window_days: int | None = None,
    min_pool_size: int | None = None,
    write_analog_store: bool = True,
    analog_store_dir: Path | None = None,
    model_name: str = configs.PJM_RTO_HOURLY_SPEC.name,
    quantiles: list[float] | None = None,
    display_quantiles: list[float] | None = None,
) -> dict:
    """Run the forecast and print the four-section terminal report.

    Returns a dict with: output_table, quantiles_table, analogs, metrics,
    forecast_date, day_type, has_actuals, n_pool, n_analogs_used, scenario.
    """
    if model_name not in _PJM_RTO_HOURLY_MODELS:
        raise ValueError(
            f"model_name='{model_name}' not in pjm_rto_hourly family {_PJM_RTO_HOURLY_MODELS}"
        )

    quantiles = list(quantiles) if quantiles is not None else list(DEFAULT_QUANTILES)
    display_quantiles = (
        list(display_quantiles) if display_quantiles is not None else list(DISPLAY_QUANTILES)
    )

    base_config = configs.KnnModelConfig(
        forecast_date=str(target_date),
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
    config, day_type = base_config.with_day_type_overrides(target_date)
    base_spec = config.resolved_spec()
    spec = replace(base_spec, flt_radius=int(flt_radius))

    pool = build_pool(
        schema=config.schema, hub=config.hub, cache_dir=configs.CACHE_DIR, spec=spec,
    )
    query = build_query_row(
        target_date=target_date, schema=config.schema, cache_dir=configs.CACHE_DIR, spec=spec,
    )
    dates_meta = _shared.load_dates_daily(configs.CACHE_DIR)

    analogs = find_twins(
        query=query, pool=pool, target_date=target_date, spec=spec,
        n_analogs=config.n_analogs,
        season_window_days=config.season_window_days,
        min_pool_size=config.min_pool_size,
        dates_meta=dates_meta,
        same_dow_group=config.same_dow_group,
        exclude_holidays=config.exclude_holidays,
        exclude_dates=config.exclude_dates,
        max_age_years=config.max_age_years,
        recency_half_life_years=config.recency_half_life_years,
    )

    if write_analog_store:
        store_dir = analog_store_dir or DEFAULT_STORE_DIR
        write_analog_explainability(
            target_date=target_date,
            config=config,
            spec=spec,
            pool=pool,
            query=query,
            analogs=analogs,
            output_dir=store_dir,
        )

    df_forecast = hourly_forecast_from_hour_analogs(analogs, quantiles)

    actuals = actuals_from_pool(pool, target_date)
    has_actuals = actuals is not None
    output_table = build_output_table(target_date, df_forecast, actuals)
    quantiles_table = build_quantiles_table(target_date, df_forecast, display_quantiles)

    metrics: dict = {}
    if has_actuals and len(df_forecast) > 0:
        merged = df_forecast.copy()
        merged["actual_lmp"] = merged["hour_ending"].map(actuals)
        merged = merged.dropna(subset=["actual_lmp"])
        if len(merged) > 0:
            y_true = merged["actual_lmp"].to_numpy(dtype=float)
            y_naive = None
            naive_full = _naive_last_week(pool, target_date)
            if naive_full is not None:
                naive_full = naive_full[merged["hour_ending"].astype(int).values - 1]
                y_naive = naive_full
            metrics = evaluate_forecast(y_true, merged, quantiles, y_naive=y_naive)

    print_config(config, spec, target_date, day_type)
    print_analogs(analogs, target_date, config.hub)
    print_forecast(output_table, metrics if metrics else None)
    print_quantiles(quantiles_table)

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "analogs": analogs,
        "metrics": metrics,
        "forecast_date": str(target_date),
        "day_type": day_type,
        "has_actuals": has_actuals,
        "n_pool": len(pool),
        "n_analogs_used": int(analogs["date"].nunique()) if len(analogs) else 0,
        "scenario": spec.name,
        "df_forecast": df_forecast,
    }


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="pjm_rto_hourly single-day forecast (terminal output).",
    )
    parser.add_argument("--date", type=_parse_date, required=True,
                        help="Target date YYYY-MM-DD.")
    parser.add_argument("--model", type=str, default=configs.PJM_RTO_HOURLY_SPEC.name,
                        choices=_PJM_RTO_HOURLY_MODELS,
                        help="pjm_rto_hourly model spec to run (default: %(default)s).")
    parser.add_argument("--flt-radius", type=int,
                        default=configs.PJM_RTO_HOURLY_SPEC.flt_radius,
                        help="Half-width of the temporal feature window (default: %(default)d).")
    parser.add_argument("--analog-store-dir", type=Path, default=None,
                        help=f"Parquet explainability store directory (default: {DEFAULT_STORE_DIR}).")
    parser.add_argument("--skip-analog-store", action="store_true",
                        help="Do not write parquet explainability tables.")
    return parser.parse_args()


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    args = _parse_args()
    generate(
        target_date=args.date,
        flt_radius=args.flt_radius,
        write_analog_store=not args.skip_analog_store,
        analog_store_dir=args.analog_store_dir,
        model_name=args.model,
    )


if __name__ == "__main__":
    main()
