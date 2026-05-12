"""Single-day pjm_rto_hourly forecast (Sunny variant) - terminal output.

Mirrors the print layout of
``like_day_model_knn/pjm_rto_hourly/pipelines/forecast_single_day.py`` so
a side-by-side run produces visually-comparable terminal output:
FORECAST CONFIGURATION, POOL SUMMARY, LIKE-DAY ANALOGS, DA LMP LIKE-DAY
FORECAST (with metrics), Quantile Bands.

``run()`` returns the dict from ``forecast.run_forecast`` augmented with
``df_forecast`` (per-HE point + quantile cols) and ``metrics`` (when
actuals are present). Tunable defaults live as module-level constants.

Usage::

    python -m backend.modelling.da_models.like_day_model_knn_sunny.pjm_rto_hourly.pipelines.forecast_single_day
    python modelling/da_models/like_day_model_knn_sunny/pjm_rto_hourly/pipelines/forecast_single_day.py
"""

from __future__ import annotations

import sys
import uuid
from datetime import date, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[6]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from backend.modelling.da_models.like_day_model_knn_sunny import configs  # noqa: E402
from backend.modelling.da_models.like_day_model_knn_sunny.pjm_rto_hourly import (  # noqa: E402
    forecast,
    metrics as metrics_mod,
    printers,
)
from backend.modelling.da_models.like_day_model_knn_sunny.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool,
)
from backend.modelling.da_models.common.publish import publish_forecast_run  # noqa: E402


# ── Defaults ──────────────────────────────────────────────────────────
TARGET_DATE: date | None = None  # None -> tomorrow
# Forecast vintage -- the date the run is produced (None -> date.today()).
RUN_DATE: date | None = None
MODEL_NAME: str = configs.PJM_RTO_HOURLY_SUNNY_SPEC.name
# Frontend ingestion identity for pjm_model_outputs.forecast_runs -- stable,
# decoupled from the spec key (the published name is what the frontend reads).
PUBLISHED_MODEL_NAME: str = "pjm_rto_hourly_sunny"
PUBLISHED_MODEL_FAMILY: str = "like_day"
N_ANALOGS: int | None = None  # None -> configs.DEFAULT_N_ANALOGS
SEASON_WINDOW_DAYS: int | None = None
MIN_POOL_SIZE: int | None = None
LABEL_SOURCE: str = configs.LABEL_SOURCE
RECENCY_HALF_LIFE_DAYS: float | None = None
# The pipeline always publishes the run to pjm_model_outputs.forecast_runs
# (one row, upserted via backend.modelling.da_models.common.publish.publish_forecast_run) so the
# frontend can read it. Batch/backtest callers that must NOT write a row per
# date pass publish=False to run().
PUBLISH: bool = True

# Quantiles for the printed bands table (P25..P75) AND the wider levels
# (P10/P90, P05/P95, P01/P99) that evaluate_forecast uses for 80/90/98%
# prediction-interval coverage. Mirrors the sibling wide pipeline.
DEFAULT_QUANTILES: tuple[float, ...] = (
    0.01,
    0.05,
    0.10,
    0.25,
    0.375,
    0.50,
    0.625,
    0.75,
    0.90,
    0.95,
    0.99,
)
DISPLAY_QUANTILES: tuple[float, ...] = (0.25, 0.375, 0.50, 0.625, 0.75)


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _naive_last_week(pool: pd.DataFrame, target_date: date) -> np.ndarray | None:
    """Naive baseline: same-day-last-week DA LMP profile (24 hours)."""
    last_week = target_date - timedelta(days=7)
    sub = pool[pool["date"] == last_week]
    if len(sub) == 0:
        return None
    by_he: dict[int, float] = {}
    for _, r in sub.iterrows():
        v = r.get("lmp")
        if pd.notna(v):
            by_he[int(r["hour_ending"])] = float(v)
    if len(by_he) < 12:
        return None
    return np.array([by_he.get(h, np.nan) for h in range(1, 25)], dtype=float)


def run(
    target_date: date | None = TARGET_DATE,
    run_date: date | None = RUN_DATE,
    model_name: str = MODEL_NAME,
    n_analogs: int | None = N_ANALOGS,
    season_window_days: int | None = SEASON_WINDOW_DAYS,
    min_pool_size: int | None = MIN_POOL_SIZE,
    label_source: str = LABEL_SOURCE,
    recency_half_life_days: float | None = RECENCY_HALF_LIFE_DAYS,
    quantiles: tuple[float, ...] | list[float] | None = None,
    display_quantiles: tuple[float, ...] | list[float] | None = None,
    pool: pd.DataFrame | None = None,
    publish: bool = PUBLISH,
    quiet: bool = False,
    y_naive_override: np.ndarray | None = None,
    feature_group_weights_override: dict[str, float] | None = None,
) -> dict:
    """Run the Sunny single-day forecast and print the five-section report.

    Returns the dict from ``forecast.run_forecast`` plus ``df_forecast``
    (per-HE point + quantile cols) and ``metrics`` (when actuals
    available; empty dict otherwise).

    ``pool`` - pre-built pool to skip the ~5-10s build (notebook reuse).
    ``quiet`` - suppress prints, still return the dict.
    ``y_naive_override`` - length-24 array to use as the rMAE
    denominator instead of the default same-day-last-week persistence.
    ``feature_group_weights_override`` - patch onto the spec's raw
    weights (missing keys keep their spec value). Common ablation:
    ``{"outage_daily": 0.0}`` to zero out outages.
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
    resolved_run_date = run_date if run_date is not None else date.today()
    run_id = str(uuid.uuid4())
    quantiles_list = list(quantiles if quantiles is not None else DEFAULT_QUANTILES)
    display_q = list(
        display_quantiles if display_quantiles is not None else DISPLAY_QUANTILES
    )

    base_cfg = configs.KnnModelConfig(
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
    resolved_cfg, day_type = base_cfg.with_day_type_overrides(resolved_date)
    spec = resolved_cfg.resolved_spec()

    if pool is None:
        pool = build_pool(
            hub=resolved_cfg.hub,
            label_source=resolved_cfg.label_source,
            cache_dir=configs.CACHE_DIR,
            spec=spec,
        )

    result = forecast.run_forecast(
        target_date=resolved_date,
        config=base_cfg,
        cache_dir=configs.CACHE_DIR,
        pool=pool,
        feature_group_weights_override=feature_group_weights_override,
    )

    analogs = result["analogs"]
    df_forecast = forecast.hourly_forecast_from_hour_analogs(analogs, quantiles_list)
    quantiles_table = forecast.build_quantiles_table(
        resolved_date, df_forecast, display_q, analogs=analogs
    )

    if publish:
        from backend.modelling.da_models.like_day_model_knn_sunny.publish import (  # noqa: PLC0415
            build_payload,
            extract_onpeak_forecast,
        )

        payload = build_payload(
            df_forecast=df_forecast,
            quantiles_table=quantiles_table,
            analogs=analogs,
            pool=pool,
            output_table=result["output_table"],
            target_date=resolved_date,
            run_date=resolved_run_date,
            model_name=PUBLISHED_MODEL_NAME,
            model_family=PUBLISHED_MODEL_FAMILY,
            run_id=run_id,
            hub=resolved_cfg.hub,
            day_type=day_type,
            n_analogs=int(resolved_cfg.n_analogs),
            quantiles=quantiles_list,
        )
        publish_forecast_run(
            model_name=PUBLISHED_MODEL_NAME,
            model_family=PUBLISHED_MODEL_FAMILY,
            target_date=resolved_date,
            run_date=resolved_run_date,
            run_id=run_id,
            payload=payload,
            da_lmp_total_onpeak_forecast=extract_onpeak_forecast(payload),
        )

    metrics: dict = {}
    if result["has_actuals"] and len(df_forecast) > 0:
        actuals_long = pool[pool["date"] == resolved_date]
        actuals_by_he = {
            int(r["hour_ending"]): float(r["lmp"])
            for _, r in actuals_long.iterrows()
            if pd.notna(r.get("lmp"))
        }
        merged = df_forecast.copy()
        merged["actual_lmp"] = merged["hour_ending"].map(actuals_by_he)
        merged = merged.dropna(subset=["actual_lmp"])
        if len(merged) > 0:
            y_true = merged["actual_lmp"].to_numpy(dtype=float)
            y_naive = None
            naive_full = (
                y_naive_override
                if y_naive_override is not None
                else _naive_last_week(pool, resolved_date)
            )
            if naive_full is not None:
                y_naive = naive_full[merged["hour_ending"].astype(int).values - 1]
            metrics = metrics_mod.evaluate_forecast(
                y_true, merged, quantiles_list, y_naive=y_naive
            )

    if not quiet:
        # Need a query frame for the analog-features printer's per-HE z sub-strips.
        from backend.modelling.da_models.like_day_model_knn_sunny.pjm_rto_hourly.builder import (
            build_query_row,
        )

        query = build_query_row(
            target_date=resolved_date,
            cache_dir=configs.CACHE_DIR,
            spec=spec,
        )

        printers.print_config(
            resolved_cfg,
            spec,
            resolved_date,
            day_type,
            effective_weights=result.get("feature_weights"),
        )
        printers.print_pool_summary(
            pool, analogs, resolved_cfg, resolved_date, day_type
        )
        printers.print_analog_features(
            analogs, pool, query, resolved_date, resolved_cfg.hub
        )
        printers.print_forecast(result["output_table"], metrics if metrics else None)
        printers.print_quantiles(quantiles_table)

    out = dict(result)
    out["df_forecast"] = df_forecast
    out["quantiles_table"] = quantiles_table
    out["metrics"] = metrics
    out["day_type"] = day_type
    out["n_pool"] = len(pool)
    out["run_id"] = run_id
    out["run_date"] = str(resolved_run_date)
    return out


if __name__ == "__main__":
    run()
