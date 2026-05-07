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
from da_models.like_day_model_knn.calendar import FunnelCounts  # noqa: E402
from da_models.like_day_model_knn.analog_store import (  # noqa: E402
    DEFAULT_STORE_DIR,
    write_analog_explainability,
)
from da_models.like_day_model_knn.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool,
    build_query_row,
)
from da_models.like_day_model_knn.pjm_rto_hourly.engine import find_twins  # noqa: E402
from da_models.common.configs import HOURS  # noqa: E402
from da_models.common.forecast.output import (  # noqa: E402
    actuals_from_pool,
    build_output_table,
)
from da_models.like_day_model_knn.pjm_rto_hourly.forecast import (  # noqa: E402
    build_quantiles_table,
    hourly_forecast_from_hour_analogs,
)
from da_models.like_day_model_knn.pjm_rto_hourly.metrics import evaluate_forecast  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.printers import (  # noqa: E402
    print_config,
    print_forecast,
    print_pool_funnel,
    print_quantiles,
)


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = None  # None -> tomorrow (date.today() + timedelta(days=1))
# Sunny-aligned spec: load + load_ramp_1h + load_ramp_3h + solar + wind +
# net_load + temperature (windowed) plus outage + gas + calendar (broadcast).
# Replaces the legacy 5-feature ``pjm_rto_hourly`` spec — see CLAUDE.md /
# domains.py for the rationale (path B sunny alignment).
MODEL_NAME: str = configs.PJM_RTO_HOURLY_SUNNY_ALIGNED_SPEC.name
FLT_RADIUS: int = configs.PJM_RTO_HOURLY_SUNNY_ALIGNED_SPEC.flt_radius
N_ANALOGS: int | None = None  # None -> configs.DEFAULT_N_ANALOGS
SEASON_WINDOW_DAYS: int | None = None  # None -> configs.SEASON_WINDOW_DAYS
MIN_POOL_SIZE: int | None = None  # None -> configs.MIN_POOL_SIZE
WRITE_ANALOG_STORE: bool = True
ANALOG_STORE_DIR: Path | None = None  # None -> DEFAULT_STORE_DIR

# 80% PI (P10/P90) + IQR (P25/P75) + median. P01/P05/P95/P99 are dropped
# because they're statistically unreliable with 20 analogs — they pin to
# min/max with no real resolution. Drop also disables 90%/98% coverage
# metrics in ``evaluate_forecast`` (it falls back to None for those).
DEFAULT_QUANTILES: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)
DISPLAY_QUANTILES: tuple[float, ...] = DEFAULT_QUANTILES


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _naive_last_week(pool: pd.DataFrame, target_date: date) -> np.ndarray | None:
    """Naive baseline: same-day-last-week DA LMP profile (24 hours)."""
    actuals = actuals_from_pool(pool, target_date - timedelta(days=7))
    if actuals is None:
        return None
    return np.array([actuals[h] for h in HOURS], dtype=float)


_BLOCK_INDICES: dict[str, np.ndarray] = {
    "OnPeak": np.array(range(7, 23), dtype=int),
    "OffPeak": np.array(list(range(0, 7)) + [23], dtype=int),
    "Flat": np.array(range(0, 24), dtype=int),
}


def _block_level_metrics(
    actual_arr: np.ndarray,
    forecast_arr: np.ndarray,
    naive_full: np.ndarray | None,
) -> dict[str, dict[str, float]]:
    """Per-block level metrics. Returns {block: {mae, rmse, mape, rmae}} with
    NaN entries when inputs miss values or denominators degenerate."""
    out: dict[str, dict[str, float]] = {}
    for name, idx in _BLOCK_INDICES.items():
        a = actual_arr[idx]
        f = forecast_arr[idx]
        nan_row = {k: float("nan") for k in ("mae", "rmse", "mape", "rmae")}
        if np.isnan(a).any() or np.isnan(f).any():
            out[name] = nan_row
            continue
        err = f - a
        mae_ = float(np.mean(np.abs(err)))
        rmse_ = float(np.sqrt(np.mean(err**2)))
        with np.errstate(divide="ignore", invalid="ignore"):
            mape_arr = np.abs(err) / np.where(a == 0, np.nan, np.abs(a))
        mape_ = (
            float(np.nanmean(mape_arr) * 100.0)
            if np.isfinite(mape_arr).any()
            else float("nan")
        )
        rmae_ = float("nan")
        if naive_full is not None and not np.isnan(naive_full[idx]).any():
            naive_mae = float(np.mean(np.abs(naive_full[idx] - a)))
            if naive_mae > 0:
                rmae_ = mae_ / naive_mae
        out[name] = {"mae": mae_, "rmse": rmse_, "mape": mape_, "rmae": rmae_}
    return out


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
    y_naive_override: np.ndarray | None = None,
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

    ``y_naive_override`` — length-24 hourly LMP profile to use as the
    rMAE denominator instead of the default same-day-last-week
    persistence. ``None`` keeps the historical behavior. Used by the
    naive_baselines integration to swap in the EPF baseline.
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
            configs.SEASON_WINDOW_DAYS
            if season_window_days is None
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
            schema=config.schema,
            hub=config.hub,
            cache_dir=configs.CACHE_DIR,
            spec=spec,
            label_source=config.label_source,
        )
    if query is None:
        query = build_query_row(
            target_date=resolved_date,
            schema=config.schema,
            cache_dir=configs.CACHE_DIR,
            spec=spec,
        )
    if dates_meta is None:
        dates_meta = _shared.load_dates_daily(configs.CACHE_DIR)

    funnel = FunnelCounts()
    analogs = find_twins(
        query=query,
        pool=pool,
        target_date=resolved_date,
        spec=spec,
        n_analogs=config.n_analogs,
        season_window_days=config.season_window_days,
        min_pool_size=config.min_pool_size,
        dates_meta=dates_meta,
        same_dow_group=config.same_dow_group,
        same_weekend_group=config.same_weekend_group,
        same_weekend_group_for_weekends=config.same_weekend_group_for_weekends,
        exclude_holidays=config.exclude_holidays,
        exclude_dates=config.exclude_dates,
        max_age_years=config.max_age_years,
        recency_half_life_days=config.recency_half_life_days,
        feature_group_weights_override=feature_group_weights_override,
        funnel=funnel,
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
    quantiles_table = build_quantiles_table(
        resolved_date,
        df_forecast,
        display_quantiles,
        analogs=analogs,
        pool=pool,
    )

    metrics: dict = {}
    block_level: dict = {}
    if has_actuals and len(df_forecast) > 0:
        point_col = (
            "point_forecast" if "point_forecast" in df_forecast.columns else "q_0.50"
        )
        fc_by_he = dict(
            zip(
                df_forecast["hour_ending"].astype(int),
                df_forecast[point_col].astype(float),
            )
        )
        actual_arr = np.array([actuals.get(h, np.nan) for h in HOURS], dtype=float)
        forecast_arr = np.array([fc_by_he.get(h, np.nan) for h in HOURS], dtype=float)
        naive_full = (
            y_naive_override
            if y_naive_override is not None
            else _naive_last_week(pool, resolved_date)
        )

        merged = df_forecast.copy()
        merged["actual_lmp"] = merged["hour_ending"].map(actuals)
        merged = merged.dropna(subset=["actual_lmp"])
        if len(merged) > 0:
            y_true = merged["actual_lmp"].to_numpy(dtype=float)
            y_naive = (
                naive_full[merged["hour_ending"].astype(int).values - 1]
                if naive_full is not None
                else None
            )
            metrics = evaluate_forecast(y_true, merged, quantiles, y_naive=y_naive)

        block_level = _block_level_metrics(actual_arr, forecast_arr, naive_full)

    in_band_80: list[bool | None] = []
    crps_per_hour = np.full(24, np.nan)
    if has_actuals and quantiles_table is not None and len(quantiles_table) > 0:
        p10_rows = quantiles_table[quantiles_table["Type"] == "P10"]
        p90_rows = quantiles_table[quantiles_table["Type"] == "P90"]
        if len(p10_rows) and len(p90_rows):
            p10 = p10_rows.iloc[0]
            p90 = p90_rows.iloc[0]
            for h in HOURS:
                actual_h = actuals.get(h) if actuals else None
                lo = p10.get(f"HE{h}")
                hi = p90.get(f"HE{h}")
                if actual_h is None or pd.isna(lo) or pd.isna(hi):
                    in_band_80.append(None)
                else:
                    in_band_80.append(bool(lo <= actual_h <= hi))

        if has_actuals and len(df_forecast) > 0:
            merged_full = df_forecast.copy()
            merged_full["actual_lmp"] = merged_full["hour_ending"].map(actuals)
            merged_full = merged_full.dropna(subset=["actual_lmp"])
            for _, mr in merged_full.iterrows():
                h_idx = int(mr["hour_ending"]) - 1
                if not (0 <= h_idx < 24):
                    continue
                actual_h = float(mr["actual_lmp"])
                per_q = []
                for q in quantiles:
                    col = f"q_{q:.2f}"
                    if col in merged_full.columns and pd.notna(mr[col]):
                        p_val = float(mr[col])
                        e = actual_h - p_val
                        per_q.append(max(q * e, (q - 1.0) * e))
                if per_q:
                    crps_per_hour[h_idx] = 2.0 * float(np.mean(per_q))

    if not quiet:
        from da_models.like_day_model_knn.pjm_rto_hourly.printers import (
            print_analog_features,
            print_band_calibration,
        )
        from utils.logging_utils import print_divider, print_header

        print_config(config, spec, resolved_date, day_type)
        print_pool_funnel(funnel, resolved_date, day_type, config.hub)
        print_analog_features(analogs, pool, query, resolved_date, config.hub)

        print_header(
            f"LIKE-DAY FORECAST — {config.hub} ($/MWh)  |  {resolved_date}",
            "=",
            120,
        )
        print_quantiles(quantiles_table)
        print_forecast(
            output_table,
            block_level=block_level if block_level else None,
        )
        print_band_calibration(
            output_table,
            quantiles_table,
            in_band_80=in_band_80 if in_band_80 else None,
            crps_per_hour=crps_per_hour,
        )
        print()
        print_divider("=", 120, dim=False)
        print()

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "analogs": analogs,
        "metrics": metrics,
        "block_level": block_level,
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
