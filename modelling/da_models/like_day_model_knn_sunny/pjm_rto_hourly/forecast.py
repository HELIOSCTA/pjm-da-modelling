"""Hourly forecast assembly for pjm_rto_hourly (Sunny variant).

Aggregates per-(hour, analog) tuples into a 24-hour forecast, then
overlays MC-derived joint quantile bands on the OnPeak/OffPeak/Flat
aggregates so synthetic-day correlated tail risk is reflected.

Faithful to forecast.py:605-638 (``_aggregate_quantile_bands``),
forecast.py:94-100 (``_weighted_quantile``), and forecast.py:643-669
(``_summarize`` / ``_build_output_table``).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.like_day_model_knn_sunny import configs
from da_models.like_day_model_knn_sunny.calendar import (
    FunnelCounts,
    load_pjm_dates_daily,
)
from da_models.like_day_model_knn_sunny.pjm_rto_hourly.builder import (
    build_pool,
    build_query_row,
)
from da_models.like_day_model_knn_sunny.pjm_rto_hourly.engine import find_twins

logger = logging.getLogger(__name__)


HOURS: tuple[int, ...] = tuple(range(1, 25))


def weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    """Weight-aware quantile via cumulative-weight interpolation."""
    idx = np.argsort(values)
    v = values[idx]
    w = weights[idx]
    cdf = np.cumsum(w)
    cdf = cdf / cdf[-1]
    return float(np.interp(q, cdf, v))


_weighted_quantile = weighted_quantile  # backward-compat alias for internal callers


def hourly_forecast_from_hour_analogs(
    analogs: pd.DataFrame,
    quantiles: list[float],
) -> pd.DataFrame:
    """Per-HE point forecast + quantiles from the engine's analog table.

    Expects ``analogs`` columns: ``hour_ending, weight, lmp``. Produces
    one row per HE with ``point_forecast`` and ``q_{q:.2f}`` columns.
    Weights are renormalized within each HE before averaging — matches
    ``run_forecast`` internals and the sibling wide-pool helper.
    """
    if len(analogs) == 0 or not {"hour_ending", "weight", "lmp"}.issubset(
        analogs.columns
    ):
        return pd.DataFrame()

    rows: list[dict] = []
    for h in HOURS:
        sub = analogs[analogs["hour_ending"] == h].dropna(subset=["lmp"])
        if len(sub) == 0:
            continue
        values = sub["lmp"].to_numpy(dtype=float)
        w = sub["weight"].to_numpy(dtype=float)
        if w.sum() <= 0:
            continue
        w = w / w.sum()
        row = {"hour_ending": h, "point_forecast": float(np.average(values, weights=w))}
        for q in quantiles:
            row[f"q_{q:.2f}"] = weighted_quantile(values, w, q)
        rows.append(row)
    return pd.DataFrame(rows)


def aggregate_quantile_bands_from_analogs(
    analogs: pd.DataFrame,
    quantiles: list[float],
    hour_groups: dict[str, list[int]] | None = None,
    n_draws: int = 2000,
    seed: int = 7,
) -> dict[str, dict[float, float]]:
    """MC joint quantile bands for OnPeak/OffPeak/Flat aggregates.

    Convenience wrapper around ``_aggregate_quantile_bands`` that takes
    the analogs DataFrame directly so callers don't have to reshape.
    """
    if hour_groups is None:
        hour_groups = {
            "OnPeak": list(configs.ONPEAK_HOURS),
            "OffPeak": list(configs.OFFPEAK_HOURS),
            "Flat": list(HOURS),
        }
    per_hour: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    for h in HOURS:
        sub = analogs[analogs["hour_ending"] == h].dropna(subset=["lmp"])
        if len(sub) == 0:
            continue
        vals = sub["lmp"].to_numpy(dtype=float)
        w = sub["weight"].to_numpy(dtype=float)
        if w.sum() <= 0:
            continue
        w = w / w.sum()
        per_hour[h] = (vals, w)
    return _aggregate_quantile_bands(per_hour, hour_groups, quantiles, n_draws, seed)


def build_quantiles_table(
    target_date: date,
    df_forecast: pd.DataFrame,
    display_quantiles: list[float] | tuple[float, ...] = (
        0.25,
        0.375,
        0.50,
        0.625,
        0.75,
    ),
    analogs: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Wide quantile-bands table — P-rows + Forecast row inserted between P50 and the next-higher band.

    Mirrors the sibling wide-pool ``build_quantiles_table`` shape so the
    terminal report is visually identical. When ``analogs`` is provided,
    OnPeak/OffPeak/Flat summary cells are overridden by the MC joint
    bands (Sunny's algorithmic improvement); otherwise the naive
    per-hour-quantile mean is used.
    """
    cols = ["Date", "Type"] + [f"HE{h}" for h in HOURS] + ["OnPeak", "OffPeak", "Flat"]
    if len(df_forecast) == 0:
        return pd.DataFrame(columns=cols)

    rows: list[dict] = []
    for q in sorted(display_quantiles):
        col = f"q_{q:.2f}"
        if col not in df_forecast.columns:
            continue
        label = _quantile_label(q)
        row: dict = {"Date": target_date, "Type": label}
        for _, r in df_forecast.iterrows():
            row[f"HE{int(r['hour_ending'])}"] = (
                float(r[col]) if pd.notna(r[col]) else None
            )
        rows.append(_summarize(row))

    forecast_row: dict = {"Date": target_date, "Type": "Forecast"}
    for _, r in df_forecast.iterrows():
        forecast_row[f"HE{int(r['hour_ending'])}"] = (
            float(r["point_forecast"]) if pd.notna(r.get("point_forecast")) else None
        )
    forecast_row = _summarize(forecast_row)

    insert_at = next(
        (i for i, row in enumerate(rows) if row["Type"] == "P50"),
        len(rows) // 2,
    )
    rows.insert(insert_at + 1, forecast_row)

    if analogs is not None and len(analogs) > 0:
        bands = aggregate_quantile_bands_from_analogs(analogs, list(display_quantiles))
        for row in rows:
            if row["Type"] == "Forecast":
                continue
            try:
                q_for_row = float(row["Type"][1:]) / 100.0
            except (TypeError, ValueError):
                continue
            for label in ("OnPeak", "OffPeak", "Flat"):
                v = bands.get(label, {}).get(q_for_row)
                if v is not None:
                    row[label] = v

    return pd.DataFrame(rows, columns=cols)


def _summarize(row: dict) -> dict:
    on = [row.get(f"HE{h}") for h in configs.ONPEAK_HOURS]
    off = [row.get(f"HE{h}") for h in configs.OFFPEAK_HOURS]
    flat = [row.get(f"HE{h}") for h in HOURS]
    on = [v for v in on if v is not None and not pd.isna(v)]
    off = [v for v in off if v is not None and not pd.isna(v)]
    flat = [v for v in flat if v is not None and not pd.isna(v)]
    row["OnPeak"] = float(np.mean(on)) if on else float("nan")
    row["OffPeak"] = float(np.mean(off)) if off else float("nan")
    row["Flat"] = float(np.mean(flat)) if flat else float("nan")
    return row


def _build_output_table(
    target_date: date,
    forecast_hourly: dict[int, float],
    actual_hourly: dict[int, float] | None,
) -> pd.DataFrame:
    rows: list[dict] = []
    if actual_hourly:
        rows.append(
            _summarize(
                {
                    "Date": target_date,
                    "Type": "Actual",
                    **{f"HE{h}": actual_hourly.get(h) for h in HOURS},
                }
            )
        )
    rows.append(
        _summarize(
            {
                "Date": target_date,
                "Type": "Forecast",
                **{f"HE{h}": forecast_hourly.get(h) for h in HOURS},
            }
        )
    )
    if actual_hourly:
        err: dict = {}
        for h in HOURS:
            f = forecast_hourly.get(h)
            a = actual_hourly.get(h)
            err[f"HE{h}"] = (
                (f - a)
                if (
                    f is not None
                    and a is not None
                    and not pd.isna(f)
                    and not pd.isna(a)
                )
                else None
            )
        rows.append(_summarize({"Date": target_date, "Type": "Error", **err}))
    cols = ["Date", "Type"] + [f"HE{h}" for h in HOURS] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _aggregate_quantile_bands(
    per_hour: dict[int, tuple[np.ndarray, np.ndarray]],
    hour_groups: dict[str, list[int]],
    quantiles: list[float],
    n_draws: int = 2000,
    seed: int = 7,
) -> dict[str, dict[float, float]]:
    rng = np.random.default_rng(seed)
    out: dict[str, dict[float, float]] = {}
    for label, hours in hour_groups.items():
        usable = [h for h in hours if h in per_hour and len(per_hour[h][0]) > 0]
        if not usable:
            out[label] = {q: float("nan") for q in quantiles}
            continue
        draws = np.zeros((n_draws, len(usable)), dtype=float)
        for j, h in enumerate(usable):
            vals, ws = per_hour[h]
            ws = ws / ws.sum()
            idx = rng.choice(len(vals), size=n_draws, p=ws)
            draws[:, j] = vals[idx]
        agg = draws.mean(axis=1)
        agg.sort()
        out[label] = {q: float(np.quantile(agg, q)) for q in quantiles}
    return out


def _quantile_label(q: float) -> str:
    pct = q * 100.0
    if float(pct).is_integer():
        return f"P{int(pct):02d}"
    return f"P{pct:.1f}".rstrip("0").rstrip(".")


def _actuals_long(pool: pd.DataFrame, target_date: date) -> dict[int, float] | None:
    sub = pool[pool["date"] == target_date]
    if len(sub) == 0:
        return None
    out: dict[int, float] = {}
    for _, r in sub.iterrows():
        v = r.get("lmp")
        if pd.notna(v):
            out[int(r["hour_ending"])] = float(v)
    if len(out) < 12:
        return None
    return out


def run_forecast(
    target_date: date | None = None,
    config: configs.KnnModelConfig | None = None,
    cache_dir: Path | None = None,
    pool: pd.DataFrame | None = None,
    feature_group_weights_override: dict[str, float] | None = None,
    query: pd.DataFrame | None = None,
) -> dict:
    """Run the Sunny KNN forecast for one target date.

    ``query`` lets callers inject a pre-built 24-row feature frame —
    the multi-day ``meteo_rto_hourly`` horizon pipeline uses it to feed
    Meteologica-sourced + forward-filled feature rows without re-running
    ``build_query_row``'s lead-1 PJM chain. When ``None`` (default), the
    query is built in-process via ``build_query_row(target_date, ...)``.
    """
    cfg = config or configs.KnnModelConfig()
    if target_date is None:
        target_date = cfg.resolved_target_date()
    target_date = pd.to_datetime(target_date).date()
    cache_dir = cache_dir or configs.CACHE_DIR

    cfg, day_type = cfg.with_day_type_overrides(target_date)
    spec = cfg.resolved_spec()
    quantiles = cfg.resolved_quantiles()

    if pool is None:
        pool = build_pool(
            hub=cfg.hub,
            label_source=cfg.label_source,
            cache_dir=cache_dir,
            spec=spec,
        )
    if query is None:
        query = build_query_row(
            target_date=target_date, cache_dir=cache_dir, spec=spec
        )
    dates_meta = load_pjm_dates_daily(cache_dir=cache_dir)

    from da_models.like_day_model_knn_sunny.pjm_rto_hourly.engine import (
        _effective_weights,
    )

    weights = _effective_weights(spec, feature_group_weights_override)

    funnel = FunnelCounts()
    analogs = find_twins(
        query=query,
        pool=pool,
        target_date=target_date,
        spec=spec,
        n_analogs=cfg.n_analogs,
        season_window_days=cfg.season_window_days,
        min_pool_size=cfg.min_pool_size,
        dates_meta=dates_meta,
        same_dow_group=cfg.same_dow_group,
        same_weekend_group=cfg.same_weekend_group,
        same_weekend_group_for_weekends=cfg.same_weekend_group_for_weekends,
        exclude_holidays=cfg.exclude_holidays,
        exclude_dates=cfg.exclude_dates,
        recency_half_life_days=cfg.recency_half_life_days,
        feature_group_weights_override=feature_group_weights_override,
        funnel=funnel,
    )

    forecast_hourly: dict[int, float] = {}
    quantiles_hourly: dict[int, dict[float, float]] = {}
    per_hour_dist: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    n_used: list[int] = []

    for h in HOURS:
        sub = analogs[analogs["hour_ending"] == h].dropna(subset=["lmp"])
        if len(sub) == 0:
            continue
        vals = sub["lmp"].to_numpy(dtype=float)
        ws = sub["weight"].to_numpy(dtype=float)
        if ws.sum() <= 0:
            continue
        ws = ws / ws.sum()
        forecast_hourly[h] = float(np.average(vals, weights=ws))
        quantiles_hourly[h] = {q: _weighted_quantile(vals, ws, q) for q in quantiles}
        per_hour_dist[h] = (vals, ws)
        n_used.append(len(sub))

    actual_hourly = _actuals_long(pool, target_date)
    output_table = _build_output_table(target_date, forecast_hourly, actual_hourly)

    hour_groups = {
        "OnPeak": list(configs.ONPEAK_HOURS),
        "OffPeak": list(configs.OFFPEAK_HOURS),
        "Flat": list(HOURS),
    }
    aggregate_bands = _aggregate_quantile_bands(per_hour_dist, hour_groups, quantiles)

    q_rows: list[dict] = []
    cols_template = (
        ["Date", "Type"] + [f"HE{h}" for h in HOURS] + ["OnPeak", "OffPeak", "Flat"]
    )
    for q in quantiles:
        row: dict = {"Date": target_date, "Type": _quantile_label(q)}
        for h in HOURS:
            row[f"HE{h}"] = quantiles_hourly.get(h, {}).get(q)
        row = _summarize(row)
        for label in ("OnPeak", "OffPeak", "Flat"):
            row[label] = aggregate_bands.get(label, {}).get(q, row.get(label))
        q_rows.append(row)
    quantiles_table = pd.DataFrame(q_rows, columns=cols_template)

    target_features: dict[int, dict[str, float | None]] = {}
    feature_cols_for_target = [
        "load_mw_at_hour",
        "temp_at_hour",
        "solar_at_hour",
        "wind_at_hour",
        "gas_m3_daily_avg",
        "outage_total_mw",
        "load_ramp_1h_at_hour",
        "load_ramp_3h_at_hour",
        "net_load_at_hour",
    ]
    for h in HOURS:
        q_rows_for_h = query[query["hour_ending"] == h]
        if len(q_rows_for_h) == 0:
            continue
        q_row = q_rows_for_h.iloc[0]
        target_features[h] = {
            c: (float(q_row[c]) if c in q_row.index and pd.notna(q_row[c]) else None)
            for c in feature_cols_for_target
        }

    logger.info(
        "Sunny hourly KNN forecast: target=%s hours=%d avg_analogs=%.1f has_actuals=%s",
        target_date,
        len(forecast_hourly),
        float(np.mean(n_used)) if n_used else 0.0,
        actual_hourly is not None,
    )

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "analogs": analogs,
        "target_features_by_hour": target_features,
        "forecast_date": str(target_date),
        "reference_date": str(target_date - timedelta(days=1)),
        "has_actuals": actual_hourly is not None,
        "n_analogs_used": int(np.mean(n_used)) if n_used else 0,
        "scenario": "hourly_knn_sunny",
        "feature_weights": weights,
        "day_type": day_type,
    }
