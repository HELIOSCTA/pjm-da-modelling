"""Hourly KNN forecast for PJM DA LMPs.

Each delivery hour gets its own analog set: for target (T, h), find K historical
(D, h) pairs with the most similar hour-h conditions, then average their LMPs.

Pool contract:
  - One row per (historical date D, hour_ending h)
  - Hourly columns: load_mw_at_hour, temp_at_hour, solar_at_hour, wind_at_hour
  - Daily columns broadcast across all 24 hours of D: gas_m3_daily_avg,
    outage_total_mw, calendar features
  - Label: lmp (one number per row, the DA LMP for that hour at the hub)

Query contract:
  - 24 rows for target date T
  - Same feature namespace as pool, populated from forecast feeds

Forward-only / leakage-safe: the analog filter restricts to dates < target_date.
"""
from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.calendar import compute_calendar_row
from da_models.common.data import loader
from da_models.hourly_knn import configs

logger = logging.getLogger(__name__)

ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = list(range(1, 8)) + [24]


# ─── Calendar / utility ─────────────────────────────────────────────────────

def _calendar_features_for(d: date) -> dict:
    """Calendar features using Sun=0..Sat=6 convention (same as forward_only_knn)."""
    weekday_mon0 = d.weekday()
    dow_num = (weekday_mon0 + 1) % 7
    base = compute_calendar_row(d)
    return {
        "day_of_week_number": int(dow_num),
        "is_nerc_holiday": int(bool(base.get("is_nerc_holiday", False))),
        "is_weekend": 1 if dow_num in (0, 6) else 0,
        "dow_sin": float(math.sin(2 * math.pi * dow_num / 7.0)),
        "dow_cos": float(math.cos(2 * math.pi * dow_num / 7.0)),
    }


def _safe_load(load_fn, cache_dir):
    try:
        return load_fn(cache_dir=cache_dir)
    except Exception as exc:
        logger.warning("Optional loader %s failed: %s", load_fn.__name__, exc)
        return None


def _circular_doy_distance(doy: np.ndarray, target_doy: int) -> np.ndarray:
    direct = np.abs(doy - float(target_doy))
    return np.minimum(direct, 366.0 - direct)


def _fit_zscore(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Pool-only z-score, NaN-safe. Same shape as forward_only_knn metrics."""
    valid = ~np.isnan(values)
    counts = valid.sum(axis=0).astype(float)
    safe = np.where(valid, values, 0.0)
    means = safe.sum(axis=0) / np.where(counts == 0.0, 1.0, counts)
    means = np.where(counts == 0.0, 0.0, means)
    centered = np.where(valid, values - means, 0.0)
    variances = (centered ** 2).sum(axis=0) / np.where(counts == 0.0, 1.0, counts)
    stds = np.sqrt(variances)
    stds = np.where((stds == 0.0) | (counts == 0.0) | np.isnan(stds), 1.0, stds)
    return means, stds


def _nan_aware_distance(query_z: np.ndarray, pool_z: np.ndarray) -> np.ndarray:
    """Per-row Euclidean distance over dimensions where both are finite."""
    diff = pool_z - query_z[np.newaxis, :]
    valid = ~np.isnan(diff)
    sq = np.where(valid, diff ** 2, 0.0)
    n_valid = valid.sum(axis=1).astype(float)
    out = np.full(len(pool_z), np.nan, dtype=float)
    has_valid = n_valid > 0
    out[has_valid] = np.sqrt(sq[has_valid].sum(axis=1))
    return out


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    idx = np.argsort(values)
    v = values[idx]
    w = weights[idx]
    cdf = np.cumsum(w)
    cdf = cdf / cdf[-1]
    return float(np.interp(q, cdf, v))


# ─── Pool / query builders ──────────────────────────────────────────────────

def build_hourly_pool(cache_dir: Path | None = None) -> pd.DataFrame:
    """Long-format pool: one row per (date, hour_ending) with realized features + LMP label."""
    cache_dir = cache_dir or configs.CACHE_DIR

    df_lmp = loader.load_lmps_da(cache_dir=cache_dir)
    pool = df_lmp[df_lmp["region"] == configs.HUB][["date", "hour_ending", "lmp"]].copy()

    # Hourly load (RT realized, RTO region)
    df_load = _safe_load(loader.load_load_rt, cache_dir)
    if df_load is not None and len(df_load) > 0:
        rt = df_load
        if "region" in rt.columns:
            rt = rt[rt["region"] == configs.LOAD_REGION]
        rt = rt[["date", "hour_ending", "rt_load_mw"]].rename(columns={"rt_load_mw": "load_mw_at_hour"})
        pool = pool.merge(rt, on=["date", "hour_ending"], how="left")
    else:
        pool["load_mw_at_hour"] = np.nan

    # Hourly weather temp (observed, station-averaged)
    df_weather = _safe_load(loader.load_weather_observed_hourly, cache_dir)
    if df_weather is None or len(df_weather) == 0:
        df_weather = _safe_load(loader.load_weather_hourly, cache_dir)
    if df_weather is not None and "temp" in df_weather.columns:
        wx = df_weather[["date", "hour_ending", "temp"]].rename(columns={"temp": "temp_at_hour"})
        pool = pool.merge(wx, on=["date", "hour_ending"], how="left")
    else:
        pool["temp_at_hour"] = np.nan

    # Hourly solar/wind (realized fuel mix)
    df_fuel = _safe_load(loader.load_fuel_mix, cache_dir)
    if df_fuel is not None and "solar" in df_fuel.columns and "wind" in df_fuel.columns:
        fm = df_fuel[["date", "hour_ending", "solar", "wind"]].rename(
            columns={"solar": "solar_at_hour", "wind": "wind_at_hour"},
        )
        pool = pool.merge(fm, on=["date", "hour_ending"], how="left")
    else:
        pool["solar_at_hour"] = np.nan
        pool["wind_at_hour"] = np.nan

    # Daily-level: gas hub price (mean across hubs available for the date)
    df_gas = _safe_load(loader.load_gas_prices_hourly, cache_dir)
    if df_gas is not None and "gas_m3" in df_gas.columns:
        gas_daily = (
            df_gas.groupby("date", as_index=False)["gas_m3"].mean()
            .rename(columns={"gas_m3": "gas_m3_daily_avg"})
        )
        pool = pool.merge(gas_daily, on="date", how="left")
    else:
        pool["gas_m3_daily_avg"] = np.nan

    # Daily-level: realized total outages
    df_outages = _safe_load(loader.load_outages_actual, cache_dir)
    if df_outages is not None and len(df_outages) > 0:
        out = df_outages
        if "region" in out.columns:
            out = out[out["region"] == configs.LOAD_REGION]
        out = out[["date", "total_outages_mw"]].rename(columns={"total_outages_mw": "outage_total_mw"})
        pool = pool.merge(out, on="date", how="left")
    else:
        pool["outage_total_mw"] = np.nan

    # Calendar (per date, broadcast to all hours)
    cal_rows = []
    for d in pool["date"].drop_duplicates():
        row = {"date": d}
        row.update(_calendar_features_for(d))
        cal_rows.append(row)
    cal_df = pd.DataFrame(cal_rows)
    pool = pool.merge(cal_df, on="date", how="left")

    pool["date"] = pd.to_datetime(pool["date"]).dt.date
    pool["hour_ending"] = pool["hour_ending"].astype(int)
    pool = pool.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    logger.info("Hourly pool built: %s rows across %s dates", len(pool), pool["date"].nunique())
    return pool


def build_hourly_query(target_date: date, cache_dir: Path | None = None) -> pd.DataFrame:
    """24 rows for target_date with forecast-side features."""
    cache_dir = cache_dir or configs.CACHE_DIR

    rows = pd.DataFrame({"date": [target_date] * 24, "hour_ending": configs.HOURS})

    # Hourly load forecast (with RT fallback for backtests on past dates)
    df_load_fc = _safe_load(loader.load_load_forecast, cache_dir)
    df_load_rt = _safe_load(loader.load_load_rt, cache_dir)

    load_q = pd.DataFrame(columns=["date", "hour_ending", "load_mw_at_hour"])
    if df_load_fc is not None and len(df_load_fc) > 0:
        lf = df_load_fc
        if "region" in lf.columns:
            lf = lf[lf["region"] == configs.LOAD_REGION]
        lf = lf[lf["date"] == target_date]
        if len(lf) > 0 and "forecast_load_mw" in lf.columns:
            load_q = lf[["date", "hour_ending", "forecast_load_mw"]].rename(
                columns={"forecast_load_mw": "load_mw_at_hour"},
            )
    if len(load_q) == 0 and df_load_rt is not None and len(df_load_rt) > 0:
        rt = df_load_rt
        if "region" in rt.columns:
            rt = rt[rt["region"] == configs.LOAD_REGION]
        rt = rt[rt["date"] == target_date]
        if len(rt) > 0 and "rt_load_mw" in rt.columns:
            load_q = rt[["date", "hour_ending", "rt_load_mw"]].rename(columns={"rt_load_mw": "load_mw_at_hour"})

    rows = rows.merge(load_q, on=["date", "hour_ending"], how="left") if len(load_q) > 0 else rows.assign(load_mw_at_hour=np.nan)

    # Hourly temp forecast (fallback to observed)
    df_weather_fc = _safe_load(loader.load_weather_forecast_hourly, cache_dir)
    if df_weather_fc is None or len(df_weather_fc) == 0:
        df_weather_fc = _safe_load(loader.load_weather_observed_hourly, cache_dir)
    if df_weather_fc is not None and "temp" in df_weather_fc.columns:
        wf = df_weather_fc[df_weather_fc["date"] == target_date][["date", "hour_ending", "temp"]].rename(
            columns={"temp": "temp_at_hour"},
        )
        rows = rows.merge(wf, on=["date", "hour_ending"], how="left") if len(wf) > 0 else rows.assign(temp_at_hour=np.nan)
    else:
        rows["temp_at_hour"] = np.nan

    # Hourly solar/wind forecasts
    df_solar_fc = _safe_load(loader.load_solar_forecast, cache_dir)
    if df_solar_fc is not None and "solar_forecast" in df_solar_fc.columns:
        sf = df_solar_fc[df_solar_fc["date"] == target_date][["date", "hour_ending", "solar_forecast"]].rename(
            columns={"solar_forecast": "solar_at_hour"},
        )
        rows = rows.merge(sf, on=["date", "hour_ending"], how="left") if len(sf) > 0 else rows.assign(solar_at_hour=np.nan)
    else:
        rows["solar_at_hour"] = np.nan

    df_wind_fc = _safe_load(loader.load_wind_forecast, cache_dir)
    if df_wind_fc is not None and "wind_forecast" in df_wind_fc.columns:
        wfc = df_wind_fc[df_wind_fc["date"] == target_date][["date", "hour_ending", "wind_forecast"]].rename(
            columns={"wind_forecast": "wind_at_hour"},
        )
        rows = rows.merge(wfc, on=["date", "hour_ending"], how="left") if len(wfc) > 0 else rows.assign(wind_at_hour=np.nan)
    else:
        rows["wind_at_hour"] = np.nan

    # Daily gas (broadcast across hours)
    df_gas = _safe_load(loader.load_gas_prices_hourly, cache_dir)
    if df_gas is not None and "gas_m3" in df_gas.columns:
        gf = df_gas[df_gas["date"] == target_date]
        rows["gas_m3_daily_avg"] = float(gf["gas_m3"].mean()) if len(gf) > 0 else np.nan
    else:
        rows["gas_m3_daily_avg"] = np.nan

    # Daily outage forecast (latest execution for target_date)
    df_outages_fc = _safe_load(loader.load_outages_forecast, cache_dir)
    out_val = np.nan
    if df_outages_fc is not None and len(df_outages_fc) > 0:
        od = df_outages_fc
        if "region" in od.columns:
            od = od[od["region"] == configs.LOAD_REGION]
        od = od[od["date"] == target_date]
        if "forecast_execution_date" in od.columns:
            od = od.sort_values("forecast_execution_date", ascending=False)
        if len(od) > 0:
            v = pd.to_numeric(od.iloc[0].get("total_outages_mw"), errors="coerce")
            if pd.notna(v):
                out_val = float(v)
    rows["outage_total_mw"] = out_val

    # Calendar (broadcast)
    cal = _calendar_features_for(target_date)
    for k, v in cal.items():
        rows[k] = v

    rows["date"] = pd.to_datetime(rows["date"]).dt.date
    rows["hour_ending"] = rows["hour_ending"].astype(int)
    return rows.sort_values("hour_ending").reset_index(drop=True)


# ─── Per-hour analog selection ──────────────────────────────────────────────

def _select_analogs_for_hour(
    pool: pd.DataFrame,
    query_row: pd.Series,
    target_date: date,
    hour: int,
    cfg: configs.HourlyKNNConfig,
    weights: dict[str, float],
) -> pd.DataFrame:
    """Per-hour KNN: same hour, season window, DOW filter, then weighted distance."""
    work = pool[(pool["hour_ending"] == hour) & (pool["date"] < target_date)].copy()

    # Season window (circular day-of-year)
    if cfg.season_window_days > 0 and len(work) > 0:
        target_doy = pd.Timestamp(target_date).dayofyear
        doy = pd.to_datetime(work["date"]).dt.dayofyear.to_numpy(dtype=float)
        keep = _circular_doy_distance(doy, target_doy) <= float(cfg.season_window_days)
        work = work[keep]

    # Calendar filter ladder (mirrors forward_only_knn approach)
    target_dow = int(query_row.get("day_of_week_number", 0))
    target_holiday = int(query_row.get("is_nerc_holiday", 0))

    candidates: list[tuple[str, pd.DataFrame]] = []
    if cfg.same_dow_group:
        exact_dow = work["day_of_week_number"] == target_dow
        if cfg.exclude_holidays:
            holiday_mask = (work["is_nerc_holiday"] == 1) if target_holiday else (work["is_nerc_holiday"] != 1)
            candidates.append(("exact_dow+holiday", work[exact_dow & holiday_mask]))
        candidates.append(("exact_dow_only", work[exact_dow]))
    candidates.append(("no_filter", work))

    chosen = work
    for stage, frame in candidates:
        if len(frame) >= cfg.min_pool_size:
            chosen = frame
            break

    if len(chosen) == 0:
        return chosen.assign(distance=np.nan, weight=np.nan)

    # Compute per-group distance
    groups = configs.FEATURE_GROUPS
    n = len(chosen)
    weighted_sum = np.zeros(n, dtype=float)
    weight_sum = np.zeros(n, dtype=float)

    for group_name, cols in groups.items():
        w = float(weights.get(group_name, 0.0))
        if w <= 0:
            continue
        present = [c for c in cols if c in chosen.columns and c in query_row.index]
        if not present:
            continue
        pool_vals = chosen[present].to_numpy(dtype=float)
        query_vals = np.asarray([query_row[c] for c in present], dtype=float)
        means, stds = _fit_zscore(pool_vals)
        pool_z = (pool_vals - means) / stds
        query_z = (query_vals - means) / stds
        d = _nan_aware_distance(query_z, pool_z)
        finite = np.isfinite(d)
        weighted_sum[finite] += w * d[finite]
        weight_sum[finite] += w

    distances = np.full(n, np.inf, dtype=float)
    valid = weight_sum > 0
    distances[valid] = weighted_sum[valid] / weight_sum[valid]

    # Recency penalty (linear ageing, same convention as forward_only_knn)
    age = (pd.to_datetime(target_date) - pd.to_datetime(chosen["date"])).dt.days.to_numpy(dtype=float)
    age = np.maximum(age, 0.0)
    distances = distances * (1.0 + age / float(max(cfg.recency_half_life_days, 1)))

    chosen = chosen.copy()
    chosen["distance"] = distances
    chosen = chosen[np.isfinite(chosen["distance"])].sort_values(["distance", "date"]).head(cfg.n_analogs).copy()
    if len(chosen) == 0:
        return chosen.assign(weight=np.nan)

    d = chosen["distance"].to_numpy(dtype=float)
    inv = 1.0 / np.square(np.maximum(d, 1e-8))
    chosen["weight"] = inv / inv.sum()
    return chosen


# ─── Output table builder ───────────────────────────────────────────────────

def _summarize(row: dict) -> dict:
    on = [row.get(f"HE{h}") for h in ONPEAK_HOURS]
    off = [row.get(f"HE{h}") for h in OFFPEAK_HOURS]
    flat = [row.get(f"HE{h}") for h in configs.HOURS]
    on = [v for v in on if v is not None and not pd.isna(v)]
    off = [v for v in off if v is not None and not pd.isna(v)]
    flat = [v for v in flat if v is not None and not pd.isna(v)]
    row["OnPeak"] = float(np.mean(on)) if on else np.nan
    row["OffPeak"] = float(np.mean(off)) if off else np.nan
    row["Flat"] = float(np.mean(flat)) if flat else np.nan
    return row


def _build_output_table(target_date, forecast_hourly, actual_hourly):
    rows: list[dict] = []
    if actual_hourly:
        rows.append(_summarize({"Date": target_date, "Type": "Actual", **{f"HE{h}": actual_hourly.get(h) for h in configs.HOURS}}))
    rows.append(_summarize({"Date": target_date, "Type": "Forecast", **{f"HE{h}": forecast_hourly.get(h) for h in configs.HOURS}}))
    if actual_hourly:
        err = {}
        for h in configs.HOURS:
            f = forecast_hourly.get(h)
            a = actual_hourly.get(h)
            err[f"HE{h}"] = (f - a) if (f is not None and a is not None and not pd.isna(f) and not pd.isna(a)) else None
        rows.append(_summarize({"Date": target_date, "Type": "Error", **err}))
    cols = ["Date", "Type"] + [f"HE{h}" for h in configs.HOURS] + ["OnPeak", "OffPeak", "Flat"]
    return pd.DataFrame(rows, columns=cols)


def _quantile_label(q: float) -> str:
    pct = q * 100.0
    if float(pct).is_integer():
        return f"P{int(pct):02d}"
    return f"P{pct:.1f}".rstrip("0").rstrip(".")


# ─── Main entrypoint ────────────────────────────────────────────────────────

def run_forecast(
    target_date: date | None = None,
    config: configs.HourlyKNNConfig | None = None,
    cache_dir: Path | None = None,
    pool: pd.DataFrame | None = None,
) -> dict:
    """Run hourly KNN for one delivery date.

    `pool` may be passed pre-built — avoids re-loading parquets when running
    many target dates back-to-back (used by the backtest and weight optimizer).
    """
    cfg = config or configs.HourlyKNNConfig()
    target_date = target_date or cfg.resolved_target_date()
    target_date = pd.to_datetime(target_date).date()

    if pool is None:
        pool = build_hourly_pool(cache_dir=cache_dir)
    query = build_hourly_query(target_date, cache_dir=cache_dir)
    weights = cfg.resolved_weights()

    forecast_hourly: dict[int, float] = {}
    quantiles_hourly: dict[int, dict[float, float]] = {}
    all_analogs: list[pd.DataFrame] = []
    n_used: list[int] = []

    for h in configs.HOURS:
        q_rows = query[query["hour_ending"] == h]
        if len(q_rows) == 0:
            continue
        q_row = q_rows.iloc[0]
        analogs = _select_analogs_for_hour(pool, q_row, target_date, h, cfg, weights)
        if len(analogs) == 0:
            continue
        valid = analogs.dropna(subset=["lmp"]).copy()
        if len(valid) == 0:
            continue

        ws = valid["weight"].to_numpy(dtype=float)
        ws = ws / ws.sum()
        vals = valid["lmp"].to_numpy(dtype=float)
        forecast_hourly[h] = float(np.average(vals, weights=ws))
        quantiles_hourly[h] = {q: _weighted_quantile(vals, ws, q) for q in cfg.quantiles}
        n_used.append(len(valid))

        keep = valid[["date", "hour_ending", "distance", "weight", "lmp"]].copy()
        keep["target_hour"] = h
        all_analogs.append(keep)

    # Actuals (if target_date is in the pool)
    actual_hourly: dict[int, float] | None = None
    actual_rows = pool[pool["date"] == target_date]
    if len(actual_rows) > 0:
        ah = {int(r["hour_ending"]): float(r["lmp"]) for _, r in actual_rows.iterrows() if pd.notna(r["lmp"])}
        if len(ah) >= 12:
            actual_hourly = ah

    output_table = _build_output_table(target_date, forecast_hourly, actual_hourly)

    q_rows = []
    cols_template = ["Date", "Type"] + [f"HE{h}" for h in configs.HOURS] + ["OnPeak", "OffPeak", "Flat"]
    for q in cfg.quantiles:
        row = {"Date": target_date, "Type": _quantile_label(q)}
        for h in configs.HOURS:
            row[f"HE{h}"] = quantiles_hourly.get(h, {}).get(q)
        q_rows.append(_summarize(row))
    quantiles_table = pd.DataFrame(q_rows, columns=cols_template)

    logger.info(
        "Hourly KNN forecast complete: target=%s hours_filled=%s avg_analogs=%.1f has_actuals=%s",
        target_date,
        len(forecast_hourly),
        float(np.mean(n_used)) if n_used else 0.0,
        actual_hourly is not None,
    )

    return {
        "output_table": output_table,
        "quantiles_table": quantiles_table,
        "analogs": pd.concat(all_analogs, ignore_index=True) if all_analogs else pd.DataFrame(),
        "metrics": None,
        "forecast_date": str(target_date),
        "reference_date": str(target_date - timedelta(days=1)),
        "has_actuals": actual_hourly is not None,
        "n_analogs_used": int(np.mean(n_used)) if n_used else 0,
        "scenario": "hourly_knn",
        "feature_weights": weights,
    }


def run(*args, **kwargs) -> dict:
    """Backward-compatible alias to mirror forward_only_knn."""
    return run_forecast(*args, **kwargs)


if __name__ == "__main__":
    result = run_forecast()
    print(result["output_table"])
