"""Feature builder for forward-only KNN.

Pool contract:
  - One row per historical delivery date D
  - Realized conditions in feature columns
  - DA labels in columns lmp_h1..lmp_h24

Query contract:
  - One row for target delivery date T
  - Same feature namespace as pool (no label columns)
"""
from __future__ import annotations

import logging
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.calendar import compute_calendar_row
from da_models.common.data import loader
from da_models.forward_only_knn import configs

logger = logging.getLogger(__name__)

_FILTER_COLS = ["day_of_week_number", "dow_group", "is_nerc_holiday"]


def _load_daily_aggregates(
    df_hourly: pd.DataFrame,
    value_col: str,
    prefix: str = "load",
) -> pd.DataFrame:
    """Compute level and ramp features from hourly data for one region.

    The prefix (default ``"load"``) controls output column names so the same
    aggregator can produce ``load_*`` or ``net_load_*`` daily features.
    """
    if df_hourly is None or len(df_hourly) == 0:
        return pd.DataFrame(columns=["date"])

    df = df_hourly.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce")
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=["date", "hour_ending", value_col])
    if len(df) == 0:
        return pd.DataFrame(columns=["date"])

    df["hour_ending"] = df["hour_ending"].astype(int)
    df = df.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    daily = (
        df.groupby("date")
        .agg(
            **{
                f"{prefix}_daily_avg": (value_col, "mean"),
                f"{prefix}_daily_peak": (value_col, "max"),
                f"{prefix}_daily_valley": (value_col, "min"),
            }
        )
        .reset_index()
    )

    df["ramp"] = df.groupby("date")[value_col].diff()
    daily = daily.merge(
        df.groupby("date", as_index=False)["ramp"]
        .max()
        .rename(columns={"ramp": f"{prefix}_ramp_max"}),
        on="date",
        how="left",
    )

    he5 = df[df["hour_ending"] == 5][["date", value_col]].rename(columns={value_col: "he5"})
    he8 = df[df["hour_ending"] == 8][["date", value_col]].rename(columns={value_col: "he8"})
    he15 = df[df["hour_ending"] == 15][["date", value_col]].rename(columns={value_col: "he15"})
    he20 = df[df["hour_ending"] == 20][["date", value_col]].rename(columns={value_col: "he20"})

    daily = daily.merge(he5, on="date", how="left")
    daily = daily.merge(he8, on="date", how="left")
    daily = daily.merge(he15, on="date", how="left")
    daily = daily.merge(he20, on="date", how="left")
    daily[f"{prefix}_morning_ramp"] = daily["he8"] - daily["he5"]
    daily[f"{prefix}_evening_ramp"] = daily["he20"] - daily["he15"]
    return daily.drop(columns=["he5", "he8", "he15", "he20"])


def _build_net_load_features_pool(
    df_rt_load: pd.DataFrame | None,
    df_fuel_mix: pd.DataFrame | None,
) -> pd.DataFrame:
    """Pool-side net-load features from realized RTO load minus realized solar+wind.

    Mirrors the renewables pool pattern: pool uses realized values, query uses
    forecasts. Output columns: net_load_daily_avg / peak / valley / *_morning_ramp /
    *_evening_ramp.
    """
    if df_rt_load is None or len(df_rt_load) == 0:
        return pd.DataFrame(columns=["date"])
    if df_fuel_mix is None or len(df_fuel_mix) == 0:
        return pd.DataFrame(columns=["date"])
    if "solar" not in df_fuel_mix.columns or "wind" not in df_fuel_mix.columns:
        return pd.DataFrame(columns=["date"])

    rt = df_rt_load.copy()
    if "region" in rt.columns:
        rt = rt[rt["region"] == configs.LOAD_REGION]
    if len(rt) == 0 or "rt_load_mw" not in rt.columns:
        return pd.DataFrame(columns=["date"])

    rt = rt[["date", "hour_ending", "rt_load_mw"]].copy()
    fm = df_fuel_mix[["date", "hour_ending", "solar", "wind"]].copy()

    rt["date"] = pd.to_datetime(rt["date"]).dt.date
    fm["date"] = pd.to_datetime(fm["date"]).dt.date

    merged = rt.merge(fm, on=["date", "hour_ending"], how="inner")
    if len(merged) == 0:
        return pd.DataFrame(columns=["date"])

    merged["rt_load_mw"] = pd.to_numeric(merged["rt_load_mw"], errors="coerce")
    merged["solar"] = pd.to_numeric(merged["solar"], errors="coerce").fillna(0.0)
    merged["wind"] = pd.to_numeric(merged["wind"], errors="coerce").fillna(0.0)
    merged["net_load"] = merged["rt_load_mw"] - merged["solar"] - merged["wind"]
    merged = merged.dropna(subset=["net_load"])

    return _load_daily_aggregates(
        merged[["date", "hour_ending", "net_load"]],
        value_col="net_load",
        prefix="net_load",
    )


def _build_net_load_features_query(
    df_net_load_forecast: pd.DataFrame | None,
    target_date: date,
) -> pd.DataFrame:
    """Query-side net-load features from the forward net-load forecast parquet."""
    if df_net_load_forecast is None or len(df_net_load_forecast) == 0:
        return pd.DataFrame(columns=["date"])
    if "net_load_forecast_mw" not in df_net_load_forecast.columns:
        return pd.DataFrame(columns=["date"])

    df = df_net_load_forecast.copy()
    if "region" in df.columns:
        df = df[df["region"] == configs.LOAD_REGION]
    if len(df) == 0:
        return pd.DataFrame(columns=["date"])

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] == target_date]
    if len(df) == 0:
        return pd.DataFrame(columns=["date"])

    sub = df[["date", "hour_ending", "net_load_forecast_mw"]].rename(
        columns={"net_load_forecast_mw": "net_load"},
    )
    return _load_daily_aggregates(sub, value_col="net_load", prefix="net_load")


def _build_outage_features_pool(df_outages_actual: pd.DataFrame | None) -> pd.DataFrame:
    """Build outage features from realized daily outages."""
    if df_outages_actual is None or len(df_outages_actual) == 0:
        return pd.DataFrame(columns=["date"])
    df = df_outages_actual.copy()
    if "region" in df.columns:
        df = df[df["region"] == configs.LOAD_REGION]
    if len(df) == 0:
        return pd.DataFrame(columns=["date"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    total = pd.to_numeric(df.get("total_outages_mw"), errors="coerce")
    forced = pd.to_numeric(df.get("forced_outages_mw"), errors="coerce")
    out = pd.DataFrame(
        {
            "date": df["date"],
            "outage_total_mw": total,
            "outage_forced_mw": forced,
        }
    )
    out["outage_forced_share"] = out["outage_forced_mw"] / out["outage_total_mw"].replace(0, np.nan)
    return out.sort_values("date").reset_index(drop=True)


def _build_outage_features_query(
    df_outages_forecast: pd.DataFrame | None,
    target_date: date,
) -> dict[str, float]:
    """Latest outage forecast row for target_date."""
    out: dict[str, float] = {}
    if df_outages_forecast is None or len(df_outages_forecast) == 0:
        return out
    df = df_outages_forecast.copy()
    if "region" in df.columns:
        df = df[df["region"] == configs.LOAD_REGION]
    df = df[df["date"] == target_date]
    if len(df) == 0:
        return out
    if "forecast_execution_date" in df.columns:
        df = df.sort_values("forecast_execution_date", ascending=False)
    latest = df.iloc[0]
    total = pd.to_numeric(latest.get("total_outages_mw"), errors="coerce")
    forced = pd.to_numeric(latest.get("forced_outages_mw"), errors="coerce")
    out["outage_total_mw"] = float(total) if pd.notna(total) else np.nan
    out["outage_forced_mw"] = float(forced) if pd.notna(forced) else np.nan
    if pd.notna(total) and float(total) != 0 and pd.notna(forced):
        out["outage_forced_share"] = float(forced) / float(total)
    else:
        out["outage_forced_share"] = np.nan
    return out


def _build_renewable_features_pool(df_fuel_mix: pd.DataFrame | None) -> pd.DataFrame:
    """Daily renewable aggregates from realized fuel-mix hourly data."""
    if df_fuel_mix is None or len(df_fuel_mix) == 0:
        return pd.DataFrame(columns=["date"])
    if "solar" not in df_fuel_mix.columns or "wind" not in df_fuel_mix.columns:
        return pd.DataFrame(columns=["date"])

    df = df_fuel_mix[["date", "hour_ending", "solar", "wind"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["solar"] = pd.to_numeric(df["solar"], errors="coerce").fillna(0.0)
    df["wind"] = pd.to_numeric(df["wind"], errors="coerce").fillna(0.0)
    daily = (
        df.groupby("date")
        .agg(solar_daily_avg=("solar", "mean"), wind_daily_avg=("wind", "mean"))
        .reset_index()
    )
    daily["renewable_daily_avg"] = daily["solar_daily_avg"] + daily["wind_daily_avg"]
    return daily


def _build_renewable_features_query(
    df_solar_forecast: pd.DataFrame | None,
    df_wind_forecast: pd.DataFrame | None,
    target_date: date,
) -> dict[str, float]:
    """Daily renewable aggregates from PJM solar/wind forecasts for target_date."""
    out: dict[str, float] = {}

    solar_avg = np.nan
    if df_solar_forecast is not None and len(df_solar_forecast) > 0:
        sf = df_solar_forecast[df_solar_forecast["date"] == target_date]
        if len(sf) > 0 and "solar_forecast" in sf.columns:
            vals = pd.to_numeric(sf["solar_forecast"], errors="coerce").fillna(0.0)
            if len(vals) > 0:
                solar_avg = float(vals.mean())

    wind_avg = np.nan
    if df_wind_forecast is not None and len(df_wind_forecast) > 0:
        wf = df_wind_forecast[df_wind_forecast["date"] == target_date]
        if len(wf) > 0 and "wind_forecast" in wf.columns:
            vals = pd.to_numeric(wf["wind_forecast"], errors="coerce").fillna(0.0)
            if len(vals) > 0:
                wind_avg = float(vals.mean())

    out["solar_daily_avg"] = solar_avg
    out["wind_daily_avg"] = wind_avg
    if pd.notna(solar_avg) and pd.notna(wind_avg):
        out["renewable_daily_avg"] = float(solar_avg) + float(wind_avg)
    elif pd.notna(solar_avg):
        out["renewable_daily_avg"] = float(solar_avg)
    elif pd.notna(wind_avg):
        out["renewable_daily_avg"] = float(wind_avg)
    else:
        out["renewable_daily_avg"] = np.nan
    return out


def _build_weather_features(
    df_weather: pd.DataFrame | None,
    *,
    hdd_base: float = 65.0,
    cdd_base: float = 65.0,
) -> pd.DataFrame:
    """Build daily weather features used by weather-level groups."""
    if df_weather is None or len(df_weather) == 0:
        return pd.DataFrame(columns=["date"])
    if "temp" not in df_weather.columns:
        return pd.DataFrame(columns=["date"])

    df = df_weather.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce")
    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df = df.dropna(subset=["date", "hour_ending", "temp"])
    if len(df) == 0:
        return pd.DataFrame(columns=["date"])

    agg_dict: dict[str, tuple[str, str]] = {
        "temp_daily_avg": ("temp", "mean"),
        "temp_daily_max": ("temp", "max"),
        "temp_daily_min": ("temp", "min"),
    }

    optional_aggs = (
        ("feels_like_temp", "feels_like_daily_avg"),
        ("dew_point_temp", "dew_point_daily_avg"),
        ("wind_speed_mph", "wind_speed_daily_avg"),
        ("relative_humidity", "humidity_daily_avg"),
        ("cloud_cover_pct", "cloud_cover_daily_avg"),
    )
    for source_col, target_col in optional_aggs:
        if source_col in df.columns:
            agg_dict[target_col] = (source_col, "mean")

    daily = df.groupby("date").agg(**agg_dict).reset_index()
    daily["temp_intraday_range"] = daily["temp_daily_max"] - daily["temp_daily_min"]
    daily["hdd"] = np.maximum(0, hdd_base - daily["temp_daily_avg"])
    daily["cdd"] = np.maximum(0, daily["temp_daily_avg"] - cdd_base)
    daily["temp_7d_rolling_mean"] = daily["temp_daily_avg"].rolling(7, min_periods=1).mean()
    daily["temp_daily_change"] = daily["temp_daily_avg"].diff()
    return daily


def _build_gas_features(df_gas_hourly: pd.DataFrame | None) -> pd.DataFrame:
    """Build daily gas features used by gas-level groups."""
    if df_gas_hourly is None or len(df_gas_hourly) == 0:
        return pd.DataFrame(columns=["date"])

    df = df_gas_hourly.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce")
    df = df.dropna(subset=["date", "hour_ending"])
    if len(df) == 0:
        return pd.DataFrame(columns=["date"])

    hubs = ("gas_m3", "gas_tco", "gas_tz6", "gas_dom_south")
    available_hubs = [hub for hub in hubs if hub in df.columns]
    if not available_hubs:
        return pd.DataFrame(columns=["date"])

    for hub in available_hubs:
        df[hub] = pd.to_numeric(df[hub], errors="coerce")

    agg_dict = {f"{hub}_daily_avg": (hub, "mean") for hub in available_hubs}
    daily = df.groupby("date").agg(**agg_dict).reset_index()
    return daily


def _build_lmp_labels(df_lmp_da: pd.DataFrame, hub: str) -> pd.DataFrame:
    """Build lmp_h1..lmp_h24 labels for one hub."""
    if df_lmp_da is None or len(df_lmp_da) == 0:
        return pd.DataFrame(columns=["date"] + configs.LMP_LABEL_COLUMNS)

    df = df_lmp_da[df_lmp_da["region"] == hub].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce")
    df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")
    df = df.dropna(subset=["date", "hour_ending"])
    if len(df) == 0:
        return pd.DataFrame(columns=["date"] + configs.LMP_LABEL_COLUMNS)

    df["hour_ending"] = df["hour_ending"].astype(int)
    pivot = (
        df.pivot_table(
            index="date",
            columns="hour_ending",
            values="lmp",
            aggfunc="mean",
        )
        .reindex(columns=configs.HOURS)
        .rename(columns={h: f"lmp_h{h}" for h in configs.HOURS})
        .reset_index()
    )
    return pivot


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Ensure all columns exist; create missing with NaN."""
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = np.nan
    return out


def _dow_group_from_num(dow_num: int) -> int:
    for group_idx, (_, days) in enumerate(configs.DOW_GROUPS.items()):
        if dow_num in days:
            return group_idx
    return 0


def _calendar_for_date(value: date) -> dict[str, int | float]:
    """Build filter + cyclical calendar features with Sun=0..Sat=6 convention."""
    weekday_mon0 = value.weekday()
    dow_num = (weekday_mon0 + 1) % 7
    base = compute_calendar_row(value)
    return {
        "day_of_week_number": int(dow_num),
        "dow_group": int(_dow_group_from_num(dow_num)),
        "is_nerc_holiday": int(bool(base.get("is_nerc_holiday", False))),
        "is_weekend": 1 if dow_num in (0, 6) else 0,
        "dow_sin": float(math.sin(2 * math.pi * dow_num / 7.0)),
        "dow_cos": float(math.cos(2 * math.pi * dow_num / 7.0)),
    }


def _safe_load(load_fn, cache_dir: Path | None) -> pd.DataFrame | None:
    try:
        return load_fn(cache_dir=cache_dir)
    except Exception as exc:
        logger.warning("Optional loader failed for %s: %s", load_fn.__name__, exc)
        return None


def build_pool(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> pd.DataFrame:
    """Build historical pool with realized features and same-day DA labels."""
    _ = (schema, cache_enabled, cache_ttl_hours, force_refresh)

    logger.info("Building forward-only KNN pool: schema=%s hub=%s", schema, hub)

    df_lmp_da = loader.load_lmps_da(cache_dir=cache_dir)
    df_rt_load = _safe_load(loader.load_load_rt, cache_dir)
    df_weather = _safe_load(loader.load_weather_observed_hourly, cache_dir)
    if df_weather is None or len(df_weather) == 0:
        df_weather = _safe_load(loader.load_weather_hourly, cache_dir)
    df_gas = _safe_load(loader.load_gas_prices_hourly, cache_dir)
    df_outages_actual = _safe_load(loader.load_outages_actual, cache_dir)
    df_fuel_mix = _safe_load(loader.load_fuel_mix, cache_dir)

    # Base feature blocks
    df_labels = _build_lmp_labels(df_lmp_da, hub)

    df_load = pd.DataFrame(columns=["date"])
    if df_rt_load is not None and len(df_rt_load) > 0:
        rt = df_rt_load.copy()
        if "region" in rt.columns:
            rt = rt[rt["region"] == configs.LOAD_REGION]
        if len(rt) > 0:
            rt = rt[["date", "hour_ending", "rt_load_mw"]].rename(columns={"rt_load_mw": "value"})
            df_load = _load_daily_aggregates(rt, value_col="value")

    df_outages = _build_outage_features_pool(df_outages_actual)
    df_renewables = _build_renewable_features_pool(df_fuel_mix)
    df_net_load = _build_net_load_features_pool(df_rt_load, df_fuel_mix)
    df_weather_daily = _build_weather_features(df_weather)
    df_gas_daily = _build_gas_features(df_gas)

    if len(df_labels) > 0:
        cal_rows = []
        for d in pd.to_datetime(df_labels["date"]).dt.date.tolist():
            row = {"date": d}
            row.update(_calendar_for_date(d))
            cal_rows.append(row)
        df_cal = pd.DataFrame(cal_rows)
    else:
        df_cal = pd.DataFrame(columns=["date"])

    # Merge into one pool matrix
    pool = df_labels.copy()
    for part in (
        df_load,
        df_weather_daily,
        df_gas_daily,
        df_outages,
        df_renewables,
        df_net_load,
        df_cal,
    ):
        if part is not None and len(part) > 0:
            pool = pool.merge(part, on="date", how="left")

    feature_cols = configs.resolved_feature_columns(configs.FEATURE_GROUP_WEIGHTS)
    keep_cols = ["date"] + _FILTER_COLS + feature_cols + configs.LMP_LABEL_COLUMNS
    pool = _ensure_columns(pool, keep_cols)[keep_cols]

    pool["date"] = pd.to_datetime(pool["date"]).dt.date
    pool = pool.sort_values("date").reset_index(drop=True)
    logger.info("Pool built: %s rows, %s feature columns", len(pool), len(feature_cols))
    return pool


def build_query_row(
    target_date: date,
    schema: str = configs.SCHEMA,
    include_gas: bool = True,
    include_outages: bool = True,
    include_renewables: bool = True,
    include_net_load: bool = True,
    cache_dir: Path | None = configs.CACHE_DIR,
    cache_enabled: bool = configs.CACHE_ENABLED,
    cache_ttl_hours: float = configs.CACHE_TTL_HOURS,
    force_refresh: bool = configs.FORCE_CACHE_REFRESH,
) -> pd.Series:
    """Build forward-looking query feature row for one target delivery date."""
    _ = (schema, include_gas, cache_enabled, cache_ttl_hours, force_refresh)

    df_load_forecast = _safe_load(loader.load_load_forecast, cache_dir)
    df_rt_load = _safe_load(loader.load_load_rt, cache_dir)
    df_weather = _safe_load(loader.load_weather_forecast_hourly, cache_dir)
    if df_weather is None or len(df_weather) == 0:
        # Fallback for days where forecast feed is absent but observed cache exists.
        df_weather = _safe_load(loader.load_weather_observed_hourly, cache_dir)

    cal = _calendar_for_date(target_date)

    # Query load from forecast first, fallback to realized if forecast absent.
    load_query = pd.DataFrame(columns=["date", "hour_ending", "value"])
    if df_load_forecast is not None and len(df_load_forecast) > 0:
        lf = df_load_forecast.copy()
        if "region" in lf.columns:
            lf = lf[lf["region"] == configs.LOAD_REGION]
        lf = lf[lf["date"] == target_date]
        if len(lf) > 0 and "forecast_load_mw" in lf.columns:
            load_query = lf[["date", "hour_ending", "forecast_load_mw"]].rename(
                columns={"forecast_load_mw": "value"},
            )

    if len(load_query) == 0 and df_rt_load is not None and len(df_rt_load) > 0:
        lf = df_rt_load.copy()
        if "region" in lf.columns:
            lf = lf[lf["region"] == configs.LOAD_REGION]
        lf = lf[lf["date"] == target_date]
        if len(lf) > 0 and "rt_load_mw" in lf.columns:
            load_query = lf[["date", "hour_ending", "rt_load_mw"]].rename(columns={"rt_load_mw": "value"})

    df_load_q = _load_daily_aggregates(load_query, value_col="value")

    df_weather_q = pd.DataFrame(columns=["date"])
    if df_weather is not None and len(df_weather) > 0:
        wf = df_weather[pd.to_datetime(df_weather["date"]).dt.date == target_date].copy()
        if len(wf) > 0:
            df_weather_q = _build_weather_features(wf)

    df_gas_q = pd.DataFrame(columns=["date"])
    if include_gas:
        df_gas = _safe_load(loader.load_gas_prices_hourly, cache_dir)
        if df_gas is not None and len(df_gas) > 0:
            gf = df_gas[pd.to_datetime(df_gas["date"]).dt.date == target_date].copy()
            if len(gf) > 0:
                df_gas_q = _build_gas_features(gf)

    # Query outages from forecast source for target_date.
    outage_vals: dict[str, float] = {}
    if include_outages:
        df_outage_forecast = _safe_load(loader.load_outages_forecast, cache_dir)
        outage_vals = _build_outage_features_query(df_outage_forecast, target_date)

    # Query renewables from PJM solar/wind forecasts for target_date.
    renewable_vals: dict[str, float] = {}
    if include_renewables:
        df_solar = _safe_load(loader.load_solar_forecast, cache_dir)
        df_wind = _safe_load(loader.load_wind_forecast, cache_dir)
        renewable_vals = _build_renewable_features_query(df_solar, df_wind, target_date)

    # Query net load from forward net-load forecast for target_date.
    df_net_load_q = pd.DataFrame(columns=["date"])
    if include_net_load:
        df_net_load_forecast = _safe_load(loader.load_net_load_forecast, cache_dir)
        df_net_load_q = _build_net_load_features_query(df_net_load_forecast, target_date)

    row_df = pd.DataFrame({"date": [target_date]})
    for part in (df_load_q, df_weather_q, df_gas_q, df_net_load_q):
        if part is not None and len(part) > 0:
            row_df = row_df.merge(part, on="date", how="left")

    for col, value in {**outage_vals, **renewable_vals}.items():
        row_df[col] = value

    for col, value in cal.items():
        row_df[col] = value

    feature_cols = configs.resolved_feature_columns(configs.FEATURE_GROUP_WEIGHTS)
    keep_cols = ["date"] + _FILTER_COLS + feature_cols
    row_df = _ensure_columns(row_df, keep_cols)[keep_cols]
    row_df["date"] = pd.to_datetime(row_df["date"]).dt.date

    query = row_df.iloc[0].copy()
    logger.info(
        "Query row built for %s (include_gas=%s, non-null features=%s/%s)",
        target_date,
        include_gas,
        int(pd.Series(query[feature_cols]).notna().sum()),
        len(feature_cols),
    )
    return query
