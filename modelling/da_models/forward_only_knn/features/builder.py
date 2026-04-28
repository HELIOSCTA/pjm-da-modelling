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


def _per_region_daily_aggregates(
    df_hourly: pd.DataFrame | None,
    value_col: str,
    metric_prefix: str,
    regions: list[str],
) -> pd.DataFrame:
    """Run _load_daily_aggregates per region; suffix output columns with _<region>."""
    if df_hourly is None or len(df_hourly) == 0 or "region" not in df_hourly.columns:
        return pd.DataFrame(columns=["date"])
    if value_col not in df_hourly.columns:
        return pd.DataFrame(columns=["date"])

    out: pd.DataFrame | None = None
    for region in regions:
        sub = df_hourly[df_hourly["region"].astype(str) == region]
        if len(sub) == 0:
            continue
        daily = _load_daily_aggregates(
            sub[["date", "hour_ending", value_col]],
            value_col=value_col,
            prefix=metric_prefix,
        )
        if len(daily) == 0:
            continue
        suffix = f"_{region.lower()}"
        rename_map = {c: f"{c}{suffix}" for c in daily.columns if c != "date"}
        daily = daily.rename(columns=rename_map)
        out = daily if out is None else out.merge(daily, on="date", how="outer")
    return out if out is not None else pd.DataFrame(columns=["date"])


def _per_region_daily_avg(
    df_hourly: pd.DataFrame | None,
    value_col: str,
    metric_name: str,
    regions: list[str],
) -> pd.DataFrame:
    """Daily mean of value_col per region; output column metric_name_daily_avg_<region>."""
    if df_hourly is None or len(df_hourly) == 0 or "region" not in df_hourly.columns:
        return pd.DataFrame(columns=["date"])
    if value_col not in df_hourly.columns:
        return pd.DataFrame(columns=["date"])

    df = df_hourly.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])

    out: pd.DataFrame | None = None
    for region in regions:
        sub = df[df["region"].astype(str) == region]
        if len(sub) == 0:
            continue
        daily = (
            sub.groupby("date", as_index=False)[value_col]
            .mean()
            .rename(columns={value_col: f"{metric_name}_daily_avg_{region.lower()}"})
        )
        out = daily if out is None else out.merge(daily, on="date", how="outer")
    return out if out is not None else pd.DataFrame(columns=["date"])


def _build_load_features_pool(
    df_rt_load: pd.DataFrame | None,
    regions: list[str],
) -> pd.DataFrame:
    """Per-region load level + ramp features from PJM RT load actuals."""
    if df_rt_load is None or len(df_rt_load) == 0 or "region" not in df_rt_load.columns:
        return pd.DataFrame(columns=["date"])
    df = df_rt_load.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["rt_load_mw"] = pd.to_numeric(df["rt_load_mw"], errors="coerce")
    df = df.dropna(subset=["rt_load_mw"])
    return _per_region_daily_aggregates(df, "rt_load_mw", "load", regions)


def _build_net_load_features_pool(
    df_net_load_actual: pd.DataFrame | None,
    regions: list[str],
) -> pd.DataFrame:
    """Per-region net-load level + ramp features from PJM RT net-load actuals.

    Pre-2019-04-02 rows have NaN net_load_mw because PJM did not publish solar
    actuals; those dates land as NaN per-region net_load_* features and the
    NaN-aware distance metric handles them per-feature.
    """
    if df_net_load_actual is None or len(df_net_load_actual) == 0:
        return pd.DataFrame(columns=["date"])
    df = df_net_load_actual.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    if "net_load_mw" in df.columns:
        df["net_load_mw"] = pd.to_numeric(df["net_load_mw"], errors="coerce")
        df = df.dropna(subset=["net_load_mw"])
    return _per_region_daily_aggregates(df, "net_load_mw", "net_load", regions)


def _slice_for_region_and_date(
    df: pd.DataFrame | None,
    region: str,
    target_date: date,
) -> pd.DataFrame | None:
    """Return df rows matching region (if a region column exists) and target_date."""
    if df is None or len(df) == 0:
        return None
    sub = df.copy()
    if "region" in sub.columns:
        sub = sub[sub["region"].astype(str) == region]
    if "date" in sub.columns:
        sub["date"] = pd.to_datetime(sub["date"]).dt.date
        sub = sub[sub["date"] == target_date]
    return sub if len(sub) > 0 else None


def _build_load_features_query(
    df_pjm_load_forecast: pd.DataFrame | None,
    df_meteo_load_forecast: pd.DataFrame | None,
    df_rt_load_fallback: pd.DataFrame | None,
    target_date: date,
    regions: list[str],
) -> pd.DataFrame:
    """Per-region load features. RTO from PJM forecast, others from Meteologica."""
    out: pd.DataFrame | None = None
    for region in regions:
        primary = df_pjm_load_forecast if region == "RTO" else df_meteo_load_forecast
        sub = _slice_for_region_and_date(primary, region, target_date)
        value_col = "forecast_load_mw"
        if sub is None or value_col not in sub.columns:
            sub = _slice_for_region_and_date(df_rt_load_fallback, region, target_date)
            value_col = "rt_load_mw" if sub is not None and "rt_load_mw" in sub.columns else value_col
        if sub is None or value_col not in sub.columns:
            continue
        daily = _load_daily_aggregates(
            sub[["date", "hour_ending", value_col]],
            value_col=value_col,
            prefix="load",
        )
        if len(daily) == 0:
            continue
        suffix = f"_{region.lower()}"
        rename_map = {c: f"{c}{suffix}" for c in daily.columns if c != "date"}
        daily = daily.rename(columns=rename_map)
        out = daily if out is None else out.merge(daily, on="date", how="outer")
    return out if out is not None else pd.DataFrame(columns=["date"])


def _build_net_load_features_query(
    df_pjm_net_load_forecast: pd.DataFrame | None,
    df_meteo_net_load_forecast: pd.DataFrame | None,
    target_date: date,
    regions: list[str],
) -> pd.DataFrame:
    """Per-region net-load features. RTO from PJM, others from Meteologica."""
    out: pd.DataFrame | None = None
    for region in regions:
        primary = df_pjm_net_load_forecast if region == "RTO" else df_meteo_net_load_forecast
        sub = _slice_for_region_and_date(primary, region, target_date)
        if sub is None or "net_load_forecast_mw" not in sub.columns:
            continue
        daily = _load_daily_aggregates(
            sub[["date", "hour_ending", "net_load_forecast_mw"]].rename(
                columns={"net_load_forecast_mw": "net_load"},
            ),
            value_col="net_load",
            prefix="net_load",
        )
        if len(daily) == 0:
            continue
        suffix = f"_{region.lower()}"
        rename_map = {c: f"{c}{suffix}" for c in daily.columns if c != "date"}
        daily = daily.rename(columns=rename_map)
        out = daily if out is None else out.merge(daily, on="date", how="outer")
    return out if out is not None else pd.DataFrame(columns=["date"])


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


def _build_renewable_features_pool(
    df_net_load_actual: pd.DataFrame | None,
    regions: list[str],
) -> pd.DataFrame:
    """Per-region solar/wind/renewable daily-avg features from PJM RT net-load actuals.

    Uses the per-region solar_gen_mw and wind_gen_mw columns. Pre-2019-04-02 hours
    have NaN solar_gen_mw, which propagates NaN into solar and renewable features
    for those dates while wind features remain populated.
    """
    if df_net_load_actual is None or len(df_net_load_actual) == 0:
        return pd.DataFrame(columns=["date"])
    df = df_net_load_actual.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    if "solar_gen_mw" in df.columns:
        df["solar_gen_mw"] = pd.to_numeric(df["solar_gen_mw"], errors="coerce")
    if "wind_gen_mw" in df.columns:
        df["wind_gen_mw"] = pd.to_numeric(df["wind_gen_mw"], errors="coerce")
    if "solar_gen_mw" in df.columns and "wind_gen_mw" in df.columns:
        df["renewable_mw"] = df["solar_gen_mw"] + df["wind_gen_mw"]

    parts: list[pd.DataFrame] = []
    if "solar_gen_mw" in df.columns:
        parts.append(_per_region_daily_avg(df, "solar_gen_mw", "solar", regions))
    if "wind_gen_mw" in df.columns:
        parts.append(_per_region_daily_avg(df, "wind_gen_mw", "wind", regions))
    if "renewable_mw" in df.columns:
        parts.append(_per_region_daily_avg(df, "renewable_mw", "renewable", regions))

    out: pd.DataFrame | None = None
    for part in parts:
        if part is None or len(part) == 0:
            continue
        out = part if out is None else out.merge(part, on="date", how="outer")
    return out if out is not None else pd.DataFrame(columns=["date"])


def _build_renewable_features_query(
    df_pjm_solar_forecast: pd.DataFrame | None,
    df_pjm_wind_forecast: pd.DataFrame | None,
    df_meteo_solar_forecast: pd.DataFrame | None,
    df_meteo_wind_forecast: pd.DataFrame | None,
    target_date: date,
    regions: list[str],
) -> dict[str, float]:
    """Per-region solar/wind/renewable daily-avg features.

    RTO uses PJM solar/wind forecasts (system-wide files, no region column);
    MIDATL/WEST/SOUTH use Meteologica regional forecasts.
    """
    out: dict[str, float] = {}

    def _avg_value(df: pd.DataFrame | None, region: str, value_col: str) -> float:
        if df is None or len(df) == 0 or value_col not in df.columns:
            return np.nan
        sub = df
        if "region" in sub.columns:
            sub = sub[sub["region"].astype(str) == region]
        if "date" in sub.columns:
            sub_dates = pd.to_datetime(sub["date"]).dt.date
            sub = sub[sub_dates == target_date]
        if len(sub) == 0:
            return np.nan
        vals = pd.to_numeric(sub[value_col], errors="coerce").dropna()
        return float(vals.mean()) if len(vals) > 0 else np.nan

    for region in regions:
        if region == "RTO":
            solar_df = df_pjm_solar_forecast
            wind_df = df_pjm_wind_forecast
        else:
            solar_df = df_meteo_solar_forecast
            wind_df = df_meteo_wind_forecast

        solar_avg = _avg_value(solar_df, region, "solar_forecast")
        wind_avg = _avg_value(wind_df, region, "wind_forecast")
        suffix = f"_{region.lower()}"

        out[f"solar_daily_avg{suffix}"] = solar_avg
        out[f"wind_daily_avg{suffix}"] = wind_avg
        if pd.notna(solar_avg) and pd.notna(wind_avg):
            out[f"renewable_daily_avg{suffix}"] = float(solar_avg) + float(wind_avg)
        elif pd.notna(solar_avg):
            out[f"renewable_daily_avg{suffix}"] = float(solar_avg)
        elif pd.notna(wind_avg):
            out[f"renewable_daily_avg{suffix}"] = float(wind_avg)
        else:
            out[f"renewable_daily_avg{suffix}"] = np.nan
    return out


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
    df_gas = _safe_load(loader.load_gas_prices_hourly, cache_dir)
    df_outages_actual = _safe_load(loader.load_outages_actual, cache_dir)
    df_net_load_actual = _safe_load(loader.load_net_load_actuals, cache_dir)

    df_labels = _build_lmp_labels(df_lmp_da, hub)

    df_load = _build_load_features_pool(df_rt_load, configs.LOAD_REGIONS)
    df_outages = _build_outage_features_pool(df_outages_actual)
    df_renewables = _build_renewable_features_pool(df_net_load_actual, configs.LOAD_REGIONS)
    df_net_load = _build_net_load_features_pool(df_net_load_actual, configs.LOAD_REGIONS)
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

    pool = df_labels.copy()
    for part in (
        df_load,
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

    df_pjm_load_forecast = _safe_load(loader.load_load_forecast, cache_dir)
    df_meteo_load_forecast = _safe_load(loader.load_meteologica_load_forecast, cache_dir)
    df_rt_load = _safe_load(loader.load_load_rt, cache_dir)

    cal = _calendar_for_date(target_date)

    # Per-region load forecast: PJM for RTO, Meteologica for MIDATL/WEST/SOUTH;
    # fall back to realized RT load if the forecast feed is missing for a region.
    df_load_q = _build_load_features_query(
        df_pjm_load_forecast,
        df_meteo_load_forecast,
        df_rt_load,
        target_date,
        configs.LOAD_REGIONS,
    )

    df_gas_q = pd.DataFrame(columns=["date"])
    if include_gas:
        df_gas = _safe_load(loader.load_gas_prices_hourly, cache_dir)
        if df_gas is not None and len(df_gas) > 0:
            gf = df_gas[pd.to_datetime(df_gas["date"]).dt.date == target_date].copy()
            if len(gf) > 0:
                df_gas_q = _build_gas_features(gf)

    outage_vals: dict[str, float] = {}
    if include_outages:
        df_outage_forecast = _safe_load(loader.load_outages_forecast, cache_dir)
        outage_vals = _build_outage_features_query(df_outage_forecast, target_date)

    # Per-region solar/wind/renewable: PJM (system-wide) for RTO, Meteologica for the rest.
    renewable_vals: dict[str, float] = {}
    if include_renewables:
        df_pjm_solar = _safe_load(loader.load_solar_forecast, cache_dir)
        df_pjm_wind = _safe_load(loader.load_wind_forecast, cache_dir)
        df_meteo_solar = _safe_load(loader.load_meteologica_solar_forecast, cache_dir)
        df_meteo_wind = _safe_load(loader.load_meteologica_wind_forecast, cache_dir)
        renewable_vals = _build_renewable_features_query(
            df_pjm_solar, df_pjm_wind, df_meteo_solar, df_meteo_wind,
            target_date, configs.LOAD_REGIONS,
        )

    # Per-region net-load forecast: PJM for RTO, Meteologica for MIDATL/WEST/SOUTH.
    df_net_load_q = pd.DataFrame(columns=["date"])
    if include_net_load:
        df_pjm_net_load = _safe_load(loader.load_net_load_forecast, cache_dir)
        df_meteo_net_load = _safe_load(loader.load_meteologica_net_load_forecast, cache_dir)
        df_net_load_q = _build_net_load_features_query(
            df_pjm_net_load, df_meteo_net_load,
            target_date, configs.LOAD_REGIONS,
        )

    row_df = pd.DataFrame({"date": [target_date]})
    for part in (df_load_q, df_gas_q, df_net_load_q):
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
