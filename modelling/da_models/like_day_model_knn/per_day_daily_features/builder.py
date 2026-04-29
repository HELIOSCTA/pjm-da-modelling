"""Pool and query builder for per_day_daily_features.

Produces 6 daily summary cols per delivery date plus 24 hourly DA LMP labels:

  fcst_load_daily_avg, fcst_load_daily_peak, fcst_load_daily_valley,
  fcst_load_morning_ramp (HE5 -> HE8),
  fcst_load_evening_ramp (HE15 -> HE20),
  fcst_load_ramp_max (max consecutive-hour ramp).

Hourly load values are NOT carried in this pool - by design, this model is
the "lossy daily aggregation" baseline. Models that need raw hourly values
have their own builders in sibling subfolders.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.like_day_model_knn import _shared, configs

logger = logging.getLogger(__name__)


def _daily_summary_features(
    df_hourly: pd.DataFrame, value_col: str = "forecast_load_mw",
) -> pd.DataFrame:
    """Aggregate hourly load forecast into 6 daily summary columns."""
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
            fcst_load_daily_avg=(value_col, "mean"),
            fcst_load_daily_peak=(value_col, "max"),
            fcst_load_daily_valley=(value_col, "min"),
        )
        .reset_index()
    )

    df["ramp"] = df.groupby("date")[value_col].diff()
    daily = daily.merge(
        df.groupby("date", as_index=False)["ramp"]
        .max()
        .rename(columns={"ramp": "fcst_load_ramp_max"}),
        on="date", how="left",
    )

    he5 = df[df["hour_ending"] == 5][["date", value_col]].rename(columns={value_col: "he5"})
    he8 = df[df["hour_ending"] == 8][["date", value_col]].rename(columns={value_col: "he8"})
    he15 = df[df["hour_ending"] == 15][["date", value_col]].rename(columns={value_col: "he15"})
    he20 = df[df["hour_ending"] == 20][["date", value_col]].rename(columns={value_col: "he20"})
    daily = daily.merge(he5, on="date", how="left")
    daily = daily.merge(he8, on="date", how="left")
    daily = daily.merge(he15, on="date", how="left")
    daily = daily.merge(he20, on="date", how="left")
    daily["fcst_load_morning_ramp"] = daily["he8"] - daily["he5"]
    daily["fcst_load_evening_ramp"] = daily["he20"] - daily["he15"]
    return daily.drop(columns=["he5", "he8", "he15", "he20"])


def _feature_cols() -> list[str]:
    """The 6 daily summary feature columns this model uses."""
    out: list[str] = []
    for cols in configs.PER_DAY_DAILY_FEATURES_SPEC.feature_groups.values():
        for c in cols:
            if c not in out:
                out.append(c)
    return out


def build_pool(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    cache_dir: Path | None = configs.CACHE_DIR,
) -> pd.DataFrame:
    """One row per delivery date with 6 daily summary cols + 24 LMP cols."""
    _ = schema

    df_pjm_load = _shared.load_pjm_load_forecast(cache_dir=cache_dir)
    df_lmp_da = _shared.load_lmp_da(cache_dir=cache_dir)

    df_rto = _shared.filter_to_region(df_pjm_load, configs.LOAD_REGION)
    df_features = _daily_summary_features(df_rto)
    df_labels = _shared.build_lmp_labels(df_lmp_da, hub)

    pool = df_labels.merge(df_features, on="date", how="left")

    feature_cols = _feature_cols()
    keep_cols = ["date"] + feature_cols + configs.LMP_LABEL_COLUMNS
    pool = _shared.ensure_columns(pool, keep_cols)[keep_cols]
    pool["date"] = pd.to_datetime(pool["date"]).dt.date
    pool = pool.sort_values("date").reset_index(drop=True)

    n_features_filled = int(pool[feature_cols].notna().any(axis=1).sum())
    n_labels_filled = int(pool[configs.LMP_LABEL_COLUMNS].notna().any(axis=1).sum())
    logger.info(
        "per_day_daily_features pool: %d rows x %d feature cols (%d w/ features, %d w/ labels)",
        len(pool), len(feature_cols), n_features_filled, n_labels_filled,
    )
    return pool


def build_query_row(
    target_date: date,
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
) -> pd.Series:
    """Query row with the 6 daily summary cols for the target date."""
    _ = schema

    df_pjm_load = _shared.load_pjm_load_forecast(cache_dir=cache_dir)
    df_rto = _shared.filter_to_region(df_pjm_load, configs.LOAD_REGION)
    df_target = df_rto[df_rto["date"] == target_date].copy()

    feature_cols = _feature_cols()

    if len(df_target) == 0:
        logger.warning(
            "per_day_daily_features: no load forecast rows for target_date=%s region=%s",
            target_date, configs.LOAD_REGION,
        )
        empty = {"date": target_date, **{c: np.nan for c in feature_cols}}
        return pd.Series(empty)

    df_features = _daily_summary_features(df_target)
    if len(df_features) == 0:
        empty = {"date": target_date, **{c: np.nan for c in feature_cols}}
        return pd.Series(empty)

    row_df = _shared.ensure_columns(df_features, ["date"] + feature_cols)[["date"] + feature_cols]
    row_df["date"] = pd.to_datetime(row_df["date"]).dt.date
    query = row_df.iloc[0].copy()
    n_filled = int(pd.Series(query[feature_cols]).notna().sum())
    logger.info(
        "per_day_daily_features query for %s: %d/%d features filled",
        target_date, n_filled, len(feature_cols),
    )
    return query
