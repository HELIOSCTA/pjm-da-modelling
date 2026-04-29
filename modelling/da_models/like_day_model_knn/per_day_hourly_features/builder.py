"""Pool and query builder for per_day_hourly_features.

Produces 24 raw hourly load forecast cols per delivery date plus 24 LMP labels:

  fcst_load_h1 .. fcst_load_h24 (raw forecast values from PJM RTO)

The engine groups these into 5 hour-blocks (overnight, morning, midday, peak,
evening) and weights them by ``PER_DAY_HOURLY_FEATURES_SPEC.feature_group_weights``.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.like_day_model_knn import _shared, configs

logger = logging.getLogger(__name__)


def _hourly_load_features(
    df_hourly: pd.DataFrame, value_col: str = "forecast_load_mw",
) -> pd.DataFrame:
    """Pivot hourly load forecast into 24 columns: fcst_load_h1..fcst_load_h24."""
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
    pivot = (
        df.pivot_table(index="date", columns="hour_ending", values=value_col, aggfunc="mean")
        .reindex(columns=range(1, 25))
    )
    pivot.columns = [f"fcst_load_h{int(h)}" for h in pivot.columns]
    return pivot.reset_index()


def _feature_cols() -> list[str]:
    """The 24 hourly feature columns this model uses (HE1..HE24)."""
    out: list[str] = []
    for cols in configs.PER_DAY_HOURLY_FEATURES_SPEC.feature_groups.values():
        for c in cols:
            if c not in out:
                out.append(c)
    return out


def build_pool(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    cache_dir: Path | None = configs.CACHE_DIR,
) -> pd.DataFrame:
    """One row per delivery date with 24 hourly load cols + 24 LMP cols."""
    _ = schema

    df_pjm_load = _shared.load_pjm_load_forecast(cache_dir=cache_dir)
    df_lmp_da = _shared.load_lmp_da(cache_dir=cache_dir)

    df_rto = _shared.filter_to_region(df_pjm_load, configs.LOAD_REGION)
    df_features = _hourly_load_features(df_rto)
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
        "per_day_hourly_features pool: %d rows x %d feature cols (%d w/ features, %d w/ labels)",
        len(pool), len(feature_cols), n_features_filled, n_labels_filled,
    )
    return pool


def build_query_row(
    target_date: date,
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
) -> pd.Series:
    """Query row with 24 hourly load cols for the target date."""
    _ = schema

    df_pjm_load = _shared.load_pjm_load_forecast(cache_dir=cache_dir)
    df_rto = _shared.filter_to_region(df_pjm_load, configs.LOAD_REGION)
    df_target = df_rto[df_rto["date"] == target_date].copy()

    feature_cols = _feature_cols()

    if len(df_target) == 0:
        logger.warning(
            "per_day_hourly_features: no load forecast rows for target_date=%s region=%s",
            target_date, configs.LOAD_REGION,
        )
        empty = {"date": target_date, **{c: np.nan for c in feature_cols}}
        return pd.Series(empty)

    df_features = _hourly_load_features(df_target)
    if len(df_features) == 0:
        empty = {"date": target_date, **{c: np.nan for c in feature_cols}}
        return pd.Series(empty)

    row_df = _shared.ensure_columns(df_features, ["date"] + feature_cols)[["date"] + feature_cols]
    row_df["date"] = pd.to_datetime(row_df["date"]).dt.date
    query = row_df.iloc[0].copy()
    n_filled = int(pd.Series(query[feature_cols]).notna().sum())
    logger.info(
        "per_day_hourly_features query for %s: %d/%d features filled",
        target_date, n_filled, len(feature_cols),
    )
    return query
