"""Shared parquet loading + pool/query assembly for the three model builders.

The per-model builder modules are thin wrappers around ``build_pool_from_spec``
and ``build_query_row_from_spec`` here; the spec's ``domains`` field drives
which features are pulled in and joined.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.data import loader
from da_models.common.data.loader import _resolve_cache_dir
from da_models.like_day_model_knn import configs
from da_models.like_day_model_knn.domains import (
    DOMAIN_REGISTRY,
    all_feature_cols,
)

logger = logging.getLogger(__name__)


def resolved_load_forecast_paths(cache_dir: Path | None) -> list[Path]:
    """Absolute paths of the load-forecast parquets that exist on disk."""
    resolved = _resolve_cache_dir(cache_dir)
    return [
        resolved / name
        for name in configs.LOAD_FORECAST_PARQUETS
        if (resolved / name).exists()
    ]


def resolved_lmp_da_path(cache_dir: Path | None) -> Path | None:
    """Absolute path of the DA LMP labels parquet, or ``None`` if missing."""
    resolved = _resolve_cache_dir(cache_dir)
    p = resolved / configs.LMP_DA_PARQUET
    return p if p.exists() else None


def load_pjm_load_forecast(cache_dir: Path | None) -> pd.DataFrame:
    """Load PJM load-forecast features from the historical-backfill parquet."""
    paths = resolved_load_forecast_paths(cache_dir)
    if not paths:
        logger.warning(
            "No load-forecast parquets found at %s (looked for %s) - "
            "falling back to default loader search",
            _resolve_cache_dir(cache_dir),
            configs.LOAD_FORECAST_PARQUETS,
        )
        return loader.load_load_forecast(cache_dir=cache_dir)

    parts: list[pd.DataFrame] = []
    for p in paths:
        parts.append(loader.load_load_forecast(path=p))
        logger.info("Loaded PJM load forecast: %s", p.name)

    df = pd.concat(parts, ignore_index=True)
    df = df.drop_duplicates(subset=["date", "hour_ending", "region"], keep="first")
    df = df.sort_values(["region", "date", "hour_ending"]).reset_index(drop=True)
    return df


def load_lmp_da(cache_dir: Path | None) -> pd.DataFrame:
    """Load DA LMPs from ``configs.LMP_DA_PARQUET`` (explicit), falling back
    to the shared loader's default search if the named file is missing."""
    p = resolved_lmp_da_path(cache_dir)
    if p is None:
        logger.warning(
            "DA LMP parquet not found at %s - falling back to default loader search",
            _resolve_cache_dir(cache_dir) / configs.LMP_DA_PARQUET,
        )
        return loader.load_lmps_da(cache_dir=cache_dir)
    logger.info("Loaded DA LMPs: %s", p.name)
    return loader.load_lmps_da(path=p)


def filter_to_region(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """Restrict a hourly load forecast frame to a specific PJM region (e.g. RTO).

    Assumes canonical dtypes from ``loader._normalize_load_forecast``.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()
    if "region" in df.columns:
        return df[df["region"] == region].copy()
    return df.copy()


def build_lmp_labels(df_lmp_da: pd.DataFrame, hub: str) -> pd.DataFrame:
    """One row per delivery date with lmp_h1..lmp_h24 for the configured hub."""
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
        df.pivot_table(index="date", columns="hour_ending", values="lmp", aggfunc="mean")
        .reindex(columns=configs.HOURS)
        .rename(columns={h: f"lmp_h{h}" for h in configs.HOURS})
        .reset_index()
    )
    return pivot


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Add NaN columns for any names missing from the frame, preserving order."""
    out = df.copy()
    missing = [c for c in columns if c not in out.columns]
    if missing:
        nan_df = pd.DataFrame({c: np.nan for c in missing}, index=out.index)
        out = pd.concat([out, nan_df], axis=1)
    return out


def load_dates_daily(cache_dir: Path | None) -> pd.DataFrame:
    """Calendar metadata frame from ``pjm_dates_daily.parquet``.

    Thin wrapper around ``calendar.load_pjm_dates_daily`` so per-model
    forecast/single_day modules don't need to import the calendar module
    directly; they already import ``_shared``.
    """
    from da_models.like_day_model_knn import calendar as _calendar
    return _calendar.load_pjm_dates_daily(cache_dir)


def load_hourly_rto(cache_dir: Path | None) -> pd.DataFrame:
    """PJM RTO hourly load forecast, region-filtered.

    Types are already canonical from ``loader._normalize_load_forecast`` -
    no re-coercion needed here.
    """
    df = load_pjm_load_forecast(cache_dir=cache_dir)
    if "region" in df.columns:
        df = df[df["region"] == configs.LOAD_REGION].copy()
    return df


# ── Spec-driven pool/query assembly ─────────────────────────────────────

def build_pool_from_spec(
    spec: configs.ModelSpec,
    hub: str = configs.HUB,
    cache_dir: Path | None = configs.CACHE_DIR,
) -> pd.DataFrame:
    """One row per delivery date with all enabled-domain feature cols + 24
    LMP labels.

    Domains are pulled from ``spec.domains`` and inner-joined on ``date``
    so any candidate day missing a feature is dropped (per the design call
    on missing data). LMP labels are then left-joined for the hub.
    """
    if not spec.domains:
        raise ValueError(f"Spec '{spec.name}' has no domains.")

    feat: pd.DataFrame | None = None
    for name in spec.domains:
        domain = DOMAIN_REGISTRY[name]
        df = domain.pool_builder(cache_dir)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        if feat is None:
            feat = df
        else:
            feat = feat.merge(df, on="date", how="inner")

    df_lmp_da = load_lmp_da(cache_dir=cache_dir)
    df_labels = build_lmp_labels(df_lmp_da, hub)
    pool = feat.merge(df_labels, on="date", how="left")

    feature_cols = all_feature_cols(spec.domains)
    keep = ["date"] + feature_cols + configs.LMP_LABEL_COLUMNS
    pool = ensure_columns(pool, keep)[keep]
    pool = pool.sort_values("date").reset_index(drop=True)

    n_features_filled = int(pool[feature_cols].notna().any(axis=1).sum())
    n_labels_filled = int(pool[configs.LMP_LABEL_COLUMNS].notna().any(axis=1).sum())
    logger.info(
        "%s pool: %d rows x %d features (%d w/ features, %d w/ labels) — domains=%s",
        spec.name, len(pool), len(feature_cols), n_features_filled,
        n_labels_filled, spec.domains,
    )
    return pool


def build_query_row_from_spec(
    spec: configs.ModelSpec,
    target_date: date,
    cache_dir: Path | None = configs.CACHE_DIR,
) -> pd.Series:
    """Single-row Series with all enabled-domain feature cols for ``target_date``.

    Each domain's query builder returns a one-row frame; results are
    horizontally concatenated. Missing values stay NaN (the engine handles
    NaN-aware distance per group).
    """
    if not spec.domains:
        raise ValueError(f"Spec '{spec.name}' has no domains.")

    parts: list[pd.DataFrame] = []
    for name in spec.domains:
        domain = DOMAIN_REGISTRY[name]
        df = domain.query_builder(target_date, cache_dir)
        if len(df) == 0:
            empty = {"date": target_date, **{c: np.nan for c in domain.feature_cols}}
            df = pd.DataFrame([empty])
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[df["date"] == target_date]
        if len(df) == 0:
            empty = {"date": target_date, **{c: np.nan for c in domain.feature_cols}}
            df = pd.DataFrame([empty])
        parts.append(df.iloc[[0]].reset_index(drop=True))

    out = parts[0]
    for p in parts[1:]:
        out = out.merge(p, on="date", how="left")

    feature_cols = all_feature_cols(spec.domains)
    out = ensure_columns(out, ["date"] + feature_cols)[["date"] + feature_cols]
    n_filled = int(pd.Series(out[feature_cols].iloc[0]).notna().sum())
    logger.info(
        "%s query for %s: %d/%d features filled — domains=%s",
        spec.name, target_date, n_filled, len(feature_cols), spec.domains,
    )
    return out.iloc[0]
