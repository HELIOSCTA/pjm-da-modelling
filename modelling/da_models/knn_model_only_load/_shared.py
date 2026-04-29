"""Shared parquet loading + LMP-label pivoting for the three model builders.

Each per-model builder owns its own feature derivation, but the upstream
plumbing (which parquet to load, region filter, LMP label pivoting) is
identical across models and lives here.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.data import loader
from da_models.common.data.loader import _resolve_cache_dir
from da_models.knn_model_only_load import configs

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
    """Restrict a hourly load forecast frame to a specific PJM region (e.g. RTO)."""
    if df is None or len(df) == 0:
        return pd.DataFrame()
    out = df.copy()
    if "region" in out.columns:
        out = out[out["region"].astype(str) == region]
    out["date"] = pd.to_datetime(out["date"]).dt.date
    return out


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


def load_hourly_rto(cache_dir: Path | None) -> pd.DataFrame:
    """PJM RTO hourly load forecast, region-filtered and typed.

    Used by dashboards to draw load curves alongside the analog selection.
    """
    df = load_pjm_load_forecast(cache_dir=cache_dir)
    if "region" in df.columns:
        df = df[df["region"].astype(str) == configs.LOAD_REGION].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["forecast_load_mw"] = pd.to_numeric(df["forecast_load_mw"], errors="coerce")
    return df
