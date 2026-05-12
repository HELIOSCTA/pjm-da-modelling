"""DA LMP pool primitives shared across model families.

Loads the day-ahead LMP parquet, filters to a hub, and pivots to one
row per delivery date with ``lmp_h1..lmp_h24`` columns. This wide
schema is the canonical pool shape consumed by every forecaster
(like-day analog selection, naive baselines, future families).
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from backend.modelling.da_models.common.configs import HOURS
from backend.modelling.da_models.common.data import loader
from backend.modelling.da_models.common.data.loader import _resolve_cache_dir

logger = logging.getLogger(__name__)


LMP_DA_PARQUET: str = "pjm_lmps_hourly.parquet"
LMP_HOUR_COLUMNS: list[str] = [f"lmp_h{h}" for h in HOURS]


def resolved_lmp_da_path(
    cache_dir: Path | None,
    parquet_name: str = LMP_DA_PARQUET,
) -> Path | None:
    """Absolute path of the DA LMP labels parquet, or ``None`` if missing."""
    resolved = _resolve_cache_dir(cache_dir)
    p = resolved / parquet_name
    return p if p.exists() else None


def load_lmp_da(
    cache_dir: Path | None,
    parquet_name: str = LMP_DA_PARQUET,
) -> pd.DataFrame:
    """Load DA LMPs from the named parquet, falling back to the shared
    loader's default search if the named file is missing."""
    p = resolved_lmp_da_path(cache_dir, parquet_name)
    if p is None:
        logger.warning(
            "DA LMP parquet not found at %s - falling back to default loader search",
            _resolve_cache_dir(cache_dir) / parquet_name,
        )
        return loader.load_lmps_da(cache_dir=cache_dir)
    logger.info("Loaded DA LMPs: %s", p.name)
    return loader.load_lmps_da(path=p)


def build_lmp_labels(df_lmp_da: pd.DataFrame, hub: str) -> pd.DataFrame:
    """One row per delivery date with ``lmp_h1..lmp_h24`` for the configured hub."""
    if df_lmp_da is None or len(df_lmp_da) == 0:
        return pd.DataFrame(columns=["date"] + LMP_HOUR_COLUMNS)

    df = df_lmp_da[df_lmp_da["region"] == hub].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce")
    df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")
    df = df.dropna(subset=["date", "hour_ending"])
    if len(df) == 0:
        return pd.DataFrame(columns=["date"] + LMP_HOUR_COLUMNS)

    df["hour_ending"] = df["hour_ending"].astype(int)
    pivot = (
        df.pivot_table(index="date", columns="hour_ending", values="lmp", aggfunc="mean")
        .reindex(columns=list(HOURS))
        .rename(columns={h: f"lmp_h{h}" for h in HOURS})
        .reset_index()
    )
    return pivot
