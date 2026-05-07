"""Regional Meteologica feature domain for the sunny KNN variant.

Defines a single hourly domain ``regional_meteo_scalar`` that pivots
``loader.load_meteologica_supply_demand_coalesced`` over MIDATL + WEST
+ SOUTH into 12 columns (load / solar / wind / net_load per region)
grouped into 3 distance groups whose weights mirror their RTO
counterparts in the baseline spec — load 3.0, renewable 1.5, net_load
2.0. Per-group raw weight totals match the RTO spec (6.5).

Registered into the shared ``like_day_model_knn_sunny.domains``
registries on import. All three sub-zones with Meteologica forecast
coverage are included; the engine z-scores each feature and lets the
data speak.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

from da_models.common.data import loader
from da_models.like_day_model_knn_sunny.domains import (
    DOMAIN_REGISTRY,
    FeatureDomain,
    _empty_long,
    _to_date,
)

logger = logging.getLogger(__name__)


REGIONS: tuple[str, ...] = ("MIDATL", "WEST", "SOUTH")

_SERIES_PREFIX: dict[str, str] = {
    "load_mw": "load",
    "solar_mw": "solar",
    "wind_mw": "wind",
    "net_load_mw": "net_load",
}


def _feature_col(series: str, region: str) -> str:
    return f"{series}_{region.lower()}_at_hour"


REGIONAL_FEATURE_COLS: list[str] = [
    _feature_col(prefix, r) for prefix in _SERIES_PREFIX.values() for r in REGIONS
]


def _load_meteo_regional_wide(
    cache_dir: Path | None,
    regions: Iterable[str] = REGIONS,
) -> pd.DataFrame:
    """Load Meteologica supply-demand and pivot regions wide.

    Single loader call returns RTO + MIDATL + WEST + SOUTH long-format.
    Filter to ``regions`` and pivot so each (series, region) becomes one
    column. The unified loader has already made the joint forecast-vs-RT
    decision, so the identity ``net_load = load - solar - wind`` holds
    within each row by construction.
    """
    regions = tuple(regions)
    try:
        df = loader.load_meteologica_supply_demand_coalesced(cache_dir=cache_dir)
    except Exception as exc:
        logger.warning("Meteologica regional loader failed: %s", exc)
        return _empty_long(REGIONAL_FEATURE_COLS)

    if df is None or len(df) == 0 or "region" not in df.columns:
        return _empty_long(REGIONAL_FEATURE_COLS)

    df = df[df["region"].astype(str).isin(regions)].copy()
    if len(df) == 0:
        return _empty_long(REGIONAL_FEATURE_COLS)

    df["date"] = _to_date(df["date"])
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype(
        "Int64"
    )
    df = df.dropna(subset=["date", "hour_ending"]).copy()
    df["hour_ending"] = df["hour_ending"].astype(int)

    grid = (
        df[["date", "hour_ending"]]
        .drop_duplicates()
        .sort_values(["date", "hour_ending"])
        .reset_index(drop=True)
    )
    out = grid

    for src_col, prefix in _SERIES_PREFIX.items():
        if src_col not in df.columns:
            for r in regions:
                out[_feature_col(prefix, r)] = pd.NA
            continue
        wide = df.pivot_table(
            index=["date", "hour_ending"],
            columns="region",
            values=src_col,
            aggfunc="first",
        ).reset_index()
        rename_map = {r: _feature_col(prefix, r) for r in regions if r in wide.columns}
        wide = wide.rename(columns=rename_map)
        for r in regions:
            col = _feature_col(prefix, r)
            if col not in wide.columns:
                wide[col] = pd.NA
        keep = ["date", "hour_ending"] + [_feature_col(prefix, r) for r in regions]
        out = out.merge(wide[keep], on=["date", "hour_ending"], how="left")

    return out.reset_index(drop=True)


def _build_regional_meteo_pool(cache_dir: Path | None) -> pd.DataFrame:
    return _load_meteo_regional_wide(cache_dir)


def _build_regional_meteo_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    pool = _load_meteo_regional_wide(cache_dir)
    if len(pool) == 0:
        return _empty_long(REGIONAL_FEATURE_COLS)
    out = pool[pool["date"] == target_date].copy()
    return out.reset_index(drop=True)


REGIONAL_METEO_SCALAR = FeatureDomain(
    name="regional_meteo_scalar",
    description=(
        "Meteologica MIDATL + WEST + SOUTH load / solar / wind / net_load "
        "at target HE. Single-call coalesced loader; identity "
        "net_load = load - solar - wind holds row-wise. Replaces RTO "
        "load + renewable + net_load groups."
    ),
    feature_groups={
        "regional_load_at_hour": [_feature_col("load", r) for r in REGIONS],
        "regional_renewable_at_hour": [_feature_col("solar", r) for r in REGIONS]
        + [_feature_col("wind", r) for r in REGIONS],
        "regional_net_load_at_hour": [_feature_col("net_load", r) for r in REGIONS],
    },
    feature_group_weights={
        "regional_load_at_hour": 3.0,
        "regional_renewable_at_hour": 1.5,
        "regional_net_load_at_hour": 2.0,
    },
    pool_builder=_build_regional_meteo_pool,
    query_builder=_build_regional_meteo_query,
)


DOMAIN_REGISTRY[REGIONAL_METEO_SCALAR.name] = REGIONAL_METEO_SCALAR
