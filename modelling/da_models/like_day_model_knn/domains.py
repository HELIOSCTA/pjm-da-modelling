"""Feature-domain plugins for like_day_model_knn.

A domain produces a pool feature frame (one row per historical delivery date)
and a query feature frame (one row for the target delivery date), both keyed
by ``date``. The variant builders concatenate domains by inner-joining on
``date`` and then optionally broadcast across ``hour_ending`` for ``per_hour``.

Pool features prefer the historical DA-cutoff forecast where the parquet
covers all 24 hours of the date, falling back to RT actuals for pre-backfill
dates (via ``loader.load_load_coalesced``). Query features come from the
DA-cutoff forecast for ``target_date``. Pool and query therefore share the
same forecast signal in the overlap window — apples-to-apples at decision
time — while old history still contributes via RT.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from da_models.common.data import loader

# Region used everywhere a region filter applies.
RTO = "RTO"

# Column names produced per domain (kept stable so engine/spec can reference).
LOAD_HOURLY_COLS = [f"load_h{h}" for h in range(1, 25)]
SOLAR_HOURLY_COLS = [f"solar_h{h}" for h in range(1, 25)]
WIND_HOURLY_COLS = [f"wind_h{h}" for h in range(1, 25)]
OUTAGE_LEVEL_COLS = ["outage_total_mw", "outage_planned_mw", "outage_forced_mw"]
GAS_LEVEL_COLS = ["gas_m3_avg"]


@dataclass(frozen=True)
class FeatureDomain:
    """A toggleable feature domain.

    ``pool_builder``: returns historical features keyed by ``date``.
    ``query_builder``: returns one row of features for ``target_date``.
    Both produce identical column sets so the engine sees a uniform schema.
    """
    name: str
    description: str
    feature_groups: dict[str, list[str]]
    feature_group_weights: dict[str, float]
    pool_builder: Callable[[Path | None], pd.DataFrame]
    query_builder: Callable[[date, Path | None], pd.DataFrame]

    @property
    def feature_cols(self) -> list[str]:
        seen: list[str] = []
        for cols in self.feature_groups.values():
            for c in cols:
                if c not in seen:
                    seen.append(c)
        return seen


# ── Helpers ──────────────────────────────────────────────────────────────

def _to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.date


def _hourly_load_profile(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Wide pivot of hourly load: one col per HE, named load_h1..load_h24."""
    return _hourly_value_profile(df, value_col, output_prefix="load")


def _hourly_value_profile(
    df: pd.DataFrame, value_col: str, output_prefix: str,
) -> pd.DataFrame:
    """Generic wide pivot: one col per HE, named ``{output_prefix}_h1..{output_prefix}_h24``."""
    out_cols = [f"{output_prefix}_h{h}" for h in range(1, 25)]
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["date"] + out_cols)
    work = df[["date", "hour_ending", value_col]].copy()
    work["date"] = _to_date(work["date"])
    work["hour_ending"] = pd.to_numeric(work["hour_ending"], errors="coerce").astype("Int64")
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=["date", "hour_ending", value_col])
    if len(work) == 0:
        return pd.DataFrame(columns=["date"] + out_cols)
    work["hour_ending"] = work["hour_ending"].astype(int)

    pivot = work.pivot_table(
        index="date", columns="hour_ending", values=value_col, aggfunc="mean",
    ).reindex(columns=range(1, 25))
    pivot = pivot.rename(columns={h: f"{output_prefix}_h{h}" for h in range(1, 25)})
    return pivot.reset_index()


# ── rto_load_profile ─────────────────────────────────────────────────────

def _build_rto_load_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_load_coalesced(cache_dir=cache_dir)
    df = df[df["region"].astype(str) == RTO]
    return _hourly_load_profile(df, "load_mw")


def _build_rto_load_profile_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_load_forecast(cache_dir=cache_dir)
    df = df[df["region"].astype(str) == RTO].copy()
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    return _hourly_load_profile(df, "forecast_load_mw")


RTO_LOAD_PROFILE = FeatureDomain(
    name="rto_load_profile",
    description="RTO load — 24 hourly cols (load_h1..load_h24) in 5 zones.",
    feature_groups={
        "load_overnight": [f"load_h{h}" for h in range(1, 7)],
        "load_morning":   [f"load_h{h}" for h in range(7, 12)],
        "load_midday":    [f"load_h{h}" for h in range(12, 17)],
        "load_peak":      [f"load_h{h}" for h in range(17, 21)],
        "load_evening":   [f"load_h{h}" for h in range(21, 25)],
    },
    feature_group_weights={
        "load_overnight": 1.0,
        "load_morning":   1.5,
        "load_midday":    2.0,
        "load_peak":      3.5,
        "load_evening":   2.0,
    },
    pool_builder=_build_rto_load_profile_pool,
    query_builder=_build_rto_load_profile_query,
)


# ── solar_profile (per-HE level) ─────────────────────────────────────────
# Designed for per_hour matching: 24 hourly cols participate in the dynamic
# 3-hour window distance, parallel to rto_load_profile. Pool prefers the
# DA-cutoff forecast (vintage-consistent with the query); RT actuals fill
# pre-2019 gaps via load_solar_coalesced.

def _build_solar_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_solar_coalesced(cache_dir=cache_dir)
    val = "solar_forecast" if "solar_forecast" in df.columns else "solar_mw"
    return _hourly_value_profile(df, val, output_prefix="solar")


def _build_solar_profile_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_solar_forecast(cache_dir=cache_dir).copy()
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    val = "solar_forecast" if "solar_forecast" in df.columns else "solar_mw"
    return _hourly_value_profile(df, val, output_prefix="solar")


SOLAR_PROFILE = FeatureDomain(
    name="solar_profile",
    description="Solar — 24 hourly level cols (solar_h1..solar_h24).",
    feature_groups={"solar_level": SOLAR_HOURLY_COLS},
    feature_group_weights={"solar_level": 1.5},
    pool_builder=_build_solar_profile_pool,
    query_builder=_build_solar_profile_query,
)


# ── wind_profile (per-HE level) ──────────────────────────────────────────

def _build_wind_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_wind_coalesced(cache_dir=cache_dir)
    val = "wind_forecast" if "wind_forecast" in df.columns else "wind_mw"
    return _hourly_value_profile(df, val, output_prefix="wind")


def _build_wind_profile_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_wind_forecast(cache_dir=cache_dir).copy()
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    val = "wind_forecast" if "wind_forecast" in df.columns else "wind_mw"
    return _hourly_value_profile(df, val, output_prefix="wind")


WIND_PROFILE = FeatureDomain(
    name="wind_profile",
    description="Wind — 24 hourly level cols (wind_h1..wind_h24).",
    feature_groups={"wind_level": WIND_HOURLY_COLS},
    feature_group_weights={"wind_level": 1.5},
    pool_builder=_build_wind_profile_pool,
    query_builder=_build_wind_profile_query,
)


# ── outages_level (daily, broadcast across HEs) ──────────────────────────
# Outages MW are published daily by PJM — no hourly granularity exists.
# Daily-broadcast features bias which candidate dates rank high overall;
# they don't differentiate between HEs of a given candidate date.

def _outages_level_features(df_rto: pd.DataFrame) -> pd.DataFrame:
    if df_rto is None or len(df_rto) == 0:
        return pd.DataFrame(columns=["date"] + OUTAGE_LEVEL_COLS)
    date_col = "forecast_date" if "forecast_date" in df_rto.columns else "date"
    df = df_rto[[date_col, "total_outages_mw", "planned_outages_mw", "forced_outages_mw"]].copy()
    df = df.rename(columns={date_col: "date"})
    df["date"] = _to_date(df["date"])
    for c in ["total_outages_mw", "planned_outages_mw", "forced_outages_mw"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df = df.rename(columns={
        "total_outages_mw": "outage_total_mw",
        "planned_outages_mw": "outage_planned_mw",
        "forced_outages_mw": "outage_forced_mw",
    })
    return df[["date"] + OUTAGE_LEVEL_COLS]


def _build_outages_level_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_outages_forecast_history(cache_dir=cache_dir, lead_days=1)
    df = df[df["region"].astype(str) == RTO]
    return _outages_level_features(df)


def _build_outages_level_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_outages_forecast_history(cache_dir=cache_dir, lead_days=1)
    df = df[df["region"].astype(str) == RTO].copy()
    date_col = "forecast_date" if "forecast_date" in df.columns else "date"
    df["date"] = _to_date(df[date_col])
    df = df[df["date"] == target_date]
    if len(df) == 0:
        empty = {"date": target_date, **{c: np.nan for c in OUTAGE_LEVEL_COLS}}
        return pd.DataFrame([empty])
    row = df.iloc[0]
    return pd.DataFrame([{
        "date": target_date,
        "outage_total_mw": float(row.get("total_outages_mw", np.nan)),
        "outage_planned_mw": float(row.get("planned_outages_mw", np.nan)),
        "outage_forced_mw": float(row.get("forced_outages_mw", np.nan)),
    }])[["date"] + OUTAGE_LEVEL_COLS]


OUTAGES_LEVEL = FeatureDomain(
    name="outages_level",
    description="RTO outages — 3 daily level cols (total/planned/forced MW). Broadcast across HEs.",
    feature_groups={"outage_level": OUTAGE_LEVEL_COLS},
    feature_group_weights={"outage_level": 2.0},
    pool_builder=_build_outages_level_pool,
    query_builder=_build_outages_level_query,
)


# ── gas_level (daily, broadcast across HEs) ──────────────────────────────
# Hourly gas ticks exist but next-day cash gas settles once per day; daily
# mean of M3 is the right denoising for next-day LMP prediction.

def _gas_level_features(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0 or "gas_m3" not in df.columns:
        return pd.DataFrame(columns=["date"] + GAS_LEVEL_COLS)
    work = df[["date", "gas_m3"]].copy()
    work["date"] = _to_date(work["date"])
    work["gas_m3"] = pd.to_numeric(work["gas_m3"], errors="coerce")
    work = work.dropna(subset=["date", "gas_m3"])
    if len(work) == 0:
        return pd.DataFrame(columns=["date"] + GAS_LEVEL_COLS)
    daily = work.groupby("date", as_index=False).agg(gas_m3_avg=("gas_m3", "mean"))
    return daily[["date"] + GAS_LEVEL_COLS]


def _build_gas_level_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_gas_prices_hourly(cache_dir=cache_dir)
    return _gas_level_features(df)


def _build_gas_level_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_gas_prices_hourly(cache_dir=cache_dir)
    daily = _gas_level_features(df)
    out = daily[daily["date"] == target_date]
    if len(out) == 0:
        out = pd.DataFrame([{"date": target_date, "gas_m3_avg": np.nan}])
    return out[["date"] + GAS_LEVEL_COLS]


GAS_LEVEL = FeatureDomain(
    name="gas_level",
    description="Gas — 1 daily level col (M3 cash daily mean). Broadcast across HEs.",
    feature_groups={"gas_level": GAS_LEVEL_COLS},
    feature_group_weights={"gas_level": 1.0},
    pool_builder=_build_gas_level_pool,
    query_builder=_build_gas_level_query,
)


# ── Registry ─────────────────────────────────────────────────────────────

DOMAIN_REGISTRY: dict[str, FeatureDomain] = {
    RTO_LOAD_PROFILE.name: RTO_LOAD_PROFILE,
    SOLAR_PROFILE.name: SOLAR_PROFILE,
    WIND_PROFILE.name: WIND_PROFILE,
    OUTAGES_LEVEL.name: OUTAGES_LEVEL,
    GAS_LEVEL.name: GAS_LEVEL,
}


def resolved_feature_groups(domain_names: tuple[str, ...]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for n in domain_names:
        out.update(DOMAIN_REGISTRY[n].feature_groups)
    return out


def resolved_feature_group_weights(domain_names: tuple[str, ...]) -> dict[str, float]:
    """Sum each domain's group weights, then renormalize so total = 1.0."""
    raw: dict[str, float] = {}
    for n in domain_names:
        raw.update(DOMAIN_REGISTRY[n].feature_group_weights)
    total = sum(raw.values())
    if total <= 0:
        return raw
    return {k: v / total for k, v in raw.items()}


def all_feature_cols(domain_names: tuple[str, ...]) -> list[str]:
    seen: list[str] = []
    for n in domain_names:
        for c in DOMAIN_REGISTRY[n].feature_cols:
            if c not in seen:
                seen.append(c)
    return seen
