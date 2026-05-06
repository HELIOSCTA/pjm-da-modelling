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

import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from da_models.common.data import loader

# Region used everywhere a region filter applies.
RTO = "RTO"

# Wide-format hourly col conventions used INSIDE pool builders (each
# domain still pivots its source data to ``{stem}_h1..{stem}_h24`` —
# no change to per-domain pool/query construction). The wide cols are
# then melted to long-format scalars by ``_shared.build_pool_from_spec``;
# spec ``feature_groups`` reference the long col names listed below.
LOAD_HOURLY_COLS = [f"load_h{h}" for h in range(1, 25)]
LOAD_RAMP_1H_HOURLY_COLS = [f"load_ramp_1h_h{h}" for h in range(1, 25)]
LOAD_RAMP_3H_HOURLY_COLS = [f"load_ramp_3h_h{h}" for h in range(1, 25)]
SOLAR_HOURLY_COLS = [f"solar_h{h}" for h in range(1, 25)]
WIND_HOURLY_COLS = [f"wind_h{h}" for h in range(1, 25)]
NET_LOAD_HOURLY_COLS = [f"net_load_h{h}" for h in range(1, 25)]
TEMP_HOURLY_COLS = [f"temp_h{h}" for h in range(1, 25)]
OUTAGE_LEVEL_COLS = ["outage_total_mw"]  # sunny parity: total only
GAS_LEVEL_COLS = ["gas_m3_avg"]
CALENDAR_LEVEL_COLS = ["dow_sin", "dow_cos", "is_weekend"]

# Long-format scalar col names that ``feature_groups`` reference. These
# are the cols on the post-melt (date, hour_ending) rows — sunny-compatible
# names so cross-family code can share the schema.
LOAD_AT_HOUR_COL = "load_mw_at_hour"
SOLAR_AT_HOUR_COL = "solar_at_hour"
WIND_AT_HOUR_COL = "wind_at_hour"
NET_LOAD_AT_HOUR_COL = "net_load_at_hour"
TEMP_AT_HOUR_COL = "temp_at_hour"
LOAD_RAMP_1H_AT_HOUR_COL = "load_ramp_1h_at_hour"
LOAD_RAMP_3H_AT_HOUR_COL = "load_ramp_3h_at_hour"
LMP_AT_HOUR_COL = "lmp"

# Wide-stem → long-col mapping used by the melt step in
# ``_shared._melt_pool_to_long``. Stem here is the prefix BEFORE the
# ``_h{N}`` suffix on a wide col (e.g. ``load_ramp_1h_h7`` → stem
# ``load_ramp_1h``).
HOURLY_STEM_TO_LONG_COL: dict[str, str] = {
    "load": LOAD_AT_HOUR_COL,
    "solar": SOLAR_AT_HOUR_COL,
    "wind": WIND_AT_HOUR_COL,
    "net_load": NET_LOAD_AT_HOUR_COL,
    "temp": TEMP_AT_HOUR_COL,
    "load_ramp_1h": LOAD_RAMP_1H_AT_HOUR_COL,
    "load_ramp_3h": LOAD_RAMP_3H_AT_HOUR_COL,
    "lmp": LMP_AT_HOUR_COL,
}


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
    df: pd.DataFrame,
    value_col: str,
    output_prefix: str,
) -> pd.DataFrame:
    """Generic wide pivot: one col per HE, named ``{output_prefix}_h1..{output_prefix}_h24``."""
    out_cols = [f"{output_prefix}_h{h}" for h in range(1, 25)]
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["date"] + out_cols)
    work = df[["date", "hour_ending", value_col]].copy()
    work["date"] = _to_date(work["date"])
    work["hour_ending"] = pd.to_numeric(work["hour_ending"], errors="coerce").astype(
        "Int64"
    )
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=["date", "hour_ending", value_col])
    if len(work) == 0:
        return pd.DataFrame(columns=["date"] + out_cols)
    work["hour_ending"] = work["hour_ending"].astype(int)

    pivot = work.pivot_table(
        index="date",
        columns="hour_ending",
        values=value_col,
        aggfunc="mean",
    ).reindex(columns=range(1, 25))
    pivot = pivot.rename(columns={h: f"{output_prefix}_h{h}" for h in range(1, 25)})
    return pivot.reset_index()


# ── rto_load_profile ─────────────────────────────────────────────────────


def _build_rto_load_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    # Reads from the unified supply-demand coalescer so load shares a single
    # forecast-vs-RT decision per (region, date) with the sibling solar/wind/
    # net_load pool builders. Cross-series source consistency in the pool
    # eliminates vintage skew between sibling features.
    df = loader.load_pjm_supply_demand_coalesced(cache_dir=cache_dir, region=RTO)
    return _hourly_value_profile(df, "load_mw", output_prefix="load")


def _build_rto_load_profile_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    df = loader.load_load_forecast(cache_dir=cache_dir)
    df = df[df["region"].astype(str) == RTO].copy()
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    return _hourly_load_profile(df, "forecast_load_mw")


RTO_LOAD_PROFILE = FeatureDomain(
    name="rto_load_profile",
    description=(
        "RTO load — 24 hourly cols (load_h1..load_h24) as a single group. "
        "Time-of-day bucketing is redundant under per-HE windowed matching "
        "(``flt_radius`` already localizes the match), so the spec-side "
        "split was removed in favor of one ``load_level`` group; tune the "
        "single weight rather than five sub-bucket weights."
    ),
    feature_groups={
        "load_level": [LOAD_AT_HOUR_COL],
    },
    feature_group_weights={
        "load_level": 3,
    },
    pool_builder=_build_rto_load_profile_pool,
    query_builder=_build_rto_load_profile_query,
)


# ── solar_profile (per-HE level) ─────────────────────────────────────────
# Designed for per_hour matching: 24 hourly cols participate in the dynamic
# 3-hour window distance, parallel to rto_load_profile. Pool reads from the
# unified supply-demand coalescer so the (forecast | RT) decision is shared
# with load and wind on every (region, date). RT actuals fill pre-2019
# (and any partial-coverage) dates with no cross-series mixing risk.


def _build_solar_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_pjm_supply_demand_coalesced(cache_dir=cache_dir, region=RTO)
    return _hourly_value_profile(df, "solar_mw", output_prefix="solar")


def _build_solar_profile_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    df = loader.load_solar_forecast(cache_dir=cache_dir).copy()
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    val = "solar_forecast" if "solar_forecast" in df.columns else "solar_mw"
    return _hourly_value_profile(df, val, output_prefix="solar")


SOLAR_PROFILE = FeatureDomain(
    name="solar_profile",
    description="Solar — scalar ``solar_at_hour`` per (date, HE) row.",
    feature_groups={"solar_level": [SOLAR_AT_HOUR_COL]},
    feature_group_weights={"solar_level": 1.5},
    pool_builder=_build_solar_profile_pool,
    query_builder=_build_solar_profile_query,
)


# ── wind_profile (per-HE level) ──────────────────────────────────────────


def _build_wind_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    # Same single-source-decision reasoning as load and solar — see
    # _build_rto_load_profile_pool.
    df = loader.load_pjm_supply_demand_coalesced(cache_dir=cache_dir, region=RTO)
    return _hourly_value_profile(df, "wind_mw", output_prefix="wind")


def _build_wind_profile_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    df = loader.load_wind_forecast(cache_dir=cache_dir).copy()
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    val = "wind_forecast" if "wind_forecast" in df.columns else "wind_mw"
    return _hourly_value_profile(df, val, output_prefix="wind")


WIND_PROFILE = FeatureDomain(
    name="wind_profile",
    description="Wind — scalar ``wind_at_hour`` per (date, HE) row.",
    feature_groups={"wind_level": [WIND_AT_HOUR_COL]},
    feature_group_weights={"wind_level": 1.5},
    pool_builder=_build_wind_profile_pool,
    query_builder=_build_wind_profile_query,
)


# ── renewable_profile (combined solar+wind, sunny parity) ───────────────
# Both stems in a single ``renewable_level`` feature group. Sunny's
# ``renewable_at_hour_scalar`` does the same — solar and wind share one
# weight and one per-group distance, so the engine doesn't double-count
# renewable signal vs ours where solar/wind were separately weighted.
# Underlying cols still named solar_h*/wind_h* so existing windowed-
# stem registries (engine._WINDOWED_COL_STEMS) keep emitting them.


def _build_renewable_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_pjm_supply_demand_coalesced(cache_dir=cache_dir, region=RTO)
    solar_pool = _hourly_value_profile(df, "solar_mw", output_prefix="solar")
    wind_pool = _hourly_value_profile(df, "wind_mw", output_prefix="wind")
    return solar_pool.merge(wind_pool, on="date", how="outer")


def _build_renewable_profile_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    solar_df = loader.load_solar_forecast(cache_dir=cache_dir).copy()
    solar_df["date"] = _to_date(solar_df["date"])
    solar_df = solar_df[solar_df["date"] == target_date]
    val_s = "solar_forecast" if "solar_forecast" in solar_df.columns else "solar_mw"
    solar_q = _hourly_value_profile(solar_df, val_s, output_prefix="solar")

    wind_df = loader.load_wind_forecast(cache_dir=cache_dir).copy()
    wind_df["date"] = _to_date(wind_df["date"])
    wind_df = wind_df[wind_df["date"] == target_date]
    val_w = "wind_forecast" if "wind_forecast" in wind_df.columns else "wind_mw"
    wind_q = _hourly_value_profile(wind_df, val_w, output_prefix="wind")

    return solar_q.merge(wind_q, on="date", how="outer")


RENEWABLE_PROFILE = FeatureDomain(
    name="renewable_profile",
    description=(
        "Combined solar+wind — 48 hourly cols (solar_h1..solar_h24, "
        "wind_h1..wind_h24) as a single ``renewable_level`` group. "
        "Mirrors sunny's renewable_at_hour_scalar: both stems share one "
        "spec weight and one per-group distance, avoiding double-counting "
        "of renewable signal vs the split solar_profile + wind_profile."
    ),
    feature_groups={
        "renewable_level": [SOLAR_AT_HOUR_COL, WIND_AT_HOUR_COL],
    },
    feature_group_weights={"renewable_level": 1.5},
    pool_builder=_build_renewable_profile_pool,
    query_builder=_build_renewable_profile_query,
)


# ── rto_net_load_profile (per-HE level, identity-safe) ──────────────────
# Net load = load - solar - wind. Pool reads from the unified supply-demand
# coalescer so the four components share a single source decision per
# (region, date), preserving the identity by construction. Avoids the
# cross-source mixing artifact that breaks `load - solar - wind` when the
# per-series coalescers disagree on forecast-vs-RT (e.g. 2025-05-01).


def _build_rto_net_load_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_pjm_supply_demand_coalesced(cache_dir=cache_dir, region=RTO)
    return _hourly_value_profile(df, "net_load_mw", output_prefix="net_load")


def _build_rto_net_load_profile_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    df = loader.load_pjm_net_load_forecast(cache_dir=cache_dir).copy()
    df = df[df["region"].astype(str) == RTO]
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    if "as_of_date" in df.columns and len(df) > 0:
        df["as_of_date"] = _to_date(df["as_of_date"])
        delta = (
            pd.to_datetime(df["date"], errors="coerce")
            - pd.to_datetime(df["as_of_date"], errors="coerce")
        ).dt.days
        df = df[delta == 1]
    return _hourly_value_profile(df, "net_load_forecast_mw", output_prefix="net_load")


RTO_NET_LOAD_PROFILE = FeatureDomain(
    name="rto_net_load_profile",
    description=(
        "RTO net load (load - solar - wind), unified-source — 24 hourly cols "
        "(net_load_h1..net_load_h24) as a single group. Pool from the "
        "unified supply-demand coalescer; query from the DA-cutoff net-load "
        "forecast (lead_days=1). Time-of-day bucketing dropped in favor of "
        "one ``net_load_level`` group, mirroring rto_load_profile."
    ),
    feature_groups={
        "net_load_level": [NET_LOAD_AT_HOUR_COL],
    },
    # Mirrors RTO_LOAD_PROFILE so the two demand domains contribute
    # comparably when both are enabled. Sunny's net_load_at_hour=2.0.
    feature_group_weights={
        "net_load_level": 2,
    },
    pool_builder=_build_rto_net_load_profile_pool,
    query_builder=_build_rto_net_load_profile_query,
)


# ── outages_level (daily, broadcast across HEs) ──────────────────────────
# Outages MW are published daily by PJM — no hourly granularity exists.
# Daily-broadcast features bias which candidate dates rank high overall;
# they don't differentiate between HEs of a given candidate date.


def _outages_level_features(df_rto: pd.DataFrame) -> pd.DataFrame:
    if df_rto is None or len(df_rto) == 0:
        return pd.DataFrame(columns=["date"] + OUTAGE_LEVEL_COLS)
    date_col = "forecast_date" if "forecast_date" in df_rto.columns else "date"
    df = df_rto[[date_col, "total_outages_mw"]].copy()
    df = df.rename(columns={date_col: "date", "total_outages_mw": "outage_total_mw"})
    df["date"] = _to_date(df["date"])
    df["outage_total_mw"] = pd.to_numeric(df["outage_total_mw"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df[["date"] + OUTAGE_LEVEL_COLS]


def _build_outages_level_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_outages_forecast_history(cache_dir=cache_dir, lead_days=1)
    df = df[df["region"].astype(str) == RTO]
    return _outages_level_features(df)


def _build_outages_level_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    df = loader.load_outages_forecast_history(cache_dir=cache_dir, lead_days=1)
    df = df[df["region"].astype(str) == RTO].copy()
    date_col = "forecast_date" if "forecast_date" in df.columns else "date"
    df["date"] = _to_date(df[date_col])
    df = df[df["date"] == target_date]
    if len(df) == 0:
        empty = {"date": target_date, **{c: np.nan for c in OUTAGE_LEVEL_COLS}}
        return pd.DataFrame([empty])
    row = df.iloc[0]
    return pd.DataFrame(
        [
            {
                "date": target_date,
                "outage_total_mw": float(row.get("total_outages_mw", np.nan)),
            }
        ]
    )[["date"] + OUTAGE_LEVEL_COLS]


OUTAGES_LEVEL = FeatureDomain(
    name="outages_level",
    description=(
        "RTO outages — total MW only (sunny parity). The planned/forced "
        "split was dropped; sunny's outage_daily uses total only and the "
        "split was inflating outage's per-group sum-Euclidean distance "
        "(sqrt(3) vs sqrt(1)) without adding orthogonal signal."
    ),
    feature_groups={"outage_level": OUTAGE_LEVEL_COLS},
    feature_group_weights={"outage_level": 1.5},
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
    feature_group_weights={"gas_level": 2.0},
    pool_builder=_build_gas_level_pool,
    query_builder=_build_gas_level_query,
)


# ── load_ramps_profile (derived from rto_load_profile, windowed) ─────────
# Intra-day load ramps captured as a peer of the load level. Mirrors
# sunny's ``load_ramp_1h_at_hour`` and ``load_ramp_3h_at_hour`` features
# but at the wide-format level: we materialize 24 ramp values per date
# (one per HE), and the per-HE windowed Euclidean picks the relevant
# subset. HE=1 1h-ramp and HE<=3 3h-ramp are NaN (no in-day predecessor) —
# the engine's NaN-aware mask drops them from per-HE distance.


def _compute_load_ramps_wide(load_wide: pd.DataFrame) -> pd.DataFrame:
    out = load_wide[["date"]].copy()
    for h in range(1, 25):
        if h > 1:
            out[f"load_ramp_1h_h{h}"] = (
                load_wide[f"load_h{h}"] - load_wide[f"load_h{h - 1}"]
            )
        else:
            out[f"load_ramp_1h_h{h}"] = np.nan
        if h > 3:
            out[f"load_ramp_3h_h{h}"] = (
                load_wide[f"load_h{h}"] - load_wide[f"load_h{h - 3}"]
            )
        else:
            out[f"load_ramp_3h_h{h}"] = np.nan
    return out


def _build_load_ramps_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_pjm_supply_demand_coalesced(cache_dir=cache_dir, region=RTO)
    load = _hourly_value_profile(df, "load_mw", output_prefix="load")
    return _compute_load_ramps_wide(load)


def _build_load_ramps_profile_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    df = loader.load_load_forecast(cache_dir=cache_dir)
    df = df[df["region"].astype(str) == RTO].copy()
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    load = _hourly_load_profile(df, "forecast_load_mw")
    return _compute_load_ramps_wide(load)


LOAD_RAMPS_PROFILE = FeatureDomain(
    name="load_ramps_profile",
    description=(
        "Derived intra-day load ramps — load_ramp_1h_h{HE} = load_h{HE} - "
        "load_h{HE-1}, load_ramp_3h_h{HE} = load_h{HE} - load_h{HE-3}. "
        "Both ramps share a single ``load_ramps`` feature group (sunny "
        "parity — sunny's load_ramps_scalar combines load_ramp_1h_at_hour "
        "and load_ramp_3h_at_hour into one per-group distance). HE=1 1h "
        "ramp and HE<=3 3h ramp are NaN (no in-day predecessor)."
    ),
    feature_groups={
        "load_ramps": [LOAD_RAMP_1H_AT_HOUR_COL, LOAD_RAMP_3H_AT_HOUR_COL],
    },
    feature_group_weights={
        "load_ramps": 1.5,
    },
    pool_builder=_build_load_ramps_profile_pool,
    query_builder=_build_load_ramps_profile_query,
)


# ── temperature_profile (windowed) ───────────────────────────────────────
# RTO-wide hourly temperature from ``load_weather_coalesced`` (observed
# wins for dates with all 24 HEs present; forecast fills the rest,
# including the future target date). Mirrors sunny's weather_at_hour but
# at the wide-format level.


def _build_temperature_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_weather_coalesced(cache_dir=cache_dir)
    return _hourly_value_profile(df, "temp", output_prefix="temp")


def _build_temperature_profile_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    df = loader.load_weather_coalesced(cache_dir=cache_dir).copy()
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    return _hourly_value_profile(df, "temp", output_prefix="temp")


TEMPERATURE_PROFILE = FeatureDomain(
    name="temperature_profile",
    description=(
        "RTO-wide hourly temperature — 24 cols (temp_h1..temp_h24). "
        "Sourced from load_weather_coalesced (observed-first, forecast "
        "fallback) so historical pool uses observed actuals and the "
        "query for a future date uses the forecast vintage."
    ),
    feature_groups={"temp_level": [TEMP_AT_HOUR_COL]},
    feature_group_weights={"temp_level": 2.0},
    pool_builder=_build_temperature_profile_pool,
    query_builder=_build_temperature_profile_query,
)


# ── calendar_level (broadcast, derived from date) ────────────────────────
# Soft DOW similarity in the distance metric — pairs with
# FILTER_SAME_DOW_GROUP=False to keep day-of-week signal in the model
# without hard filtering. Mirrors sunny's calendar group.


def _calendar_features_from_date(d: date) -> dict[str, float]:
    weekday_mon0 = d.weekday()  # Mon=0..Sun=6 (Python)
    dow_num = (weekday_mon0 + 1) % 7  # Sun=0..Sat=6 (PJM/sunny convention)
    return {
        "dow_sin": float(math.sin(2 * math.pi * dow_num / 7.0)),
        "dow_cos": float(math.cos(2 * math.pi * dow_num / 7.0)),
        "is_weekend": 1.0 if dow_num in (0, 6) else 0.0,
    }


def _build_calendar_level_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_pjm_dates_daily(cache_dir=cache_dir)
    if df is None or len(df) == 0 or "date" not in df.columns:
        return pd.DataFrame(columns=["date"] + CALENDAR_LEVEL_COLS)
    work = df[["date"]].copy()
    work["date"] = _to_date(work["date"])
    work = work.dropna(subset=["date"]).drop_duplicates("date").reset_index(drop=True)
    feats = pd.DataFrame(
        [_calendar_features_from_date(d) for d in work["date"]],
        columns=CALENDAR_LEVEL_COLS,
    )
    return pd.concat([work, feats], axis=1)[["date"] + CALENDAR_LEVEL_COLS]


def _build_calendar_level_query(
    target_date: date, cache_dir: Path | None
) -> pd.DataFrame:
    feats = _calendar_features_from_date(target_date)
    return pd.DataFrame([{"date": target_date, **feats}])[
        ["date"] + CALENDAR_LEVEL_COLS
    ]


CALENDAR_LEVEL = FeatureDomain(
    name="calendar_level",
    description=(
        "Calendar features (dow_sin, dow_cos, is_weekend) derived from the "
        "date. Broadcast across HEs (single value per date). Soft DOW "
        "similarity in the distance metric — pairs with "
        "FILTER_SAME_DOW_GROUP=False default to keep day-of-week signal "
        "without hard filtering."
    ),
    feature_groups={"calendar_level": CALENDAR_LEVEL_COLS},
    feature_group_weights={"calendar_level": 1.0},
    pool_builder=_build_calendar_level_pool,
    query_builder=_build_calendar_level_query,
)


# ── Registry ─────────────────────────────────────────────────────────────

DOMAIN_REGISTRY: dict[str, FeatureDomain] = {
    RTO_LOAD_PROFILE.name: RTO_LOAD_PROFILE,
    LOAD_RAMPS_PROFILE.name: LOAD_RAMPS_PROFILE,
    SOLAR_PROFILE.name: SOLAR_PROFILE,
    WIND_PROFILE.name: WIND_PROFILE,
    RENEWABLE_PROFILE.name: RENEWABLE_PROFILE,
    RTO_NET_LOAD_PROFILE.name: RTO_NET_LOAD_PROFILE,
    TEMPERATURE_PROFILE.name: TEMPERATURE_PROFILE,
    OUTAGES_LEVEL.name: OUTAGES_LEVEL,
    GAS_LEVEL.name: GAS_LEVEL,
    CALENDAR_LEVEL.name: CALENDAR_LEVEL,
}


def resolved_feature_groups(domain_names: tuple[str, ...]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for n in domain_names:
        out.update(DOMAIN_REGISTRY[n].feature_groups)
    return out


def resolved_raw_feature_group_weights(
    domain_names: tuple[str, ...],
) -> dict[str, float]:
    """Sum each domain's group weights without renormalization."""
    raw: dict[str, float] = {}
    for n in domain_names:
        raw.update(DOMAIN_REGISTRY[n].feature_group_weights)
    return raw


def resolved_feature_group_weights(domain_names: tuple[str, ...]) -> dict[str, float]:
    """Sum each domain's group weights, then renormalize so total = 1.0."""
    raw = resolved_raw_feature_group_weights(domain_names)
    total = sum(raw.values())
    if total <= 0:
        return raw
    return {k: v / total for k, v in raw.items()}


def feature_group_weight_locations() -> dict[str, tuple[str, int]]:
    """Map each feature-group name to the (file, line) of its weight literal.

    Parses this module's source to find every ``FeatureDomain(...)``
    construction and returns the line number of each key in its
    ``feature_group_weights={...}`` dict literal — i.e. the exact line
    where you'd edit the raw weight.
    """
    import ast as _ast

    src_file = __file__
    with open(src_file, encoding="utf-8") as f:
        tree = _ast.parse(f.read())
    out: dict[str, tuple[str, int]] = {}
    for node in _ast.walk(tree):
        if not (
            isinstance(node, _ast.Call)
            and isinstance(node.func, _ast.Name)
            and node.func.id == "FeatureDomain"
        ):
            continue
        for kw in node.keywords:
            if kw.arg != "feature_group_weights" or not isinstance(kw.value, _ast.Dict):
                continue
            for k in kw.value.keys:
                if isinstance(k, _ast.Constant) and isinstance(k.value, str):
                    out[k.value] = (src_file, k.lineno)
    return out


def all_feature_cols(domain_names: tuple[str, ...]) -> list[str]:
    seen: list[str] = []
    for n in domain_names:
        for c in DOMAIN_REGISTRY[n].feature_cols:
            if c not in seen:
                seen.append(c)
    return seen
