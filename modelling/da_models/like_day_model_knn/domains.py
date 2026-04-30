"""Feature-domain plugins for like_day_model_knn.

A domain produces a pool feature frame (one row per historical delivery date)
and a query feature frame (one row for the target delivery date), both keyed
by ``date``. The variant builders concatenate domains by inner-joining on
``date`` and then optionally broadcast across ``hour_ending`` for ``per_hour``.

Asymmetry by design: pool features come from actuals (deep history),
query features come from forecasts (operational reality at decision time).
Both share the same column names so the engine treats them uniformly.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from da_models.common.data import loader
from da_models.common.data.loader import _resolve_cache_dir

# Raw parquet filenames for cases where the normalizer drops fields we need
# (forecast_rank for solar/wind) or remaps hub names in inconvenient ways.
_RAW_SOLAR_FORECAST = "pjm_solar_forecast_hourly_da_cutoff.parquet"
_RAW_WIND_FORECAST = "pjm_wind_forecast_hourly_da_cutoff.parquet"

# Region used everywhere a region filter applies.
RTO = "RTO"

# Column names produced per domain (kept stable so engine/spec can reference).
LOAD_HOURLY_COLS = [f"load_h{h}" for h in range(1, 25)]
LOAD_SUMMARY_COLS = [
    "load_daily_avg", "load_daily_peak", "load_daily_valley", "load_peak_ratio",
    "load_morning_ramp", "load_evening_ramp", "load_ramp_max",
]
RENEWABLE_COLS = [
    "solar_daily_avg", "solar_daily_max", "wind_daily_avg", "wind_daily_max",
    "renewable_daily_avg", "renewable_daily_max",
    "solar_peak_concentration", "wind_intraday_std",
]
OUTAGE_COLS = [
    "outage_total_mw", "outage_forced_mw",
    "outage_planned_mw", "outage_maintenance_mw", "outage_forced_share",
    "outage_total_7d_mean", "outage_total_daily_change",
]
GAS_COLS = [
    "gas_m3_avg", "gas_m3_max", "gas_m3_intraday_range",
    "gas_m3_onpeak_avg", "gas_m3_offpeak_avg", "gas_m3_morning_ramp",
    "gas_basis_m3_dom_south", "gas_basis_tz6_m3", "gas_basis_tco_m3",
    "gas_m3_7d_rolling",
]


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

def _resolve_path(cache_dir: Path | None, name: str) -> Path:
    return _resolve_cache_dir(cache_dir) / name


def _to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.date


def _hourly_load_aggregations(
    df: pd.DataFrame, value_col: str,
) -> pd.DataFrame:
    """Daily summaries (7) from an hourly (date, hour_ending, value) frame."""
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["date"] + LOAD_SUMMARY_COLS)

    work = df[["date", "hour_ending", value_col]].copy()
    work["date"] = _to_date(work["date"])
    work["hour_ending"] = pd.to_numeric(work["hour_ending"], errors="coerce").astype("Int64")
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=["date", "hour_ending", value_col])
    if len(work) == 0:
        return pd.DataFrame(columns=["date"] + LOAD_SUMMARY_COLS)
    work["hour_ending"] = work["hour_ending"].astype(int)
    work = work.sort_values(["date", "hour_ending"])

    daily = work.groupby("date").agg(
        load_daily_avg=(value_col, "mean"),
        load_daily_peak=(value_col, "max"),
        load_daily_valley=(value_col, "min"),
    ).reset_index()
    daily["load_peak_ratio"] = daily["load_daily_peak"] / daily["load_daily_avg"]

    work["ramp"] = work.groupby("date")[value_col].diff()
    daily = daily.merge(
        work.groupby("date", as_index=False)["ramp"].max()
        .rename(columns={"ramp": "load_ramp_max"}),
        on="date", how="left",
    )

    pivot = work.pivot_table(
        index="date", columns="hour_ending", values=value_col, aggfunc="mean",
    )
    he5 = pivot.get(5)
    he8 = pivot.get(8)
    he15 = pivot.get(15)
    he20 = pivot.get(20)
    daily["load_morning_ramp"] = (he8 - he5).reindex(daily["date"]).to_numpy() if he5 is not None and he8 is not None else np.nan
    daily["load_evening_ramp"] = (he20 - he15).reindex(daily["date"]).to_numpy() if he15 is not None and he20 is not None else np.nan
    return daily[["date"] + LOAD_SUMMARY_COLS]


def _hourly_load_profile(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Wide pivot of hourly load: one col per HE, named load_h1..load_h24."""
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["date"] + LOAD_HOURLY_COLS)
    work = df[["date", "hour_ending", value_col]].copy()
    work["date"] = _to_date(work["date"])
    work["hour_ending"] = pd.to_numeric(work["hour_ending"], errors="coerce").astype("Int64")
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=["date", "hour_ending", value_col])
    if len(work) == 0:
        return pd.DataFrame(columns=["date"] + LOAD_HOURLY_COLS)
    work["hour_ending"] = work["hour_ending"].astype(int)

    pivot = work.pivot_table(
        index="date", columns="hour_ending", values=value_col, aggfunc="mean",
    ).reindex(columns=range(1, 25))
    pivot = pivot.rename(columns={h: f"load_h{h}" for h in range(1, 25)})
    return pivot.reset_index()


# ── rto_load_summary ─────────────────────────────────────────────────────

def _build_rto_load_summary_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_load_rt(cache_dir=cache_dir)
    df = df[df["region"].astype(str) == RTO]
    return _hourly_load_aggregations(df, "rt_load_mw")


def _build_rto_load_summary_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_load_forecast(cache_dir=cache_dir)
    df = df[df["region"].astype(str) == RTO].copy()
    df["date"] = _to_date(df["date"])
    df = df[df["date"] == target_date]
    return _hourly_load_aggregations(df, "forecast_load_mw")


RTO_LOAD_SUMMARY = FeatureDomain(
    name="rto_load_summary",
    description="RTO load — 7 daily summaries (level + ramps).",
    feature_groups={
        "load_level": ["load_daily_avg", "load_daily_peak", "load_daily_valley", "load_peak_ratio"],
        "load_ramps": ["load_morning_ramp", "load_evening_ramp", "load_ramp_max"],
    },
    feature_group_weights={"load_level": 1.0, "load_ramps": 1.5},
    pool_builder=_build_rto_load_summary_pool,
    query_builder=_build_rto_load_summary_query,
)


# ── rto_load_profile ─────────────────────────────────────────────────────

def _build_rto_load_profile_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_load_rt(cache_dir=cache_dir)
    df = df[df["region"].astype(str) == RTO]
    return _hourly_load_profile(df, "rt_load_mw")


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


# ── renewables ───────────────────────────────────────────────────────────

PEAK_HOURS = list(range(10, 17))  # HE10..HE16


def _renewable_daily_features(
    df_solar: pd.DataFrame, df_wind: pd.DataFrame,
) -> pd.DataFrame:
    """8 daily renewable features from hourly solar and wind series.

    Both inputs must have ``date``, ``hour_ending``, and a single value col
    named ``solar`` / ``wind`` respectively.
    """
    if (df_solar is None or len(df_solar) == 0) and (df_wind is None or len(df_wind) == 0):
        return pd.DataFrame(columns=["date"] + RENEWABLE_COLS)

    def _prep(df: pd.DataFrame, val: str) -> pd.DataFrame:
        if df is None or len(df) == 0:
            return pd.DataFrame(columns=["date", "hour_ending", val])
        out = df[["date", "hour_ending", val]].copy()
        out["date"] = _to_date(out["date"])
        out["hour_ending"] = pd.to_numeric(out["hour_ending"], errors="coerce").astype("Int64")
        out[val] = pd.to_numeric(out[val], errors="coerce")
        return out.dropna(subset=["date", "hour_ending", val])

    s = _prep(df_solar, "solar")
    w = _prep(df_wind, "wind")

    s_daily = s.groupby("date").agg(
        solar_daily_avg=("solar", "mean"),
        solar_daily_max=("solar", "max"),
    ).reset_index() if len(s) else pd.DataFrame(columns=["date", "solar_daily_avg", "solar_daily_max"])

    if len(s):
        peak_mask = s["hour_ending"].isin(PEAK_HOURS)
        peak_sum = s[peak_mask].groupby("date")["solar"].sum()
        total_sum = s.groupby("date")["solar"].sum()
        with np.errstate(invalid="ignore", divide="ignore"):
            conc = (peak_sum / total_sum.where(total_sum > 0)).rename("solar_peak_concentration")
        s_daily = s_daily.merge(conc.reset_index(), on="date", how="left")
    else:
        s_daily["solar_peak_concentration"] = np.nan

    w_daily = w.groupby("date").agg(
        wind_daily_avg=("wind", "mean"),
        wind_daily_max=("wind", "max"),
        wind_intraday_std=("wind", "std"),
    ).reset_index() if len(w) else pd.DataFrame(
        columns=["date", "wind_daily_avg", "wind_daily_max", "wind_intraday_std"]
    )

    daily = s_daily.merge(w_daily, on="date", how="outer")
    daily["renewable_daily_avg"] = daily.get("solar_daily_avg", 0).fillna(0) + daily.get("wind_daily_avg", 0).fillna(0)
    daily["renewable_daily_max"] = daily[["solar_daily_max", "wind_daily_max"]].max(axis=1)
    return daily[["date"] + RENEWABLE_COLS]


def _build_renewables_pool(cache_dir: Path | None) -> pd.DataFrame:
    fm = loader.load_fuel_mix(cache_dir=cache_dir)
    if fm is None or len(fm) == 0:
        return pd.DataFrame(columns=["date"] + RENEWABLE_COLS)
    df_solar = fm[["date", "hour_ending", "solar"]].copy()
    df_wind = fm[["date", "hour_ending", "wind"]].copy()
    return _renewable_daily_features(df_solar, df_wind)


def _read_raw_forecast(name: str, cache_dir: Path | None, value_col: str, target_date: date) -> pd.DataFrame:
    """Read a forecast parquet directly so we can filter on forecast_rank=1
    (the normalized loaders drop that field for solar/wind)."""
    path = _resolve_path(cache_dir, name)
    if not path.exists():
        return pd.DataFrame(columns=["date", "hour_ending", value_col])
    raw = pd.read_parquet(path)
    if "forecast_rank" in raw.columns:
        raw = raw[raw["forecast_rank"] == 1]
    if "forecast_date" in raw.columns:
        raw["date"] = _to_date(raw["forecast_date"])
    elif "date" in raw.columns:
        raw["date"] = _to_date(raw["date"])
    else:
        return pd.DataFrame(columns=["date", "hour_ending", value_col])
    raw = raw[raw["date"] == target_date]
    return raw[["date", "hour_ending", value_col]].copy()


def _build_renewables_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    s = _read_raw_forecast(_RAW_SOLAR_FORECAST, cache_dir, "solar_forecast", target_date)
    w = _read_raw_forecast(_RAW_WIND_FORECAST, cache_dir, "wind_forecast", target_date)
    s = s.rename(columns={"solar_forecast": "solar"})
    w = w.rename(columns={"wind_forecast": "wind"})
    return _renewable_daily_features(s, w)


RENEWABLES = FeatureDomain(
    name="renewables",
    description="Solar + wind — 8 daily features (level + intraday shape).",
    feature_groups={
        "renewable_level": [
            "solar_daily_avg", "solar_daily_max",
            "wind_daily_avg", "wind_daily_max",
            "renewable_daily_avg", "renewable_daily_max",
        ],
        "renewable_shape": ["solar_peak_concentration", "wind_intraday_std"],
    },
    feature_group_weights={"renewable_level": 0.5, "renewable_shape": 0.25},
    pool_builder=_build_renewables_pool,
    query_builder=_build_renewables_query,
)


# ── outages ──────────────────────────────────────────────────────────────

def _outage_features_from_actuals(df_actual_rto: pd.DataFrame) -> pd.DataFrame:
    """Build all 7 outage features from a date-keyed RTO actuals frame.

    The trend features (7d mean, daily change) are computed as backward
    rolling stats over the actuals series and used for both pool and query.
    """
    if df_actual_rto is None or len(df_actual_rto) == 0:
        return pd.DataFrame(columns=["date"] + OUTAGE_COLS)

    df = df_actual_rto[[
        "date", "total_outages_mw", "planned_outages_mw",
        "maintenance_outages_mw", "forced_outages_mw",
    ]].copy()
    df["date"] = _to_date(df["date"])
    for c in ["total_outages_mw", "planned_outages_mw", "maintenance_outages_mw", "forced_outages_mw"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    df = df.rename(columns={
        "total_outages_mw": "outage_total_mw",
        "planned_outages_mw": "outage_planned_mw",
        "maintenance_outages_mw": "outage_maintenance_mw",
        "forced_outages_mw": "outage_forced_mw",
    })
    with np.errstate(invalid="ignore", divide="ignore"):
        df["outage_forced_share"] = df["outage_forced_mw"] / df["outage_total_mw"].where(df["outage_total_mw"] > 0)
    # Backward 7-day mean (excluding self) and day-over-day change.
    df["outage_total_7d_mean"] = df["outage_total_mw"].shift(1).rolling(7, min_periods=1).mean()
    df["outage_total_daily_change"] = df["outage_total_mw"].diff()
    return df[["date"] + OUTAGE_COLS]


def _build_outages_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_outages_actual(cache_dir=cache_dir)
    df = df[df["region"].astype(str) == RTO]
    return _outage_features_from_actuals(df)


def _build_outages_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    """Query: level cols come from forecast for target_date; trend cols come
    from actuals leading up to target_date (forecast file has no history)."""
    fc = loader.load_outages_forecast(cache_dir=cache_dir)
    fc = fc[fc["region"].astype(str) == RTO].copy()
    if "forecast_rank" in fc.columns:
        fc = fc[fc["forecast_rank"] == 1]
    fc["date"] = _to_date(fc["date"])
    fc = fc[fc["date"] == target_date]
    if len(fc) == 0:
        empty = {"date": target_date, **{c: np.nan for c in OUTAGE_COLS}}
        return pd.DataFrame([empty])
    row = fc.iloc[0]
    out = {
        "date": target_date,
        "outage_total_mw": float(row.get("total_outages_mw", np.nan)),
        "outage_planned_mw": float(row.get("planned_outages_mw", np.nan)),
        "outage_maintenance_mw": float(row.get("maintenance_outages_mw", np.nan)),
        "outage_forced_mw": float(row.get("forced_outages_mw", np.nan)),
    }
    total = out["outage_total_mw"]
    out["outage_forced_share"] = (out["outage_forced_mw"] / total) if total and total > 0 else np.nan

    # Trend cols: backward 7d mean and day-over-day change from actuals
    # leading into target_date.
    actuals = loader.load_outages_actual(cache_dir=cache_dir)
    actuals = actuals[actuals["region"].astype(str) == RTO].copy()
    actuals["date"] = _to_date(actuals["date"])
    cutoff_lo = target_date - timedelta(days=8)
    prior = actuals[(actuals["date"] >= cutoff_lo) & (actuals["date"] < target_date)].sort_values("date")
    if len(prior) >= 1:
        last7 = prior.tail(7)["total_outages_mw"]
        out["outage_total_7d_mean"] = float(pd.to_numeric(last7, errors="coerce").mean())
        if len(prior) >= 1 and not prior.empty:
            last = float(prior["total_outages_mw"].iloc[-1])
            out["outage_total_daily_change"] = total - last if pd.notna(total) and pd.notna(last) else np.nan
        else:
            out["outage_total_daily_change"] = np.nan
    else:
        out["outage_total_7d_mean"] = np.nan
        out["outage_total_daily_change"] = np.nan
    return pd.DataFrame([out])[["date"] + OUTAGE_COLS]


OUTAGES = FeatureDomain(
    name="outages",
    description="RTO outages — 7 features (level + composition + trend).",
    feature_groups={
        "outage_level": ["outage_total_mw", "outage_forced_mw"],
        "outage_composition": ["outage_planned_mw", "outage_maintenance_mw", "outage_forced_share"],
        "outage_trend": ["outage_total_7d_mean", "outage_total_daily_change"],
    },
    feature_group_weights={"outage_level": 4.0, "outage_composition": 2.0, "outage_trend": 0.5},
    pool_builder=_build_outages_pool,
    query_builder=_build_outages_query,
)


# ── gas ──────────────────────────────────────────────────────────────────

ON_PEAK_HOURS = list(range(8, 24))  # HE8..HE23
OFF_PEAK_HOURS = [1, 2, 3, 4, 5, 6, 7, 24]


def _gas_daily_features(df: pd.DataFrame) -> pd.DataFrame:
    """10 daily gas features from the normalized hourly gas frame.

    The normalized loader renames hub cols to ``gas_m3``, ``gas_tco``,
    ``gas_tz6``, ``gas_dom_south``.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=["date"] + GAS_COLS)

    keep = [c for c in ["date", "hour_ending", "gas_m3", "gas_tco", "gas_tz6", "gas_dom_south"] if c in df.columns]
    work = df[keep].copy()
    work["date"] = _to_date(work["date"])
    work["hour_ending"] = pd.to_numeric(work["hour_ending"], errors="coerce").astype("Int64")
    for c in ["gas_m3", "gas_tco", "gas_tz6", "gas_dom_south"]:
        if c in work.columns:
            work[c] = pd.to_numeric(work[c], errors="coerce")
    work = work.dropna(subset=["date", "hour_ending", "gas_m3"])
    if len(work) == 0:
        return pd.DataFrame(columns=["date"] + GAS_COLS)
    work["hour_ending"] = work["hour_ending"].astype(int)

    daily = work.groupby("date").agg(
        gas_m3_avg=("gas_m3", "mean"),
        gas_m3_max=("gas_m3", "max"),
        gas_m3_min=("gas_m3", "min"),
    ).reset_index()
    daily["gas_m3_intraday_range"] = daily["gas_m3_max"] - daily["gas_m3_min"]
    daily = daily.drop(columns=["gas_m3_min"])

    onpeak = work[work["hour_ending"].isin(ON_PEAK_HOURS)].groupby("date")["gas_m3"].mean().rename("gas_m3_onpeak_avg")
    offpeak = work[work["hour_ending"].isin(OFF_PEAK_HOURS)].groupby("date")["gas_m3"].mean().rename("gas_m3_offpeak_avg")
    daily = daily.merge(onpeak.reset_index(), on="date", how="left")
    daily = daily.merge(offpeak.reset_index(), on="date", how="left")

    pivot = work.pivot_table(index="date", columns="hour_ending", values="gas_m3", aggfunc="mean")
    he5 = pivot.get(5)
    he8 = pivot.get(8)
    if he5 is not None and he8 is not None:
        ramp = (he8 - he5).rename("gas_m3_morning_ramp").reset_index()
        daily = daily.merge(ramp, on="date", how="left")
    else:
        daily["gas_m3_morning_ramp"] = np.nan

    def _avg(col: str) -> pd.Series:
        if col not in work.columns:
            return pd.Series(dtype=float)
        return work.groupby("date")[col].mean()

    m3 = _avg("gas_m3")
    dom = _avg("gas_dom_south")
    tz6 = _avg("gas_tz6")
    tco = _avg("gas_tco")
    basis = pd.DataFrame({
        "gas_basis_m3_dom_south": (m3 - dom),
        "gas_basis_tz6_m3": (tz6 - m3),
        "gas_basis_tco_m3": (tco - m3),
    }).reset_index()
    daily = daily.merge(basis, on="date", how="left")

    daily = daily.sort_values("date").reset_index(drop=True)
    daily["gas_m3_7d_rolling"] = daily["gas_m3_avg"].shift(1).rolling(7, min_periods=1).mean()

    return daily[["date"] + GAS_COLS]


def _build_gas_pool(cache_dir: Path | None) -> pd.DataFrame:
    df = loader._load_dataset("gas_prices_hourly", cache_dir=cache_dir)
    return _gas_daily_features(df)


def _build_gas_query(target_date: date, cache_dir: Path | None) -> pd.DataFrame:
    df = loader._load_dataset("gas_prices_hourly", cache_dir=cache_dir)
    daily = _gas_daily_features(df)
    return daily[daily["date"] == target_date]


GAS = FeatureDomain(
    name="gas",
    description="Gas (M3 primary + basis) — 10 daily features.",
    feature_groups={
        "gas_level": ["gas_m3_avg", "gas_m3_max", "gas_m3_intraday_range"],
        "gas_shape": ["gas_m3_onpeak_avg", "gas_m3_offpeak_avg", "gas_m3_morning_ramp"],
        "gas_basis": ["gas_basis_m3_dom_south", "gas_basis_tz6_m3", "gas_basis_tco_m3"],
        "gas_trend": ["gas_m3_7d_rolling"],
    },
    feature_group_weights={"gas_level": 2.0, "gas_shape": 1.5, "gas_basis": 1.0, "gas_trend": 0.5},
    pool_builder=_build_gas_pool,
    query_builder=_build_gas_query,
)


# ── Registry ─────────────────────────────────────────────────────────────

DOMAIN_REGISTRY: dict[str, FeatureDomain] = {
    RTO_LOAD_SUMMARY.name: RTO_LOAD_SUMMARY,
    RTO_LOAD_PROFILE.name: RTO_LOAD_PROFILE,
    RENEWABLES.name: RENEWABLES,
    OUTAGES.name: OUTAGES,
    GAS.name: GAS,
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
