"""
PJM hub price ramp-indicator backtest.

Tests ~150 indicators (heavily focused on solar / wind / net-load ramps,
broken down by region) against next-day on-peak DA LMPs at five PJM hubs:
Western, Eastern, AEP-Dayton, N.Illinois, Dominion.

Data window: 2020-01-01 -> latest fuel_mix date.

Outputs: modelling/backtests/output/hub_ramp_backtest_<YYYY-MM-DD>.html
"""
from __future__ import annotations

import sys
from datetime import date as Date
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

_MODELLING_ROOT = Path(__file__).resolve().parent.parent
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

CACHE_DIR = _MODELLING_ROOT / "data" / "cache"
OUTPUT_DIR = Path(__file__).parent / "output"
START_DATE = pd.Timestamp("2020-01-01")

TARGET_HUBS = [
    ("WESTERN HUB", "western"),
    ("EASTERN HUB", "eastern"),
    ("AEP-DAYTON HUB", "aep_dayton"),
    ("N ILLINOIS HUB", "n_illinois"),
    ("DOMINION HUB", "dominion"),
]
LOAD_REGIONS = ["RTO", "MIDATL", "WEST", "SOUTH"]

# Standard PJM on-peak: HE 8-23 inclusive, weekday non-NERC-holidays
ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = [h for h in range(1, 25) if h not in ONPEAK_HOURS]
MORNING_HOURS = [5, 6, 7, 8, 9]
EVENING_HOURS = [15, 16, 17, 18, 19, 20, 21]
SUPER_PEAK_HOURS = [17, 18, 19, 20]
MIDDAY_HOURS = [11, 12, 13, 14]
OVERNIGHT_HOURS = [1, 2, 3, 4, 5]


# ============================================================================
# DATA LOADING
# ============================================================================

def _read(p: str, columns=None) -> pd.DataFrame:
    return pd.read_parquet(CACHE_DIR / p, columns=columns)


def load_lmps_wide() -> pd.DataFrame:
    """Hourly LMPs pivoted: rows=(date,he), cols={market}_{hub}_{lmp/cong}."""
    df = _read(
        "pjm_lmps_hourly.parquet",
        columns=["date", "hour_ending", "hub", "market", "lmp_total", "lmp_congestion_price"],
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= START_DATE]
    df = df[df["hub"].isin([h for h, _ in TARGET_HUBS])]
    df["hour_ending"] = df["hour_ending"].astype(int)
    hub_slug = {h: s for h, s in TARGET_HUBS}
    df["hub_slug"] = df["hub"].map(hub_slug)

    # Build wide: one row per (date, he), columns for market x hub
    out = df.pivot_table(
        index=["date", "hour_ending"],
        columns=["market", "hub_slug"],
        values=["lmp_total", "lmp_congestion_price"],
        aggfunc="mean",
    )
    out.columns = [f"{val}_{mkt}_{hub}" for val, mkt, hub in out.columns]
    out = out.reset_index()
    return out


def load_fuel_mix() -> pd.DataFrame:
    df = _read("pjm_fuel_mix_hourly.parquet")
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= START_DATE].copy()
    df["hour_ending"] = df["hour_ending"].astype(int)
    keep = ["date", "hour_ending", "coal", "gas", "nuclear", "wind", "solar",
            "hydro", "oil", "storage", "total", "thermal", "renewables"]
    return df[keep]


def load_load_wide() -> pd.DataFrame:
    df = _read(
        "pjm_load_rt_hourly.parquet",
        columns=["date", "hour_ending", "region", "rt_load_mw"],
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= START_DATE]
    df = df[df["region"].isin(LOAD_REGIONS)]
    df["hour_ending"] = df["hour_ending"].astype(int)
    out = df.pivot_table(index=["date", "hour_ending"], columns="region", values="rt_load_mw")
    out.columns = [f"load_{c.lower()}" for c in out.columns]
    return out.reset_index()


def load_weather_daily() -> pd.DataFrame:
    """Aggregate WSI hourly observed temp to daily PJM-wide statistics."""
    df = _read(
        "wsi_pjm_hourly_observed_temp.parquet",
        columns=["date", "hour_ending", "temperature"],
    )
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= START_DATE]
    # Average across stations within each (date, hour)
    pjm_hourly = df.groupby(["date", "hour_ending"])["temperature"].mean().reset_index()
    daily = pjm_hourly.groupby("date").agg(
        temp_mean=("temperature", "mean"),
        temp_max=("temperature", "max"),
        temp_min=("temperature", "min"),
    ).reset_index()
    daily["temp_range"] = daily["temp_max"] - daily["temp_min"]
    daily["hdd"] = (65 - daily["temp_mean"]).clip(lower=0)
    daily["cdd"] = (daily["temp_mean"] - 65).clip(lower=0)
    daily["temp_dod_chg"] = daily["temp_mean"].diff()
    return daily


def load_gas_daily() -> pd.DataFrame:
    df = _read("ice_python_next_day_gas_hourly.parquet")
    df["gas_day"] = pd.to_datetime(df["gas_day"])
    df = df[df["gas_day"] >= START_DATE]
    cash_cols = [c for c in df.columns if c.endswith("_cash")]
    daily = df.groupby("gas_day")[cash_cols].mean().reset_index()
    daily = daily.rename(columns={"gas_day": "date"})
    # Basis spreads (use Tetco M3 as proxy for Henry Hub since hh_cash isn't here)
    if "tetco_m3_cash" in daily and "dominion_south_cash" in daily:
        daily["gas_basis_dom_m3"] = daily["dominion_south_cash"] - daily["tetco_m3_cash"]
    if "transco_z6_ny_cash" in daily and "tetco_m3_cash" in daily:
        daily["gas_basis_z6_m3"] = daily["transco_z6_ny_cash"] - daily["tetco_m3_cash"]
    if "transco_leidy_cash" in daily and "tetco_m3_cash" in daily:
        daily["gas_basis_leidy_m3"] = daily["transco_leidy_cash"] - daily["tetco_m3_cash"]
    return daily


def load_outages_daily() -> pd.DataFrame:
    df = _read("pjm_outages_actual_daily.parquet")
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= START_DATE]
    out = df.pivot_table(
        index="date", columns="region",
        values=["total_outages_mw", "planned_outages_mw", "forced_outages_mw"],
    )
    out.columns = [f"{val}_{reg.lower()}" for val, reg in out.columns]
    return out.reset_index()


def load_calendar() -> pd.DataFrame:
    df = _read("pjm_dates_daily.parquet")
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["date"] >= START_DATE]
    return df[["date", "month", "day_of_week_number", "is_weekend",
               "is_nerc_holiday", "is_federal_holiday", "summer_winter"]]


# ============================================================================
# RAMP FEATURE BUILDERS (vectorized)
# ============================================================================

def _wide(hourly: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Pivot to wide: rows=date, cols=HE 1..24, values=value_col."""
    return hourly.pivot_table(
        index="date", columns="hour_ending", values=value_col, aggfunc="mean"
    ).reindex(columns=range(1, 25))


def ramp_features(wide: pd.DataFrame, prefix: str, *, include_specific=True) -> pd.DataFrame:
    """Vectorized per-day ramp/aggregation features for a wide hourly series."""
    out = pd.DataFrame(index=wide.index)

    onpeak = [c for c in ONPEAK_HOURS if c in wide.columns]
    offpeak = [c for c in OFFPEAK_HOURS if c in wide.columns]
    morning = [c for c in MORNING_HOURS if c in wide.columns]
    evening = [c for c in EVENING_HOURS if c in wide.columns]
    super_peak = [c for c in SUPER_PEAK_HOURS if c in wide.columns]
    midday = [c for c in MIDDAY_HOURS if c in wide.columns]
    overnight = [c for c in OVERNIGHT_HOURS if c in wide.columns]

    out[f"{prefix}_max"] = wide.max(axis=1)
    out[f"{prefix}_min"] = wide.min(axis=1)
    out[f"{prefix}_avg"] = wide.mean(axis=1)
    out[f"{prefix}_std"] = wide.std(axis=1)
    out[f"{prefix}_range"] = out[f"{prefix}_max"] - out[f"{prefix}_min"]

    out[f"{prefix}_avg_onpeak"] = wide[onpeak].mean(axis=1)
    out[f"{prefix}_avg_offpeak"] = wide[offpeak].mean(axis=1)
    out[f"{prefix}_avg_super_peak"] = wide[super_peak].mean(axis=1)
    out[f"{prefix}_avg_midday"] = wide[midday].mean(axis=1)
    out[f"{prefix}_avg_overnight"] = wide[overnight].mean(axis=1)
    out[f"{prefix}_avg_morning"] = wide[morning].mean(axis=1)
    out[f"{prefix}_avg_evening"] = wide[evening].mean(axis=1)

    # Hourly delta (h - h-1)
    diff1 = wide.diff(axis=1)
    out[f"{prefix}_max_hourly_ramp_up"] = diff1.max(axis=1)
    out[f"{prefix}_max_hourly_ramp_down"] = diff1.min(axis=1)
    out[f"{prefix}_hourly_ramp_std"] = diff1.std(axis=1)

    # 3-hour delta
    diff3 = wide.diff(periods=3, axis=1)
    out[f"{prefix}_max_3hr_ramp_up"] = diff3.max(axis=1)
    out[f"{prefix}_max_3hr_ramp_down"] = diff3.min(axis=1)

    # 6-hour delta
    diff6 = wide.diff(periods=6, axis=1)
    out[f"{prefix}_max_6hr_ramp_up"] = diff6.max(axis=1)
    out[f"{prefix}_max_6hr_ramp_down"] = diff6.min(axis=1)

    # Specific named ramps (signed: positive = level rose)
    if include_specific:
        for (a, b, label) in [
            (5, 9, "morning_5_9"),
            (1, 8, "predawn_1_8"),
            (16, 20, "evening_16_20"),
            (15, 19, "evening_15_19"),
            (17, 20, "super_peak_17_20"),
            (13, 19, "valley_to_peak_13_19"),
            (11, 19, "midday_to_peak_11_19"),
            (19, 23, "post_peak_19_23"),
            (23, 5, "overnight_23_5"),  # next-day overnight: just a window comparison
        ]:
            if a in wide.columns and b in wide.columns:
                out[f"{prefix}_ramp_{label}"] = wide[b] - wide[a]

        # Specific hour values (often most predictive)
        for h in (8, 14, 17, 18, 19, 20):
            if h in wide.columns:
                out[f"{prefix}_he{h}"] = wide[h]

        # Peak / valley HOURS (categorical hour-of-day)
        out[f"{prefix}_peak_hour"] = wide.idxmax(axis=1)
        out[f"{prefix}_valley_hour"] = wide.idxmin(axis=1)

        # Steepness: range divided by hours from valley to peak
        with np.errstate(divide="ignore", invalid="ignore"):
            hours_v_to_p = (out[f"{prefix}_peak_hour"] - out[f"{prefix}_valley_hour"]).abs().replace(0, np.nan)
            out[f"{prefix}_steepness"] = out[f"{prefix}_range"] / hours_v_to_p

    return out


def solar_specific_features(wide: pd.DataFrame) -> pd.DataFrame:
    """Solar-specific shape features (sunrise, sunset, dropoff, asymmetry)."""
    out = pd.DataFrame(index=wide.index)

    # Sunrise / sunset: first/last HE with solar > 1 MW
    above = wide.gt(1)
    # Numpy-friendly first/last True per row
    cols_arr = np.array(wide.columns)
    above_arr = above.to_numpy()
    sunrise = np.where(above_arr.any(axis=1), cols_arr[above_arr.argmax(axis=1)], np.nan)
    rev = above_arr[:, ::-1]
    sunset = np.where(rev.any(axis=1), cols_arr[::-1][rev.argmax(axis=1)], np.nan)
    out["solar_sunrise_he"] = sunrise
    out["solar_sunset_he"] = sunset
    out["solar_daylight_hours"] = sunset - sunrise + 1

    # Specific dropoffs
    if 17 in wide.columns and 20 in wide.columns:
        out["solar_dropoff_17_20"] = wide[17] - wide[20]
    if 16 in wide.columns and 19 in wide.columns:
        out["solar_dropoff_16_19"] = wide[16] - wide[19]
    if 14 in wide.columns and 20 in wide.columns:
        out["solar_dropoff_14_20"] = wide[14] - wide[20]

    # Morning ramp from HE 6 -> peak hour (as proxy: HE 6 to HE 12)
    if 6 in wide.columns and 12 in wide.columns:
        out["solar_rampup_6_12"] = wide[12] - wide[6]

    # Evening / morning ramp asymmetry
    if 6 in wide.columns and 12 in wide.columns and 12 in wide.columns and 20 in wide.columns:
        rampup = wide[12] - wide[6]
        rampdn = wide[12] - wide[20]
        with np.errstate(divide="ignore", invalid="ignore"):
            out["solar_ramp_asymmetry"] = rampdn / rampup.replace(0, np.nan)

    # Total energy (proxy for capacity factor)
    out["solar_total_energy"] = wide.sum(axis=1)
    out["solar_capacity_factor_proxy"] = wide.sum(axis=1) / (wide.max(axis=1) * 24).replace(0, np.nan)

    return out


def cross_features(daily: pd.DataFrame) -> pd.DataFrame:
    """Cross-feature interactions (penetration, coincidence, anti-correlation)."""
    out = pd.DataFrame(index=daily.index)

    # Renewable penetration
    if "solar_avg" in daily and "load_rto_avg" in daily:
        out["solar_penetration"] = daily["solar_avg"] / daily["load_rto_avg"]
    if "wind_avg" in daily and "load_rto_avg" in daily:
        out["wind_penetration"] = daily["wind_avg"] / daily["load_rto_avg"]
    if "solar_avg" in daily and "wind_avg" in daily and "load_rto_avg" in daily:
        out["renewables_penetration"] = (daily["solar_avg"] + daily["wind_avg"]) / daily["load_rto_avg"]

    # Solar at peak load hour (perfect coincidence proxy)
    # Approximated via super-peak averages
    if "solar_avg_super_peak" in daily and "load_rto_avg_super_peak" in daily:
        out["solar_coincidence_at_peak"] = daily["solar_avg_super_peak"] / daily["load_rto_avg_super_peak"]
    if "wind_avg_super_peak" in daily and "load_rto_avg_super_peak" in daily:
        out["wind_coincidence_at_peak"] = daily["wind_avg_super_peak"] / daily["load_rto_avg_super_peak"]

    # Renewables ramp into peak (combined)
    if "solar_ramp_super_peak_17_20" in daily and "wind_ramp_super_peak_17_20" in daily:
        out["renewables_ramp_super_peak_17_20"] = (
            daily["solar_ramp_super_peak_17_20"] + daily["wind_ramp_super_peak_17_20"]
        )
    if "solar_ramp_evening_16_20" in daily and "wind_ramp_evening_16_20" in daily:
        out["renewables_ramp_evening_16_20"] = (
            daily["solar_ramp_evening_16_20"] + daily["wind_ramp_evening_16_20"]
        )

    # Cross-region load ramp differentials (does WEST ramp harder than MIDATL?)
    if "load_west_ramp_evening_16_20" in daily and "load_midatl_ramp_evening_16_20" in daily:
        out["load_ramp_west_minus_midatl_evening"] = (
            daily["load_west_ramp_evening_16_20"] - daily["load_midatl_ramp_evening_16_20"]
        )

    # Net load coincidence with thermal capacity (capacity tightness)
    if "net_load_max" in daily and "total_outages_mw_rto" in daily:
        out["net_load_to_outage_ratio"] = daily["net_load_max"] / daily["total_outages_mw_rto"].replace(0, np.nan)

    # DoD ramp changes (the "is today's ramp bigger than yesterday's" signal)
    for col in [
        "solar_ramp_super_peak_17_20",
        "solar_ramp_evening_16_20",
        "wind_ramp_super_peak_17_20",
        "wind_ramp_evening_16_20",
        "net_load_ramp_valley_to_peak_13_19",
        "net_load_ramp_super_peak_17_20",
        "load_rto_ramp_super_peak_17_20",
        "load_west_ramp_super_peak_17_20",
    ]:
        if col in daily.columns:
            out[f"{col}_dod_chg"] = daily[col].diff()
            out[f"{col}_yoy_chg"] = daily[col].diff(periods=365)

    # Spark spread and implied heat rate proxies (use west DA as lag-1)
    if "tetco_m3_cash" in daily and "lmp_total_da_western_lag1" in daily:
        # placeholder; rebuilt later after lags computed
        pass

    return out


# ============================================================================
# MASTER ASSEMBLY
# ============================================================================

def build_daily_features() -> pd.DataFrame:
    print("Loading data...")
    lmp = load_lmps_wide()
    fuel = load_fuel_mix()
    load = load_load_wide()
    weather = load_weather_daily()
    gas = load_gas_daily()
    outages = load_outages_daily()
    cal = load_calendar()
    print(f"  LMP rows: {len(lmp):,}, fuel: {len(fuel):,}, load: {len(load):,}")

    # Build hourly master (date, hour_ending) -> wide signals
    hourly = lmp.merge(fuel, on=["date", "hour_ending"], how="outer")
    hourly = hourly.merge(load, on=["date", "hour_ending"], how="outer")
    hourly = hourly.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # RTO net load = load_rto - solar - wind
    if "load_rto" in hourly and "solar" in hourly and "wind" in hourly:
        hourly["net_load"] = hourly["load_rto"] - hourly["solar"].fillna(0) - hourly["wind"].fillna(0)

    # === DAILY FEATURE TABLES ===
    daily_parts = []

    # Solar / Wind / Net-load (RTO only; fuel_mix is RTO)
    print("Building solar / wind / net-load ramp features...")
    daily_parts.append(ramp_features(_wide(hourly, "solar"), "solar"))
    daily_parts.append(solar_specific_features(_wide(hourly, "solar")))
    daily_parts.append(ramp_features(_wide(hourly, "wind"), "wind"))
    daily_parts.append(ramp_features(_wide(hourly, "net_load"), "net_load"))
    daily_parts.append(ramp_features(_wide(hourly, "renewables"), "renewables"))
    daily_parts.append(ramp_features(_wide(hourly, "thermal"), "thermal"))

    # Load ramps by region
    print("Building load ramp features by region...")
    for region in [r.lower() for r in LOAD_REGIONS]:
        col = f"load_{region}"
        if col in hourly.columns:
            daily_parts.append(ramp_features(_wide(hourly, col), col))

    # Hub LMP daily features (each hub: DA / RT on-peak avg, super-peak avg, vol)
    print("Building hub LMP daily features...")
    for _, slug in TARGET_HUBS:
        for mkt in ("da", "rt"):
            col = f"lmp_total_{mkt}_{slug}"
            if col in hourly.columns:
                daily_parts.append(ramp_features(_wide(hourly, col), f"{mkt}_{slug}", include_specific=False))
        # Congestion component
        cong_col = f"lmp_congestion_price_rt_{slug}"
        if cong_col in hourly.columns:
            daily_parts.append(ramp_features(_wide(hourly, cong_col), f"cong_rt_{slug}", include_specific=False))

    daily = pd.concat(daily_parts, axis=1).reset_index()

    # Merge in scalar daily sources
    daily = daily.merge(weather, on="date", how="left")
    daily = daily.merge(gas, on="date", how="left")
    daily = daily.merge(outages, on="date", how="left")
    daily = daily.merge(cal, on="date", how="left")

    # Cross features (penetration, coincidence, DoD)
    print("Building cross / interaction features...")
    cross = cross_features(daily.set_index("date"))
    cross = cross.reset_index()
    daily = daily.merge(cross, on="date", how="left")

    # === HUB-LEVEL DERIVED FEATURES ===
    # DART spreads, hub spreads, lags
    for _, slug in TARGET_HUBS:
        da = f"da_{slug}_avg_onpeak"
        rt = f"rt_{slug}_avg_onpeak"
        if da in daily and rt in daily:
            daily[f"dart_{slug}"] = daily[da] - daily[rt]

        # Lags / MAs of RT on-peak (the universal LMP baseline)
        if rt in daily:
            daily[f"{rt}_lag1"] = daily[rt].shift(1)
            daily[f"{rt}_lag2"] = daily[rt].shift(2)
            daily[f"{rt}_lag7"] = daily[rt].shift(7)
            daily[f"{rt}_ma5"] = daily[rt].rolling(5).mean()
            daily[f"{rt}_ma20"] = daily[rt].rolling(20).mean()
            daily[f"{rt}_momentum_5"] = daily[rt] - daily[rt].shift(5)
            daily[f"{rt}_zscore_20"] = (daily[rt] - daily[rt].rolling(20).mean()) / daily[rt].rolling(20).std()

    # Hub spreads (Western vs others)
    base = "rt_western_avg_onpeak"
    if base in daily:
        for _, slug in TARGET_HUBS:
            if slug == "western":
                continue
            other = f"rt_{slug}_avg_onpeak"
            if other in daily:
                daily[f"spread_western_{slug}"] = daily[base] - daily[other]

    # Spark spread / implied heat rate (rough)
    if "tetco_m3_cash" in daily and "rt_western_avg_onpeak_lag1" in daily:
        daily["implied_hr_western_lag1"] = (
            daily["rt_western_avg_onpeak_lag1"] / daily["tetco_m3_cash"].replace(0, np.nan)
        )

    daily = daily.sort_values("date").reset_index(drop=True)
    print(f"  Final daily frame: {len(daily):,} days, {len(daily.columns)} columns")
    return daily


# ============================================================================
# TARGETS + INDICATOR TESTS
# ============================================================================

def build_targets(daily: pd.DataFrame) -> pd.DataFrame:
    """Add next-day on-peak DA targets for each hub.

    Restricts target to next day being a weekday non-NERC-holiday (real on-peak).
    """
    df = daily.copy()
    next_is_onpeak_day = (
        (df["is_weekend"].shift(-1) == False)
        & (df["is_nerc_holiday"].shift(-1) == False)
    )
    for _, slug in TARGET_HUBS:
        col = f"da_{slug}_avg_onpeak"
        if col in df.columns:
            target = df[col].shift(-1)
            target = target.where(next_is_onpeak_day)
            df[f"target_{slug}_da_t1"] = target
            df[f"target_{slug}_da_t1_chg"] = target - df[col]
    return df


def categorize(key: str) -> str:
    k = key.lower()
    # Order matters
    if k.startswith("solar_"): return "Solar Ramp"
    if k.startswith("wind_"): return "Wind Ramp"
    if k.startswith("net_load_"): return "Net-Load Ramp"
    if k.startswith("renewables_"): return "Renewables"
    if k.startswith("load_west_") or k.startswith("load_midatl_") or k.startswith("load_south_") or k.startswith("load_rto_"):
        return "Load Ramp (Region)"
    if k.startswith("thermal_"): return "Thermal"
    if k.startswith("rt_") or k.startswith("da_"): return "LMP Hub Profile"
    if k.startswith("cong_"): return "Congestion"
    if k.startswith("dart_") or k.startswith("spread_"): return "DART / Hub Spread"
    if k.startswith("hh_") or "_cash" in k or k.startswith("gas_basis"): return "Gas"
    if k.startswith("temp") or k in ("hdd", "cdd"): return "Weather"
    if "outage" in k: return "Outages"
    if k in ("month", "day_of_week_number", "is_weekend", "is_nerc_holiday", "is_federal_holiday", "summer_winter"):
        return "Calendar"
    if "penetration" in k or "coincidence" in k or "ramp_west_minus" in k or "outage_ratio" in k or "implied_hr" in k:
        return "Cross / Interaction"
    if "dod_chg" in k or "yoy_chg" in k:
        return "Ramp DoD/YoY"
    return "Other"


def test_indicator(x: np.ndarray, y_level: np.ndarray, y_chg: np.ndarray) -> dict:
    """Battery of statistical tests for one (indicator, target) pair."""
    valid = ~(np.isnan(x) | np.isnan(y_level) | np.isinf(x) | np.isinf(y_level))
    n = int(valid.sum())
    if n < 60:
        return {"n": n, "corr_level": np.nan, "corr_change": np.nan, "r2": np.nan,
                "dir_acc": np.nan, "ic": np.nan, "ic_pval": np.nan, "qspread": np.nan}

    xv, yv = x[valid], y_level[valid]
    sx, sy = xv.std(), yv.std()
    corr_level = float(np.corrcoef(xv, yv)[0, 1]) if sx > 0 and sy > 0 else np.nan

    # Change
    valid_c = valid & ~np.isnan(y_chg)
    if valid_c.sum() > 60:
        xc, ycc = x[valid_c], y_chg[valid_c]
        sxc, syc = xc.std(), ycc.std()
        corr_change = float(np.corrcoef(xc, ycc)[0, 1]) if sxc > 0 and syc > 0 else np.nan
    else:
        corr_change = np.nan

    # Single-feature OLS R²
    try:
        X = np.column_stack([np.ones(len(xv)), xv])
        beta, *_ = np.linalg.lstsq(X, yv, rcond=None)
        y_pred = X @ beta
        ss_res = ((yv - y_pred) ** 2).sum()
        ss_tot = ((yv - yv.mean()) ** 2).sum()
        r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else np.nan
    except Exception:
        r2 = np.nan

    # Direction accuracy (median split)
    med = np.median(xv)
    pred_up = (xv > med).astype(int)
    actual_up = (yv > np.median(yv)).astype(int)
    dir_acc = float((pred_up == actual_up).mean())

    # Spearman IC vs level
    try:
        ic, ic_p = spearmanr(xv, yv)
        ic = float(ic) if not np.isnan(ic) else np.nan
        ic_p = float(ic_p) if not np.isnan(ic_p) else np.nan
    except Exception:
        ic, ic_p = np.nan, np.nan

    # Top-quintile minus bottom-quintile next-day price (signal-strength metric)
    try:
        q_low, q_high = np.quantile(xv, [0.2, 0.8])
        top = yv[xv >= q_high]
        bot = yv[xv <= q_low]
        qspread = float(top.mean() - bot.mean()) if len(top) > 5 and len(bot) > 5 else np.nan
    except Exception:
        qspread = np.nan

    return {"n": n, "corr_level": corr_level, "corr_change": corr_change,
            "r2": r2, "dir_acc": dir_acc, "ic": ic, "ic_pval": ic_p, "qspread": qspread}


SKIP_AS_INDICATOR = set()


def gather_indicators(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Pick all numeric columns that aren't targets/IDs."""
    skip = {"date"}
    skip |= {c for c in df.columns if c.startswith("target_")}
    # Skip raw next-day forward LMP itself (would be leakage if we used it)
    indicators = {}
    for c in df.columns:
        if c in skip:
            continue
        if not pd.api.types.is_numeric_dtype(df[c]):
            continue
        if df[c].notna().sum() < 100:
            continue
        indicators[c] = df[c]
    return indicators


def run_all_tests(df: pd.DataFrame) -> pd.DataFrame:
    """Test every indicator against every hub target. Returns long DataFrame."""
    target_specs = []
    for _, slug in TARGET_HUBS:
        t_level = f"target_{slug}_da_t1"
        t_chg = f"target_{slug}_da_t1_chg"
        if t_level in df.columns:
            target_specs.append((slug, t_level, t_chg))

    indicators = gather_indicators(df)
    print(f"  Testing {len(indicators)} indicators against {len(target_specs)} targets...")

    rows = []
    for ind_key, series in indicators.items():
        x = series.to_numpy(dtype=float)
        cat = categorize(ind_key)
        for slug, t_level, t_chg in target_specs:
            y = df[t_level].to_numpy(dtype=float)
            y_chg = df[t_chg].to_numpy(dtype=float)
            stats = test_indicator(x, y, y_chg)
            rows.append({
                "indicator": ind_key,
                "category": cat,
                "target": slug,
                **stats,
                "abs_corr": abs(stats["corr_level"]) if not np.isnan(stats["corr_level"]) else np.nan,
            })
    return pd.DataFrame(rows)


# ============================================================================
# HTML REPORT
# ============================================================================

def _color_corr(v: float) -> str:
    if pd.isna(v): return "#eee"
    a = min(abs(v), 1.0)
    if v > 0:
        return f"rgba(46,125,50,{0.15 + 0.85 * a:.2f})"
    return f"rgba(198,40,40,{0.15 + 0.85 * a:.2f})"


def render_html(results: pd.DataFrame, n_days: int, date_range: tuple) -> str:
    hubs = [s for _, s in TARGET_HUBS]
    cats = sorted(results["category"].unique())

    # Pivot for heatmap: rows=indicator, cols=hub, vals=corr_level
    heat = results.pivot_table(index="indicator", columns="target", values="corr_level")
    heat["mean_abs_corr"] = heat[hubs].abs().mean(axis=1)
    heat = heat.sort_values("mean_abs_corr", ascending=False)
    heat_categories = results.drop_duplicates("indicator").set_index("indicator")["category"]

    # Top 10 per hub
    top_per_hub = {}
    for hub in hubs:
        h = results[results["target"] == hub].copy()
        h["abs_corr_finite"] = h["abs_corr"].fillna(-1)
        h = h.sort_values("abs_corr_finite", ascending=False).head(10)
        top_per_hub[hub] = h

    # Category averages (across all hubs)
    cat_summary = (
        results.groupby("category")
        .agg(
            n_indicators=("indicator", "nunique"),
            mean_abs_corr=("abs_corr", "mean"),
            best_indicator=("abs_corr", lambda s: results.loc[s.idxmax(), "indicator"] if s.notna().any() else "-"),
        )
        .sort_values("mean_abs_corr", ascending=False)
    )

    css = """
    *{margin:0;padding:0;box-sizing:border-box;}
    body{font-family:-apple-system,Arial,sans-serif;background:#f7f8fa;padding:18px;max-width:1700px;margin:0 auto;color:#222;}
    h1{margin-bottom:4px;}
    h2{margin:24px 0 10px;color:#1565c0;border-bottom:2px solid #1565c0;padding-bottom:6px;}
    .sub{color:#666;font-size:13px;margin-bottom:18px;}
    .card{background:white;border-radius:8px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,0.06);margin-bottom:16px;}
    table{width:100%;border-collapse:collapse;font-size:11.5px;}
    th{background:#2c3e50;color:white;padding:6px 8px;text-align:left;cursor:pointer;position:sticky;top:0;}
    td{padding:4px 8px;border-bottom:1px solid #f0f0f0;}
    td.num{text-align:right;font-family:'JetBrains Mono',Consolas,monospace;font-size:11px;}
    tr:hover td{background:#f4f8ff;}
    .cat{display:inline-block;padding:2px 8px;border-radius:10px;font-size:9.5px;font-weight:bold;color:white;}
    .cat-Solar.Ramp{background:#f57c00;}
    .cat-Wind.Ramp{background:#0288d1;}
    .cat-Net-Load.Ramp{background:#5e35b1;}
    .cat-Renewables{background:#388e3c;}
    .cat-Load.Ramp.\\(Region\\){background:#00897b;}
    .cat-Thermal{background:#6d4c41;}
    .cat-LMP.Hub.Profile{background:#1565c0;}
    .cat-Congestion{background:#c62828;}
    .cat-DART.\\/.Hub.Spread{background:#ad1457;}
    .cat-Gas{background:#ef6c00;}
    .cat-Weather{background:#0097a7;}
    .cat-Outages{background:#455a64;}
    .cat-Calendar{background:#9e9e9e;}
    .cat-Cross.\\/.Interaction{background:#7b1fa2;}
    .cat-Ramp.DoD\\/YoY{background:#bf360c;}
    .cat-Other{background:#999;}
    .cat-pill{padding:2px 8px;border-radius:10px;font-size:9.5px;font-weight:bold;color:white;}
    .heat td{font-family:'JetBrains Mono',Consolas,monospace;font-size:10.5px;text-align:right;}
    .filter{margin-bottom:10px;display:flex;gap:6px;flex-wrap:wrap;}
    .fbtn{padding:5px 11px;border:1px solid #ccc;border-radius:14px;cursor:pointer;font-size:11px;background:white;}
    .fbtn.active{background:#1565c0;color:white;border-color:#1565c0;}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:14px;}
    .small{font-size:11px;color:#666;}
    .pos{color:#2e7d32;font-weight:bold;}
    .neg{color:#c62828;font-weight:bold;}
    """

    parts = []
    parts.append(f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>PJM Hub Ramp Indicator Backtest</title><style>{css}</style></head><body>")
    parts.append(f"<h1>PJM Hub Ramp Indicator Backtest</h1>")
    parts.append(f"<p class='sub'>{len(results['indicator'].unique())} indicators &times; {len(hubs)} hub targets &middot; "
                 f"{n_days:,} days &middot; {date_range[0]} &rarr; {date_range[1]} &middot; "
                 f"target = next-day on-peak DA LMP (HE 8-23 weekday non-holiday)</p>")

    # === Section: Category summary ===
    parts.append("<h2>Category Summary (avg |correlation| across all hubs)</h2>")
    parts.append("<div class='card'><table>")
    parts.append("<tr><th>Category</th><th>#</th><th>Avg |Corr|</th><th>Best Indicator</th></tr>")
    for cat, row in cat_summary.iterrows():
        cls_cat = cat.replace(" ", ".").replace("/", "\\/")
        parts.append(
            f"<tr><td><span class='cat-pill' style='background:{_cat_color(cat)};'>{cat}</span></td>"
            f"<td class='num'>{int(row['n_indicators'])}</td>"
            f"<td class='num'>{row['mean_abs_corr']:.3f}</td>"
            f"<td class='small'>{row['best_indicator']}</td></tr>"
        )
    parts.append("</table></div>")

    # === Section: Top 10 per hub ===
    parts.append("<h2>Top 10 Indicators per Hub (by |corr| with next-day on-peak DA)</h2>")
    parts.append("<div class='grid'>")
    for hub in hubs:
        parts.append(f"<div class='card'><h3 style='margin-bottom:8px;color:#1565c0;'>{hub.upper()}</h3>")
        parts.append("<table><tr><th>#</th><th>Indicator</th><th>Cat</th><th>Corr</th><th>R²</th><th>IC</th><th>QSpread $</th></tr>")
        for i, r in enumerate(top_per_hub[hub].itertuples(), 1):
            cls = "pos" if r.corr_level > 0 else "neg"
            parts.append(
                f"<tr><td>{i}</td><td class='small'>{r.indicator}</td>"
                f"<td><span class='cat-pill' style='background:{_cat_color(r.category)};'>{r.category}</span></td>"
                f"<td class='num {cls}'>{r.corr_level:.3f}</td>"
                f"<td class='num'>{r.r2:.3f}</td>"
                f"<td class='num'>{r.ic:.3f}</td>"
                f"<td class='num'>{r.qspread:.1f}</td></tr>"
            )
        parts.append("</table></div>")
    parts.append("</div>")

    # === Section: Heatmap ===
    parts.append(f"<h2>Top 50 Indicators &mdash; Correlation Heatmap by Hub</h2>")
    parts.append("<div class='card'><div style='overflow-x:auto;'>")
    parts.append("<table class='heat'><tr><th>Indicator</th><th>Cat</th>")
    for h in hubs:
        parts.append(f"<th>{h}</th>")
    parts.append("<th>|Mean|</th></tr>")
    for ind in heat.head(50).index:
        cat = heat_categories.get(ind, "Other")
        parts.append(f"<tr><td class='small'>{ind}</td><td><span class='cat-pill' style='background:{_cat_color(cat)};'>{cat}</span></td>")
        for h in hubs:
            v = heat.loc[ind, h]
            color = _color_corr(v)
            text = "—" if pd.isna(v) else f"{v:+.3f}"
            parts.append(f"<td style='background:{color};'>{text}</td>")
        m = heat.loc[ind, "mean_abs_corr"]
        parts.append(f"<td class='num'><b>{m:.3f}</b></td></tr>")
    parts.append("</table></div></div>")

    # === Section: Full filterable table ===
    parts.append("<h2>All Results &mdash; Filterable</h2>")
    parts.append("<div class='card'>")
    parts.append("<div class='filter'><b style='align-self:center;margin-right:6px;'>Hub:</b>")
    parts.append("<button class='fbtn active' onclick=\"filt('hub','ALL',this)\">All</button>")
    for h in hubs:
        parts.append(f"<button class='fbtn' onclick=\"filt('hub','{h}',this)\">{h}</button>")
    parts.append("</div><div class='filter'><b style='align-self:center;margin-right:6px;'>Category:</b>")
    parts.append("<button class='fbtn active' onclick=\"filt('cat','ALL',this)\">All</button>")
    for c in cats:
        parts.append(f"<button class='fbtn' onclick=\"filt('cat','{c}',this)\">{c}</button>")
    parts.append("</div>")

    parts.append("<table id='full'><tr><th>Indicator</th><th>Category</th><th>Hub</th><th>n</th>"
                 "<th>Corr (Level)</th><th>Corr (Chg)</th><th>R²</th><th>Dir Acc</th>"
                 "<th>IC</th><th>p</th><th>QSpread $</th></tr>")
    sorted_results = results.copy()
    sorted_results["abs_corr_fill"] = sorted_results["abs_corr"].fillna(-1)
    sorted_results = sorted_results.sort_values("abs_corr_fill", ascending=False)
    for r in sorted_results.itertuples():
        cls = "pos" if (not pd.isna(r.corr_level) and r.corr_level > 0) else "neg"
        dir_cls = "pos" if (not pd.isna(r.dir_acc) and r.dir_acc > 0.55) else ("neg" if (not pd.isna(r.dir_acc) and r.dir_acc < 0.45) else "")
        pval = "<0.001" if (not pd.isna(r.ic_pval) and r.ic_pval < 0.001) else (f"{r.ic_pval:.3f}" if not pd.isna(r.ic_pval) else "—")
        def f(v, fmt="{:.3f}"):
            return "—" if pd.isna(v) else fmt.format(v)
        parts.append(
            f"<tr data-cat='{r.category}' data-hub='{r.target}'>"
            f"<td class='small'>{r.indicator}</td>"
            f"<td><span class='cat-pill' style='background:{_cat_color(r.category)};'>{r.category}</span></td>"
            f"<td>{r.target}</td><td class='num'>{r.n}</td>"
            f"<td class='num {cls}'>{f(r.corr_level)}</td>"
            f"<td class='num'>{f(r.corr_change)}</td>"
            f"<td class='num'>{f(r.r2)}</td>"
            f"<td class='num {dir_cls}'>{f(r.dir_acc, '{:.1%}')}</td>"
            f"<td class='num'>{f(r.ic)}</td>"
            f"<td class='num'>{pval}</td>"
            f"<td class='num'>{f(r.qspread, '{:.1f}')}</td></tr>"
        )
    parts.append("</table></div>")

    parts.append("""
<script>
let f={hub:'ALL',cat:'ALL'};
function filt(k,v,btn){
  f[k]=v;
  btn.parentElement.querySelectorAll('.fbtn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('#full tr[data-cat]').forEach(tr=>{
    const okC = f.cat==='ALL' || tr.dataset.cat===f.cat;
    const okH = f.hub==='ALL' || tr.dataset.hub===f.hub;
    tr.style.display = (okC && okH) ? '' : 'none';
  });
}
</script>
</body></html>""")
    return "".join(parts)


_CAT_COLORS = {
    "Solar Ramp": "#f57c00",
    "Wind Ramp": "#0288d1",
    "Net-Load Ramp": "#5e35b1",
    "Renewables": "#388e3c",
    "Load Ramp (Region)": "#00897b",
    "Thermal": "#6d4c41",
    "LMP Hub Profile": "#1565c0",
    "Congestion": "#c62828",
    "DART / Hub Spread": "#ad1457",
    "Gas": "#ef6c00",
    "Weather": "#0097a7",
    "Outages": "#455a64",
    "Calendar": "#9e9e9e",
    "Cross / Interaction": "#7b1fa2",
    "Ramp DoD/YoY": "#bf360c",
    "Other": "#999",
}


def _cat_color(c: str) -> str:
    return _CAT_COLORS.get(c, "#999")


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    daily = build_daily_features()
    daily = build_targets(daily)

    print("Running indicator tests...")
    results = run_all_tests(daily)
    n_days_used = int(daily.dropna(subset=[f"target_{TARGET_HUBS[0][1]}_da_t1"]).shape[0])
    date_range = (str(daily["date"].min().date()), str(daily["date"].max().date()))

    print("Rendering HTML...")
    html = render_html(results, n_days_used, date_range)
    out_path = OUTPUT_DIR / f"hub_ramp_backtest_{Date.today().isoformat()}.html"
    out_path.write_text(html, encoding="utf-8")

    # Also dump the long-form CSV for downstream use
    csv_path = OUTPUT_DIR / f"hub_ramp_backtest_{Date.today().isoformat()}.csv"
    results.to_csv(csv_path, index=False)
    daily_path = OUTPUT_DIR / f"hub_ramp_daily_{Date.today().isoformat()}.parquet"
    daily.to_parquet(daily_path, index=False)

    print(f"\nReport:    {out_path}")
    print(f"Long CSV:  {csv_path}")
    print(f"Daily parq:{daily_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
