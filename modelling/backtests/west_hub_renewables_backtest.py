"""
Western Hub renewables-ramp study.

PART 1 (Dynamics): how have solar / wind grown across PJM areas, what do the
diurnal profiles look like, and how do regional renewables coincide with
Western Hub peak prices?

PART 2 (Backtest): tests ramp/profile/penetration indicators built from
regional solar & wind generation (RTO, RFC, WEST, SOUTH, MIDATL) for
predictive power against next-day Western Hub on-peak DA LMP.

Data: pjm.solar_generation_by_area + pjm.wind_generation_by_area, cached as
modelling/data/cache/pjm_(solar|wind)_generation_by_area_hourly.parquet.

Output: modelling/backtests/output/west_hub_renewables_backtest_<date>.html
"""
from __future__ import annotations

import sys
from datetime import date as Date
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

_BACKTESTS = Path(__file__).resolve().parent
_MODELLING_ROOT = _BACKTESTS.parent
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))
if str(_BACKTESTS) not in sys.path:
    sys.path.insert(0, str(_BACKTESTS))

# Re-use generic ramp builders from the hub_ramp_backtest script.
from hub_ramp_backtest import (  # noqa: E402
    ramp_features,
    solar_specific_features,
    test_indicator,
    _wide,
    _cat_color,
    _color_corr,
    load_lmps_wide,
    load_load_wide,
    load_fuel_mix,
    load_weather_daily,
    load_gas_daily,
    load_outages_daily,
    load_calendar,
    TARGET_HUBS,
    LOAD_REGIONS,
    ONPEAK_HOURS,
    SUPER_PEAK_HOURS,
)

CACHE_DIR = _MODELLING_ROOT / "data" / "cache"
OUTPUT_DIR = _BACKTESTS / "output"

# Solar growth means earlier years have ~zero solar in WEST/SOUTH; use 2022+
START_DATE = pd.Timestamp("2022-01-01")
# PJM zonal regions only (RFC = NERC reliability region overlapping WEST+MIDATL; RTO = aggregate)
AREAS = ["WEST", "SOUTH", "MIDATL"]


# ============================================================================
# REGIONAL DATA LOADERS
# ============================================================================

def load_regional_solar() -> pd.DataFrame:
    df = pd.read_parquet(CACHE_DIR / "pjm_solar_generation_by_area_hourly.parquet")
    df["datetime_beginning_ept"] = pd.to_datetime(df["datetime_beginning_ept"])
    df = df[df["datetime_beginning_ept"] >= START_DATE]
    df["date"] = df["datetime_beginning_ept"].dt.normalize()
    df["hour_ending"] = df["datetime_beginning_ept"].dt.hour + 1
    df = df[df["area"].isin(AREAS)]
    return df[["date", "hour_ending", "area", "solar_generation_mw"]]


def load_regional_wind() -> pd.DataFrame:
    df = pd.read_parquet(CACHE_DIR / "pjm_wind_generation_by_area_hourly.parquet")
    df["datetime_beginning_ept"] = pd.to_datetime(df["datetime_beginning_ept"])
    df = df[df["datetime_beginning_ept"] >= START_DATE]
    df["date"] = df["datetime_beginning_ept"].dt.normalize()
    df["hour_ending"] = df["datetime_beginning_ept"].dt.hour + 1
    df = df[df["area"].isin(AREAS)]
    return df[["date", "hour_ending", "area", "wind_generation_mw"]]


# ============================================================================
# DYNAMICS STUDY
# ============================================================================

def dynamics_capacity_growth(solar_long: pd.DataFrame, wind_long: pd.DataFrame) -> pd.DataFrame:
    """Year x area peak/avg MW for solar and wind."""
    out = []
    for source, df, val in [
        ("solar", solar_long, "solar_generation_mw"),
        ("wind", wind_long, "wind_generation_mw"),
    ]:
        d = df.copy()
        d["year"] = d["date"].dt.year
        agg = d.groupby(["year", "area"])[val].agg(["mean", "max"]).reset_index()
        agg = agg.rename(columns={"mean": f"{source}_avg_mw", "max": f"{source}_max_mw"})
        out.append(agg)
    df = out[0].merge(out[1], on=["year", "area"], how="outer")
    return df.sort_values(["area", "year"]).reset_index(drop=True)


def dynamics_diurnal_profile(solar_long: pd.DataFrame, wind_long: pd.DataFrame) -> pd.DataFrame:
    """Average MW by HE, by area, by season (last 12 months only)."""
    cutoff = solar_long["date"].max() - pd.Timedelta(days=365)

    def _profile(df, val, source):
        d = df[df["date"] >= cutoff].copy()
        d["season"] = np.where(d["date"].dt.month.isin([6, 7, 8, 9]), "summer", "winter")
        agg = d.groupby(["area", "season", "hour_ending"])[val].mean().reset_index()
        agg["source"] = source
        agg = agg.rename(columns={val: "mw"})
        return agg
    return pd.concat([
        _profile(solar_long, "solar_generation_mw", "solar"),
        _profile(wind_long, "wind_generation_mw", "wind"),
    ], ignore_index=True)


def dynamics_coincidence_with_west_hub_peak(
    solar_wide_by_area: dict[str, pd.DataFrame],
    wind_wide_by_area: dict[str, pd.DataFrame],
    west_da_onpeak: pd.Series,
) -> pd.DataFrame:
    """
    Coincidence: how does each area's avg renewable output during HE 17-20
    correlate with Western Hub on-peak DA LMP?

    Computed unconditionally and on price-spike days (top decile).
    """
    rows = []
    spike_thresh = west_da_onpeak.quantile(0.9)

    for area in AREAS:
        for source, wide_dict in [("solar", solar_wide_by_area), ("wind", wind_wide_by_area)]:
            wide = wide_dict[area]
            sp_cols = [c for c in SUPER_PEAK_HOURS if c in wide.columns]
            super_peak_avg = wide[sp_cols].mean(axis=1)
            joined = pd.concat([super_peak_avg, west_da_onpeak], axis=1, keys=["x", "y"]).dropna()
            if len(joined) < 50:
                continue
            corr_all = joined["x"].corr(joined["y"])
            spikes = joined[joined["y"] >= spike_thresh]
            corr_spike = spikes["x"].corr(spikes["y"]) if len(spikes) > 30 else np.nan
            rows.append({
                "area": area,
                "source": source,
                "n_days": len(joined),
                "avg_super_peak_mw": joined["x"].mean(),
                "corr_with_west_da": corr_all,
                "corr_on_spike_days": corr_spike,
            })
    return pd.DataFrame(rows)


# ============================================================================
# REGIONAL RAMP FEATURE BUILDER
# ============================================================================

def build_regional_renewable_features(
    solar_long: pd.DataFrame, wind_long: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, dict[str, pd.DataFrame]]]:
    """Build daily ramp features for solar and wind, per area."""
    daily_parts = []
    wide_by = {"solar": {}, "wind": {}}

    for area in AREAS:
        # SOLAR
        s = solar_long[solar_long["area"] == area]
        wide = s.pivot_table(
            index="date", columns="hour_ending", values="solar_generation_mw"
        ).reindex(columns=range(1, 25))
        wide_by["solar"][area] = wide
        prefix = f"solar_{area.lower()}"
        daily_parts.append(ramp_features(wide, prefix))
        daily_parts.append(_solar_specific_with_prefix(wide, prefix))

        # WIND
        w = wind_long[wind_long["area"] == area]
        wide_w = w.pivot_table(
            index="date", columns="hour_ending", values="wind_generation_mw"
        ).reindex(columns=range(1, 25))
        wide_by["wind"][area] = wide_w
        prefix_w = f"wind_{area.lower()}"
        daily_parts.append(ramp_features(wide_w, prefix_w))

    daily = pd.concat(daily_parts, axis=1)
    return daily.reset_index(), wide_by


def _solar_specific_with_prefix(wide: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """solar_specific_features but with arbitrary prefix (it hardcodes 'solar')."""
    df = pd.DataFrame(index=wide.index)
    above = wide.gt(1)
    cols_arr = np.array(wide.columns)
    above_arr = above.to_numpy()
    sunrise = np.where(above_arr.any(axis=1), cols_arr[above_arr.argmax(axis=1)], np.nan)
    rev = above_arr[:, ::-1]
    sunset = np.where(rev.any(axis=1), cols_arr[::-1][rev.argmax(axis=1)], np.nan)
    df[f"{prefix}_sunrise_he"] = sunrise
    df[f"{prefix}_sunset_he"] = sunset
    df[f"{prefix}_daylight_hours"] = sunset - sunrise + 1

    if 17 in wide.columns and 20 in wide.columns:
        df[f"{prefix}_dropoff_17_20"] = wide[17] - wide[20]
    if 16 in wide.columns and 19 in wide.columns:
        df[f"{prefix}_dropoff_16_19"] = wide[16] - wide[19]
    if 14 in wide.columns and 20 in wide.columns:
        df[f"{prefix}_dropoff_14_20"] = wide[14] - wide[20]
    if 6 in wide.columns and 12 in wide.columns:
        df[f"{prefix}_rampup_6_12"] = wide[12] - wide[6]
    if 6 in wide.columns and 12 in wide.columns and 20 in wide.columns:
        rampup = wide[12] - wide[6]
        rampdn = wide[12] - wide[20]
        with np.errstate(divide="ignore", invalid="ignore"):
            df[f"{prefix}_ramp_asymmetry"] = rampdn / rampup.replace(0, np.nan)

    df[f"{prefix}_total_energy"] = wide.sum(axis=1)
    df[f"{prefix}_capacity_factor_proxy"] = wide.sum(axis=1) / (wide.max(axis=1) * 24).replace(0, np.nan)
    return df


def build_cross_region_features(daily: pd.DataFrame) -> pd.DataFrame:
    """Cross-region renewable interactions."""
    out = pd.DataFrame(index=daily.index)

    # Penetration share: each area's contribution to PJM zonal total (WEST+SOUTH+MIDATL)
    for src in ["solar", "wind"]:
        zone_cols = [f"{src}_{a.lower()}_avg" for a in AREAS]
        if all(c in daily.columns for c in zone_cols):
            zonal_total = daily[zone_cols].sum(axis=1)
            for area in AREAS:
                col_a = f"{src}_{area.lower()}_avg"
                out[f"{src}_{area.lower()}_share_of_pjm"] = daily[col_a] / zonal_total.replace(0, np.nan)

    # Dropoff differentials — WEST vs others (does WEST solar dropoff matter more?)
    for src in ["solar"]:
        for window in ["dropoff_17_20", "dropoff_16_19", "ramp_super_peak_17_20", "ramp_evening_16_20"]:
            wcol = f"{src}_west_{window}"
            scol = f"{src}_south_{window}"
            mcol = f"{src}_midatl_{window}"
            if wcol in daily and scol in daily:
                out[f"{src}_west_minus_south_{window}"] = daily[wcol] - daily[scol]
            if wcol in daily and mcol in daily:
                out[f"{src}_west_minus_midatl_{window}"] = daily[wcol] - daily[mcol]

    for src in ["wind"]:
        for window in ["ramp_super_peak_17_20", "ramp_evening_16_20", "max_3hr_ramp_down", "max_6hr_ramp_down"]:
            wcol = f"{src}_west_{window}"
            mcol = f"{src}_midatl_{window}"
            scol = f"{src}_south_{window}"
            if wcol in daily and mcol in daily:
                out[f"{src}_west_minus_midatl_{window}"] = daily[wcol] - daily[mcol]
            if wcol in daily and scol in daily:
                out[f"{src}_west_minus_south_{window}"] = daily[wcol] - daily[scol]

    # WEST renewables coincidence with WEST load (penetration of WEST renewables in WEST load)
    for src in ["solar", "wind"]:
        gen_col = f"{src}_west_avg_super_peak"
        load_col = "load_west_avg_super_peak"
        if gen_col in daily and load_col in daily:
            out[f"{src}_west_coincidence_west_load"] = daily[gen_col] / daily[load_col].replace(0, np.nan)

    # Combined renewables ramp by region (solar + wind)
    for area in AREAS:
        for window in ["ramp_super_peak_17_20", "ramp_evening_16_20", "ramp_valley_to_peak_13_19"]:
            scol = f"solar_{area.lower()}_{window}"
            wcol = f"wind_{area.lower()}_{window}"
            if scol in daily and wcol in daily:
                out[f"renewables_{area.lower()}_{window}"] = daily[scol] + daily[wcol]
            scol_avg = f"solar_{area.lower()}_avg_super_peak"
            wcol_avg = f"wind_{area.lower()}_avg_super_peak"
            if scol_avg in daily and wcol_avg in daily:
                out[f"renewables_{area.lower()}_avg_super_peak"] = daily[scol_avg] + daily[wcol_avg]

    # Day-over-day ramp changes for the critical windows
    for col in [
        "solar_west_dropoff_17_20",
        "solar_west_dropoff_16_19",
        "solar_west_ramp_super_peak_17_20",
        "solar_west_ramp_evening_16_20",
        "solar_south_dropoff_17_20",
        "solar_midatl_dropoff_17_20",
        "wind_west_ramp_super_peak_17_20",
        "wind_west_max_3hr_ramp_down",
        "wind_west_max_6hr_ramp_down",
    ]:
        if col in daily.columns:
            out[f"{col}_dod_chg"] = daily[col].diff()

    return out


# ============================================================================
# CATEGORIZER (regional)
# ============================================================================

def categorize_regional(key: str) -> str:
    k = key.lower()
    # Special patterns FIRST (otherwise solar_west_* matches before solar_west_minus_*)
    if "_minus_" in k and (k.startswith("solar_") or k.startswith("wind_")):
        return "Cross-Region Diff"
    if "share_of_pjm" in k or "share_of_rto" in k:
        return "Penetration Share"
    if "_coincidence_west_load" in k:
        return "Coincidence (WEST/WEST)"
    if k.endswith("_dod_chg"):
        return "Ramp DoD Change"
    # Regional renewable
    for area in ["west", "rfc", "rto", "south", "midatl"]:
        if k.startswith(f"solar_{area}_"):
            return f"Solar Ramp ({area.upper()})"
        if k.startswith(f"wind_{area}_"):
            return f"Wind Ramp ({area.upper()})"
        if k.startswith(f"renewables_{area}_"):
            return f"Renewables ({area.upper()})"

    # Fallbacks (mirror hub_ramp_backtest categorize)
    if k.startswith("solar_"): return "Solar Ramp (RTO-old)"
    if k.startswith("wind_"): return "Wind Ramp (RTO-old)"
    if k.startswith("net_load_"): return "Net-Load Ramp"
    if k.startswith("load_west_") or k.startswith("load_midatl_") or k.startswith("load_south_") or k.startswith("load_rto_"):
        return "Load Ramp (Region)"
    if k.startswith("thermal_"): return "Thermal"
    if k.startswith("rt_") or k.startswith("da_"): return "LMP Hub Profile"
    if k.startswith("cong_"): return "Congestion"
    if k.startswith("dart_") or k.startswith("spread_"): return "DART / Hub Spread"
    if "_cash" in k or k.startswith("gas_basis"): return "Gas"
    if k.startswith("temp") or k in ("hdd", "cdd"): return "Weather"
    if "outage" in k: return "Outages"
    if k in ("month", "day_of_week_number", "is_weekend", "is_nerc_holiday", "is_federal_holiday", "summer_winter"):
        return "Calendar"
    return "Other"


REGIONAL_CAT_COLORS = {
    "Solar Ramp (WEST)": "#e65100",
    "Solar Ramp (RFC)": "#ef6c00",
    "Solar Ramp (RTO)": "#fb8c00",
    "Solar Ramp (SOUTH)": "#ffa726",
    "Solar Ramp (MIDATL)": "#ffb74d",
    "Solar Ramp (RTO-old)": "#ffcc80",
    "Wind Ramp (WEST)": "#01579b",
    "Wind Ramp (RFC)": "#0277bd",
    "Wind Ramp (RTO)": "#0288d1",
    "Wind Ramp (SOUTH)": "#039be5",
    "Wind Ramp (MIDATL)": "#29b6f6",
    "Wind Ramp (RTO-old)": "#81d4fa",
    "Renewables (WEST)": "#1b5e20",
    "Renewables (RFC)": "#2e7d32",
    "Renewables (RTO)": "#388e3c",
    "Renewables (SOUTH)": "#43a047",
    "Renewables (MIDATL)": "#66bb6a",
    "Cross-Region Diff": "#7b1fa2",
    "Penetration Share": "#9c27b0",
    "Coincidence (WEST/WEST)": "#ad1457",
    "Ramp DoD Change": "#bf360c",
}


def cat_color_regional(c: str) -> str:
    if c in REGIONAL_CAT_COLORS:
        return REGIONAL_CAT_COLORS[c]
    return _cat_color(c)


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def build_master_daily() -> tuple[pd.DataFrame, pd.DataFrame, dict, dict]:
    """Returns (daily, capacity_growth, profile, wide_by_area)."""
    print("Loading LMPs / load / fuel-mix / weather / gas / outages / calendar...")
    lmp = load_lmps_wide()
    fuel = load_fuel_mix()
    load = load_load_wide()
    weather = load_weather_daily()
    gas = load_gas_daily()
    outages = load_outages_daily()
    cal = load_calendar()

    # Filter to study window
    for nm, d in [("lmp", lmp), ("fuel", fuel), ("load", load)]:
        d.drop(d[d["date"] < START_DATE].index, inplace=True)

    print("Loading regional solar / wind...")
    solar = load_regional_solar()
    wind = load_regional_wind()
    print(f"  solar rows: {len(solar):,}  wind rows: {len(wind):,}")

    # Hourly merges (date, hour_ending) for non-regional sources
    hourly = lmp.merge(fuel, on=["date", "hour_ending"], how="outer")
    hourly = hourly.merge(load, on=["date", "hour_ending"], how="outer")
    hourly = hourly.sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # ===== Dynamics tables =====
    print("Building dynamics tables...")
    cap_growth = dynamics_capacity_growth(solar, wind)
    profile = dynamics_diurnal_profile(solar, wind)

    # ===== Daily features =====
    print("Building RTO solar/wind/net-load features (baselines)...")
    daily_parts = []
    daily_parts.append(ramp_features(_wide(hourly, "solar"), "solar"))
    daily_parts.append(solar_specific_features(_wide(hourly, "solar")))
    daily_parts.append(ramp_features(_wide(hourly, "wind"), "wind"))
    if "load_rto" in hourly and "solar" in hourly and "wind" in hourly:
        hourly["net_load"] = hourly["load_rto"] - hourly["solar"].fillna(0) - hourly["wind"].fillna(0)
        daily_parts.append(ramp_features(_wide(hourly, "net_load"), "net_load"))

    print("Building load ramp features by region...")
    for region in [r.lower() for r in LOAD_REGIONS]:
        col = f"load_{region}"
        if col in hourly.columns:
            daily_parts.append(ramp_features(_wide(hourly, col), col))

    print("Building hub LMP profile features...")
    for _, slug in TARGET_HUBS:
        for mkt in ("da", "rt"):
            col = f"lmp_total_{mkt}_{slug}"
            if col in hourly.columns:
                daily_parts.append(ramp_features(_wide(hourly, col), f"{mkt}_{slug}", include_specific=False))

    daily = pd.concat(daily_parts, axis=1).reset_index()

    print("Building REGIONAL renewable ramp features...")
    regional_daily, wide_by_area = build_regional_renewable_features(solar, wind)
    daily = daily.merge(regional_daily, on="date", how="outer")

    # Scalar daily merges
    daily = daily.merge(weather, on="date", how="left")
    daily = daily.merge(gas, on="date", how="left")
    daily = daily.merge(outages, on="date", how="left")
    daily = daily.merge(cal, on="date", how="left")

    # Cross-region features
    print("Building cross-region interaction features...")
    cross = build_cross_region_features(daily.set_index("date"))
    daily = daily.merge(cross.reset_index(), on="date", how="left")

    # Hub LMP lags / spreads (Western-focused)
    base_western_rt = "rt_western_avg_onpeak"
    base_western_da = "da_western_avg_onpeak"
    if base_western_da in daily:
        daily["da_western_lag1"] = daily[base_western_da].shift(1)
        daily["da_western_lag7"] = daily[base_western_da].shift(7)
        daily["da_western_ma5"] = daily[base_western_da].rolling(5).mean()
        daily["da_western_ma20"] = daily[base_western_da].rolling(20).mean()
    if base_western_rt in daily:
        daily["rt_western_lag1"] = daily[base_western_rt].shift(1)
        daily["rt_western_ma5"] = daily[base_western_rt].rolling(5).mean()

    daily = daily.sort_values("date").reset_index(drop=True)
    print(f"  daily frame: {len(daily):,} days x {len(daily.columns)} cols")
    return daily, cap_growth, profile, wide_by_area


def build_targets(daily: pd.DataFrame) -> pd.DataFrame:
    df = daily.copy()
    next_is_onpeak = (
        (df["is_weekend"].shift(-1) == False) & (df["is_nerc_holiday"].shift(-1) == False)
    )
    for _, slug in TARGET_HUBS:
        col = f"da_{slug}_avg_onpeak"
        if col in df.columns:
            t = df[col].shift(-1).where(next_is_onpeak)
            df[f"target_{slug}_da_t1"] = t
            df[f"target_{slug}_da_t1_chg"] = t - df[col]
    return df


def gather_indicators(df: pd.DataFrame) -> dict[str, pd.Series]:
    skip = {"date"}
    skip |= {c for c in df.columns if c.startswith("target_")}
    out = {}
    for c in df.columns:
        if c in skip:
            continue
        if not pd.api.types.is_numeric_dtype(df[c]):
            continue
        if df[c].notna().sum() < 100:
            continue
        out[c] = df[c]
    return out


def run_tests(daily: pd.DataFrame) -> pd.DataFrame:
    target_specs = []
    for _, slug in TARGET_HUBS:
        if f"target_{slug}_da_t1" in daily.columns:
            target_specs.append((slug, f"target_{slug}_da_t1", f"target_{slug}_da_t1_chg"))

    indicators = gather_indicators(daily)
    print(f"  {len(indicators)} indicators x {len(target_specs)} targets")

    rows = []
    for ind_key, series in indicators.items():
        x = series.to_numpy(dtype=float)
        cat = categorize_regional(ind_key)
        for slug, t_lvl, t_chg in target_specs:
            y = daily[t_lvl].to_numpy(dtype=float)
            yc = daily[t_chg].to_numpy(dtype=float)
            stats = test_indicator(x, y, yc)
            rows.append({
                "indicator": ind_key, "category": cat, "target": slug,
                **stats,
                "abs_corr": abs(stats["corr_level"]) if not pd.isna(stats["corr_level"]) else np.nan,
            })
    return pd.DataFrame(rows)


# ============================================================================
# HTML REPORT
# ============================================================================

def render_html(
    results: pd.DataFrame,
    cap_growth: pd.DataFrame,
    profile: pd.DataFrame,
    coincidence: pd.DataFrame,
    n_days: int, date_range: tuple,
) -> str:
    hubs = [s for _, s in TARGET_HUBS]
    cats = sorted(results["category"].unique())

    # Build heatmap (Western Hub priority, others secondary)
    heat = results.pivot_table(index="indicator", columns="target", values="corr_level")
    heat["abs_western"] = heat["western"].abs()
    heat["mean_abs"] = heat[hubs].abs().mean(axis=1)
    heat = heat.sort_values("abs_western", ascending=False)
    cat_lookup = results.drop_duplicates("indicator").set_index("indicator")["category"]

    # Top per-hub
    top_per_hub = {}
    for hub in hubs:
        h = results[results["target"] == hub].copy()
        h["abs_corr_finite"] = h["abs_corr"].fillna(-1)
        top_per_hub[hub] = h.sort_values("abs_corr_finite", ascending=False).head(15)

    # Western-Hub-only ramp focus tables (filter to renewable categories only)
    renewable_cats = [c for c in cats if any(s in c for s in ["Solar", "Wind", "Renewables", "Cross-Region", "Penetration", "Coincidence", "DoD"])]
    western_renew = results[(results["target"] == "western") & (results["category"].isin(renewable_cats))].copy()
    western_renew["abs_corr_finite"] = western_renew["abs_corr"].fillna(-1)
    western_renew_top = western_renew.sort_values("abs_corr_finite", ascending=False).head(40)

    css = """
    *{margin:0;padding:0;box-sizing:border-box;}
    body{font-family:-apple-system,Arial,sans-serif;background:#f7f8fa;padding:18px;max-width:1750px;margin:0 auto;color:#222;}
    h1{margin-bottom:4px;}
    h2{margin:24px 0 10px;color:#1565c0;border-bottom:2px solid #1565c0;padding-bottom:6px;}
    h3{color:#444;margin-bottom:8px;}
    .sub{color:#666;font-size:13px;margin-bottom:18px;}
    .card{background:white;border-radius:8px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,0.06);margin-bottom:16px;}
    table{width:100%;border-collapse:collapse;font-size:11.5px;}
    th{background:#2c3e50;color:white;padding:6px 8px;text-align:left;cursor:pointer;position:sticky;top:0;}
    td{padding:4px 8px;border-bottom:1px solid #f0f0f0;}
    td.num{text-align:right;font-family:'JetBrains Mono',Consolas,monospace;font-size:11px;}
    tr:hover td{background:#f4f8ff;}
    .cat-pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:9.5px;font-weight:bold;color:white;}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:14px;}
    .grid-2{display:grid;grid-template-columns:1fr 1fr;gap:14px;}
    .small{font-size:11px;color:#666;}
    .pos{color:#2e7d32;font-weight:bold;}
    .neg{color:#c62828;font-weight:bold;}
    .filter{margin-bottom:10px;display:flex;gap:6px;flex-wrap:wrap;}
    .fbtn{padding:5px 11px;border:1px solid #ccc;border-radius:14px;cursor:pointer;font-size:11px;background:white;}
    .fbtn.active{background:#1565c0;color:white;border-color:#1565c0;}
    .heat td{font-family:'JetBrains Mono',Consolas,monospace;font-size:10.5px;text-align:right;}
    .insight{background:#e3f2fd;border-left:4px solid #1565c0;padding:10px 14px;margin:8px 0;border-radius:0 4px 4px 0;font-size:13px;}
    .focus{background:#fff3e0;border-left:4px solid #e65100;padding:10px 14px;margin:8px 0;border-radius:0 4px 4px 0;font-size:13px;}
    canvas{max-width:100%;}
    """

    parts = []
    parts.append(f"<!DOCTYPE html><html><head><meta charset='UTF-8'><title>PJM Western Hub Renewables Ramp Study</title>")
    parts.append("<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>")
    parts.append(f"<style>{css}</style></head><body>")
    parts.append("<h1>PJM Western Hub: Regional Solar &amp; Wind Ramp Study</h1>")
    parts.append(
        f"<p class='sub'>{len(results['indicator'].unique())} indicators &middot; "
        f"{len(hubs)} hub targets (Western Hub primary) &middot; {n_days:,} days &middot; "
        f"{date_range[0]} &rarr; {date_range[1]} &middot; "
        "regional renewables from <code>pjm.solar_generation_by_area</code> + "
        "<code>pjm.wind_generation_by_area</code> &middot; "
        "target = next-day on-peak DA LMP (HE 8-23 weekday non-holiday)</p>"
    )

    # ===== PART 1: DYNAMICS =====
    parts.append("<h2>Part 1 &mdash; Dynamics: Solar &amp; Wind by PJM Area</h2>")

    # 1a. Capacity growth table
    parts.append("<div class='card'><h3>Annual Avg / Peak MW by Area</h3>")
    parts.append("<div class='grid-2'>")
    parts.append("<div><h4>Solar</h4><canvas id='solarGrowth' height='200'></canvas></div>")
    parts.append("<div><h4>Wind</h4><canvas id='windGrowth' height='200'></canvas></div>")
    parts.append("</div></div>")

    # 1b. Diurnal profile chart
    parts.append("<div class='card'><h3>Diurnal Profile (Last 12 Months) by Area</h3>")
    parts.append("<div class='grid-2'>")
    parts.append("<div><h4>Solar (summer)</h4><canvas id='solarSummer' height='200'></canvas></div>")
    parts.append("<div><h4>Solar (winter)</h4><canvas id='solarWinter' height='200'></canvas></div>")
    parts.append("<div><h4>Wind (summer)</h4><canvas id='windSummer' height='200'></canvas></div>")
    parts.append("<div><h4>Wind (winter)</h4><canvas id='windWinter' height='200'></canvas></div>")
    parts.append("</div></div>")

    # 1c. Coincidence table
    parts.append("<div class='card'><h3>Coincidence with Western Hub Peak Prices</h3>")
    parts.append("<p class='small'>Correlation between each area's avg renewable output during HE 17-20 and the same day's Western Hub on-peak DA LMP. "
                 "Negative = renewables suppress price. Top decile spike days isolated to test if the relationship strengthens during stress.</p>")
    parts.append("<table><tr><th>Area</th><th>Source</th><th>n days</th><th>Avg Super-Peak MW</th>"
                 "<th>Corr w/ West DA (all days)</th><th>Corr on Spike Days (top 10%)</th></tr>")
    for r in coincidence.itertuples():
        cls_all = "pos" if (not pd.isna(r.corr_with_west_da) and r.corr_with_west_da > 0) else "neg"
        cls_spike = "pos" if (not pd.isna(r.corr_on_spike_days) and r.corr_on_spike_days > 0) else "neg"
        ca = "—" if pd.isna(r.corr_with_west_da) else f"{r.corr_with_west_da:+.3f}"
        cs = "—" if pd.isna(r.corr_on_spike_days) else f"{r.corr_on_spike_days:+.3f}"
        parts.append(
            f"<tr><td><b>{r.area}</b></td><td>{r.source}</td><td class='num'>{r.n_days}</td>"
            f"<td class='num'>{r.avg_super_peak_mw:,.0f}</td>"
            f"<td class='num {cls_all}'>{ca}</td><td class='num {cls_spike}'>{cs}</td></tr>"
        )
    parts.append("</table></div>")

    # ===== PART 2: WESTERN-HUB FOCUS =====
    parts.append("<h2>Part 2 &mdash; Western Hub Backtest Results</h2>")
    parts.append("<div class='focus'><b>Focus:</b> next-day Western Hub on-peak DA LMP. Renewable-driven indicators only. "
                 "Quintile spread = avg next-day price when indicator is in top 20% minus avg when in bottom 20% ($/MWh).</div>")

    parts.append("<div class='card'><h3>Top 40 Renewable Indicators &mdash; Western Hub Next-Day DA</h3>")
    parts.append("<table><tr><th>#</th><th>Indicator</th><th>Category</th><th>n</th>"
                 "<th>Corr (Lvl)</th><th>Corr (Chg)</th><th>R²</th><th>Dir Acc</th>"
                 "<th>IC</th><th>p</th><th>QSpread $</th></tr>")
    for i, r in enumerate(western_renew_top.itertuples(), 1):
        cls = "pos" if (not pd.isna(r.corr_level) and r.corr_level > 0) else "neg"
        dir_cls = "pos" if (not pd.isna(r.dir_acc) and r.dir_acc > 0.55) else ("neg" if (not pd.isna(r.dir_acc) and r.dir_acc < 0.45) else "")
        pval = "<0.001" if (not pd.isna(r.ic_pval) and r.ic_pval < 0.001) else (f"{r.ic_pval:.3f}" if not pd.isna(r.ic_pval) else "—")
        def f(v, fmt="{:.3f}"):
            return "—" if pd.isna(v) else fmt.format(v)
        parts.append(
            f"<tr><td>{i}</td><td class='small'>{r.indicator}</td>"
            f"<td><span class='cat-pill' style='background:{cat_color_regional(r.category)};'>{r.category}</span></td>"
            f"<td class='num'>{r.n}</td>"
            f"<td class='num {cls}'>{f(r.corr_level)}</td>"
            f"<td class='num'>{f(r.corr_change)}</td>"
            f"<td class='num'>{f(r.r2)}</td>"
            f"<td class='num {dir_cls}'>{f(r.dir_acc, '{:.1%}')}</td>"
            f"<td class='num'>{f(r.ic)}</td><td class='num'>{pval}</td>"
            f"<td class='num'>{f(r.qspread, '{:.1f}')}</td></tr>"
        )
    parts.append("</table></div>")

    # ===== Heatmap by hub =====
    parts.append("<h2>Cross-Hub: Regional Renewable Ramps vs All Hubs</h2>")
    parts.append("<p class='small'>Top 50 indicators ranked by |corr| against Western Hub. "
                 "Heatmap shows whether the same indicator generalizes to other hubs.</p>")
    parts.append("<div class='card'><div style='overflow-x:auto;'>")
    parts.append("<table class='heat'><tr><th>Indicator</th><th>Cat</th>")
    for h in hubs:
        parts.append(f"<th>{h}</th>")
    parts.append("<th>Mean |Corr|</th></tr>")
    for ind in heat.head(50).index:
        cat = cat_lookup.get(ind, "Other")
        parts.append(f"<tr><td class='small'>{ind}</td>"
                     f"<td><span class='cat-pill' style='background:{cat_color_regional(cat)};'>{cat}</span></td>")
        for h in hubs:
            v = heat.loc[ind, h]
            color = _color_corr(v)
            text = "—" if pd.isna(v) else f"{v:+.3f}"
            parts.append(f"<td style='background:{color};'>{text}</td>")
        m = heat.loc[ind, "mean_abs"]
        parts.append(f"<td class='num'><b>{m:.3f}</b></td></tr>")
    parts.append("</table></div></div>")

    # ===== Top per other hub (compact) =====
    parts.append("<h2>Other Hub Snapshots (Top 10 Each)</h2>")
    parts.append("<div class='grid'>")
    for hub in hubs:
        parts.append(f"<div class='card'><h3>{hub.upper()}</h3>")
        parts.append("<table><tr><th>#</th><th>Indicator</th><th>Cat</th><th>Corr</th><th>R²</th></tr>")
        for i, r in enumerate(top_per_hub[hub].head(10).itertuples(), 1):
            cls = "pos" if (not pd.isna(r.corr_level) and r.corr_level > 0) else "neg"
            parts.append(
                f"<tr><td>{i}</td><td class='small'>{r.indicator}</td>"
                f"<td><span class='cat-pill' style='background:{cat_color_regional(r.category)};'>{r.category}</span></td>"
                f"<td class='num {cls}'>{r.corr_level:.3f}</td>"
                f"<td class='num'>{r.r2:.3f}</td></tr>"
            )
        parts.append("</table></div>")
    parts.append("</div>")

    # ===== All results filterable =====
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

    parts.append("<table id='full'><tr><th>Indicator</th><th>Cat</th><th>Hub</th><th>n</th>"
                 "<th>Corr (L)</th><th>Corr (C)</th><th>R²</th><th>Dir</th><th>IC</th><th>p</th><th>QSpread</th></tr>")
    sr = results.copy()
    sr["abs_corr_fill"] = sr["abs_corr"].fillna(-1)
    sr = sr.sort_values("abs_corr_fill", ascending=False)
    for r in sr.itertuples():
        cls = "pos" if (not pd.isna(r.corr_level) and r.corr_level > 0) else "neg"
        dir_cls = "pos" if (not pd.isna(r.dir_acc) and r.dir_acc > 0.55) else ("neg" if (not pd.isna(r.dir_acc) and r.dir_acc < 0.45) else "")
        pval = "<0.001" if (not pd.isna(r.ic_pval) and r.ic_pval < 0.001) else (f"{r.ic_pval:.3f}" if not pd.isna(r.ic_pval) else "—")
        def f(v, fmt="{:.3f}"):
            return "—" if pd.isna(v) else fmt.format(v)
        parts.append(
            f"<tr data-cat='{r.category}' data-hub='{r.target}'>"
            f"<td class='small'>{r.indicator}</td>"
            f"<td><span class='cat-pill' style='background:{cat_color_regional(r.category)};'>{r.category}</span></td>"
            f"<td>{r.target}</td><td class='num'>{r.n}</td>"
            f"<td class='num {cls}'>{f(r.corr_level)}</td>"
            f"<td class='num'>{f(r.corr_change)}</td>"
            f"<td class='num'>{f(r.r2)}</td>"
            f"<td class='num {dir_cls}'>{f(r.dir_acc, '{:.1%}')}</td>"
            f"<td class='num'>{f(r.ic)}</td><td class='num'>{pval}</td>"
            f"<td class='num'>{f(r.qspread, '{:.1f}')}</td></tr>"
        )
    parts.append("</table></div>")

    # ===== JS: filters + Chart.js charts =====
    cap_chart = _capacity_chart_js(cap_growth)
    profile_chart = _profile_chart_js(profile)
    parts.append(f"""
<script>
let f={{hub:'ALL',cat:'ALL'}};
function filt(k,v,btn){{
  f[k]=v;
  btn.parentElement.querySelectorAll('.fbtn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('#full tr[data-cat]').forEach(tr=>{{
    const okC = f.cat==='ALL' || tr.dataset.cat===f.cat;
    const okH = f.hub==='ALL' || tr.dataset.hub===f.hub;
    tr.style.display = (okC && okH) ? '' : 'none';
  }});
}}
{cap_chart}
{profile_chart}
</script>
</body></html>""")
    return "".join(parts)


def _capacity_chart_js(cap: pd.DataFrame) -> str:
    """Build Chart.js JS for two stacked area-by-year line charts."""
    areas_present = [a for a in AREAS if a in cap["area"].unique()]
    years = sorted(cap["year"].unique().tolist())
    color_map = {"RTO": "#2c3e50", "RFC": "#1565c0", "WEST": "#e65100", "SOUTH": "#388e3c", "MIDATL": "#7b1fa2"}

    def _ds(metric: str) -> list:
        out = []
        for a in areas_present:
            sub = cap[cap["area"] == a].set_index("year").reindex(years)
            vals = sub[metric].fillna(0).tolist()
            out.append({
                "label": a,
                "data": vals,
                "borderColor": color_map.get(a, "#999"),
                "backgroundColor": color_map.get(a, "#999") + "22",
                "tension": 0.2,
            })
        return out

    import json
    solar_ds = json.dumps(_ds("solar_avg_mw"))
    wind_ds = json.dumps(_ds("wind_avg_mw"))
    yrs_js = json.dumps(years)
    return f"""
new Chart(document.getElementById('solarGrowth'), {{
  type:'line', data:{{labels:{yrs_js}, datasets:{solar_ds}}},
  options:{{plugins:{{legend:{{position:'bottom'}}, title:{{display:true, text:'Solar avg MW by year'}}}}}}
}});
new Chart(document.getElementById('windGrowth'), {{
  type:'line', data:{{labels:{yrs_js}, datasets:{wind_ds}}},
  options:{{plugins:{{legend:{{position:'bottom'}}, title:{{display:true, text:'Wind avg MW by year'}}}}}}
}});
"""


def _profile_chart_js(profile: pd.DataFrame) -> str:
    import json
    color_map = {"RTO": "#2c3e50", "RFC": "#1565c0", "WEST": "#e65100", "SOUTH": "#388e3c", "MIDATL": "#7b1fa2"}
    hours = list(range(1, 25))

    def _ds(source: str, season: str) -> list:
        sub = profile[(profile["source"] == source) & (profile["season"] == season)]
        out = []
        for a in AREAS:
            row = sub[sub["area"] == a].set_index("hour_ending").reindex(hours)
            vals = row["mw"].fillna(0).tolist()
            out.append({
                "label": a, "data": vals,
                "borderColor": color_map.get(a, "#999"),
                "backgroundColor": color_map.get(a, "#999") + "22",
                "tension": 0.3, "pointRadius": 1,
            })
        return out

    out = []
    for ch_id, src, sea in [
        ("solarSummer", "solar", "summer"), ("solarWinter", "solar", "winter"),
        ("windSummer", "wind", "summer"), ("windWinter", "wind", "winter"),
    ]:
        ds = json.dumps(_ds(src, sea))
        out.append(f"""
new Chart(document.getElementById('{ch_id}'), {{
  type:'line', data:{{labels:{json.dumps(hours)}, datasets:{ds}}},
  options:{{plugins:{{legend:{{position:'bottom'}}}}, scales:{{x:{{title:{{display:true,text:'HE'}}}}, y:{{title:{{display:true,text:'MW'}}}}}}}}
}});""")
    return "\n".join(out)


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    daily, cap_growth, profile, wide_by_area = build_master_daily()
    daily = build_targets(daily)

    # Coincidence analysis: West Hub DA on-peak vs each region's renewable super-peak
    if "da_western_avg_onpeak" in daily.columns:
        west_da = daily.set_index("date")["da_western_avg_onpeak"]
        coinc = dynamics_coincidence_with_west_hub_peak(
            wide_by_area["solar"], wide_by_area["wind"], west_da
        )
    else:
        coinc = pd.DataFrame()

    print("Running indicator tests...")
    results = run_tests(daily)

    n_used = int(daily.dropna(subset=["target_western_da_t1"]).shape[0])
    drng = (str(daily["date"].min().date()), str(daily["date"].max().date()))

    print("Rendering HTML...")
    html = render_html(results, cap_growth, profile, coinc, n_used, drng)
    out_html = OUTPUT_DIR / f"west_hub_renewables_backtest_{Date.today().isoformat()}.html"
    out_html.write_text(html, encoding="utf-8")

    out_csv = OUTPUT_DIR / f"west_hub_renewables_backtest_{Date.today().isoformat()}.csv"
    results.to_csv(out_csv, index=False)
    out_pq = OUTPUT_DIR / f"west_hub_renewables_daily_{Date.today().isoformat()}.parquet"
    daily.to_parquet(out_pq, index=False)

    print(f"\nReport: {out_html}")
    print(f"CSV:    {out_csv}")
    print(f"Daily:  {out_pq}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
