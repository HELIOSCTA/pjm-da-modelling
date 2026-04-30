"""Verify loader.py pairs — forecast vs actual: coverage, hourly chart, table."""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[2]
_MODELLING_ROOT = _APP_ROOT.parent
for path in (_APP_ROOT, _MODELLING_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from da_models.common import configs  # noqa: E402
from da_models.common.data import loader  # noqa: E402

DEFAULT_LOOKBACK_DAYS = 30

# Each entry pairs a forecast loader with an actual loader and tells us
# how to merge them: which columns hold the values, the join key, and any
# pre-filter to apply to the actuals frame (e.g. solar/wind actuals are
# scoped to region=RTO since the forecasts are system-wide).
SOURCES: dict[str, dict] = {
    "Load": {
        "forecast_loader": loader.load_load_forecast,
        "actual_loader":   loader.load_load_rt,
        "forecast_col":    "forecast_load_mw",
        "actual_col":      "rt_load_mw",
        "key":             ("region", "date", "hour_ending"),
        "default_region":  "RTO",
        "unit":            "MW",
    },
    "Net Load": {
        "forecast_loader": loader.load_net_load_forecast,
        "actual_loader":   loader.load_net_load_actuals,
        "forecast_col":    "net_load_forecast_mw",
        "actual_col":      "net_load_mw",
        "key":             ("region", "date", "hour_ending"),
        "default_region":  "RTO",
        "unit":            "MW",
        "notes": "PJM did not publish solar gen actuals before 2019-04-02; "
                 "net_load_mw is NaN for that period.",
    },
    "Solar": {
        "forecast_loader": loader.load_solar_forecast,
        "actual_loader":   loader.load_net_load_actuals,
        "forecast_col":    "solar_forecast",
        "actual_col":      "solar_gen_mw",
        "key":             ("date", "hour_ending"),
        "actual_filter":   {"region": "RTO"},
        "unit":            "MW",
        "notes": "Forecast is RTO-system-wide; actuals are RTO-only. "
                 "Begins 2019-04-02.",
    },
    "Wind": {
        "forecast_loader": loader.load_wind_forecast,
        "actual_loader":   loader.load_net_load_actuals,
        "forecast_col":    "wind_forecast",
        "actual_col":      "wind_gen_mw",
        "key":             ("date", "hour_ending"),
        "actual_filter":   {"region": "RTO"},
        "unit":            "MW",
    },
    "Weather": {
        "forecast_loader": loader.load_weather_forecast_hourly,
        "actual_loader":   loader.load_weather_observed_hourly,
        "forecast_col":    "temp",
        "actual_col":      "temp",
        "key":             ("date", "hour_ending"),
        "unit":            "°F",
    },
    "LMP DA vs RT": {
        "forecast_loader": loader.load_lmps_da,
        "actual_loader":   loader.load_lmps_rt,
        "forecast_col":    "lmp",
        "actual_col":      "lmp",
        "key":             ("region", "date", "hour_ending"),
        "default_region":  "WESTERN HUB",
        "unit":            "$/MWh",
        "notes": "DA settle treated as 'forecast' of RT settle.",
    },
}

st.title("Verify Loaders — Forecast vs Actual")
st.caption(
    "Loads each forecast/actual pair through `common.data.loader`, joins them on "
    "their declared key, and reports coverage, hourly overlay, error metrics, "
    "and the merged hourly table."
)


@st.cache_data(show_spinner="Loading parquets...")
def _load_pair(source_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    cfg = SOURCES[source_name]
    fc = cfg["forecast_loader"](cache_dir=configs.CACHE_DIR)
    ac = cfg["actual_loader"](cache_dir=configs.CACHE_DIR)
    fc = pd.DataFrame() if fc is None else fc.copy()
    ac = pd.DataFrame() if ac is None else ac.copy()
    for df in (fc, ac):
        if len(df) and not pd.api.types.is_datetime64_any_dtype(df["date"]):
            df["date"] = pd.to_datetime(df["date"])
    return fc, ac


# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Verify Loaders")
    source_name = st.selectbox("Source pair", list(SOURCES.keys()))
    if st.button("Refresh"):
        _load_pair.clear()
        st.rerun()

cfg = SOURCES[source_name]
fc_full, ac_full = _load_pair(source_name)

if len(fc_full) == 0 or len(ac_full) == 0:
    st.error(
        f"Missing data for {source_name}: "
        f"forecast rows={len(fc_full)}, actual rows={len(ac_full)}."
    )
    st.stop()

if cfg["forecast_col"] not in fc_full.columns:
    st.error(
        f"Forecast loader returned no '{cfg['forecast_col']}' column. "
        f"Available: {fc_full.columns.tolist()}"
    )
    st.stop()
if cfg["actual_col"] not in ac_full.columns:
    st.error(
        f"Actual loader returned no '{cfg['actual_col']}' column. "
        f"Available: {ac_full.columns.tolist()}"
    )
    st.stop()

# Pre-filter on actuals (e.g. solar/wind → region=RTO).
for k, v in cfg.get("actual_filter", {}).items():
    if k in ac_full.columns:
        ac_full = ac_full[ac_full[k] == v].copy()

keys = list(cfg["key"])
region: str | None = None
if "region" in keys:
    regions = sorted(
        set(fc_full["region"].dropna().unique())
        & set(ac_full["region"].dropna().unique())
    )
    default = cfg.get("default_region") or (regions[0] if regions else None)
    if default not in regions and regions:
        default = regions[0]
    if not regions:
        st.error("No overlapping regions between forecast and actual.")
        st.stop()
    region = st.sidebar.selectbox(
        "Region", regions, index=regions.index(default),
    )
    fc_full = fc_full[fc_full["region"] == region]
    ac_full = ac_full[ac_full["region"] == region]

# Date window — default last 30 days of the joinable range.
data_min = max(fc_full["date"].min(), ac_full["date"].min()).date()
data_max = min(fc_full["date"].max(), ac_full["date"].max()).date()
default_start = max(data_min, data_max - timedelta(days=DEFAULT_LOOKBACK_DAYS - 1))

start_date = st.sidebar.date_input(
    "Start date", default_start, min_value=data_min, max_value=data_max,
)
end_date = st.sidebar.date_input(
    "End date", data_max, min_value=data_min, max_value=data_max,
)
if start_date > end_date:
    st.error("Start must be on or before end.")
    st.stop()

start_ts = pd.Timestamp(start_date)
end_ts = pd.Timestamp(end_date)
fc_w = fc_full[(fc_full["date"] >= start_ts) & (fc_full["date"] <= end_ts)]
ac_w = ac_full[(ac_full["date"] >= start_ts) & (ac_full["date"] <= end_ts)]

if cfg.get("notes"):
    st.info(cfg["notes"])

st.caption(
    f"Pair: **{source_name}**  ·  "
    f"Window: {start_date} → {end_date}  ·  "
    + (f"Region: **{region}**" if region else "Region: n/a")
)

# ── Merge ──────────────────────────────────────────────────────────────────
fc_col = cfg["forecast_col"]
ac_col = cfg["actual_col"]
unit = cfg["unit"]

display_name = source_name
fc_label = f"Forecast {display_name} ({unit})"
ac_label = f"Actual {display_name} ({unit})"

fc_keep = fc_w[keys + [fc_col]].rename(columns={fc_col: fc_label})
ac_keep = ac_w[keys + [ac_col]].rename(columns={ac_col: ac_label})
merged = (
    fc_keep.merge(ac_keep, on=keys, how="outer")
    .sort_values(["date", "hour_ending"])
    .reset_index(drop=True)
)
merged["Error"] = merged[fc_label] - merged[ac_label]

# ── Coverage ───────────────────────────────────────────────────────────────
st.subheader("Coverage")
both_mask = merged[fc_label].notna() & merged[ac_label].notna()
fc_only = (merged[fc_label].notna() & merged[ac_label].isna()).sum()
ac_only = (merged[fc_label].isna() & merged[ac_label].notna()).sum()

# Duplicate-key detection — silent duplicates inflate joined-row counts.
fc_dup_keys = int((fc_w.groupby(keys).size() > 1).sum()) if len(fc_w) else 0
ac_dup_keys = int((ac_w.groupby(keys).size() > 1).sum()) if len(ac_w) else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Forecast rows", f"{len(fc_w):,}")
c2.metric("Actual rows",   f"{len(ac_w):,}")
c3.metric("Joined (both)", f"{int(both_mask.sum()):,}")
c4.metric(
    "Mismatched",
    f"{int(fc_only + ac_only):,}",
    help=f"Forecast-only: {int(fc_only):,}  ·  Actual-only: {int(ac_only):,}",
)

if fc_dup_keys or ac_dup_keys:
    st.warning(
        f"Duplicate join keys detected — forecast: {fc_dup_keys:,} keys with "
        f">1 row, actual: {ac_dup_keys:,} keys with >1 row. "
        f"Outer merge will produce a Cartesian-style row blow-up; "
        f"investigate the loader normalizer."
    )

both_df = merged[both_mask]
if len(both_df) == 0:
    st.warning("No overlapping rows in the window — nothing to compare.")
    st.stop()

# ── Error metrics ──────────────────────────────────────────────────────────
err = both_df["Error"]
abs_err = err.abs()
denom = both_df[ac_label].replace(0, np.nan).abs()
pct_err = (err / denom) * 100

st.subheader("Error metrics — joined rows")
m1, m2, m3, m4 = st.columns(4)
m1.metric("Bias",  f"{err.mean():,.2f} {unit}")
m2.metric("MAE",   f"{abs_err.mean():,.2f} {unit}")
m3.metric("RMSE",  f"{np.sqrt((err ** 2).mean()):,.2f} {unit}")
m4.metric("MAPE",  f"{pct_err.abs().mean():.2f} %")

# ── Hourly overlay chart ───────────────────────────────────────────────────
st.subheader(f"Hourly overlay — {source_name}")
plot_df = merged.copy()
plot_df["dt"] = (
    plot_df["date"]
    + pd.to_timedelta(plot_df["hour_ending"].astype(int) - 1, unit="h")
)
plot_df = plot_df.sort_values("dt")

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=plot_df["dt"], y=plot_df[fc_label],
    mode="lines", name=fc_label,
    line=dict(color="#4cc9f0", width=1.5),
))
fig.add_trace(go.Scatter(
    x=plot_df["dt"], y=plot_df[ac_label],
    mode="lines", name=ac_label,
    line=dict(color="#f0b429", width=1.5),
))
fig.update_layout(
    height=420, template="plotly_dark",
    xaxis_title="Datetime (HE)", yaxis_title=unit,
    legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
    margin=dict(l=60, r=20, t=30, b=40),
    hovermode="x unified",
)
st.plotly_chart(fig, use_container_width=True)

# ── Hourly table ───────────────────────────────────────────────────────────
st.subheader("Hourly table — Forecast vs Actual")
table = merged.copy()
table["date"] = table["date"].dt.date
table["Pct Error"] = (table["Error"] / table[ac_label].replace(0, np.nan).abs()) * 100

show_cols = ["date", "hour_ending"]
if "region" in keys:
    show_cols.append("region")
show_cols += [fc_label, ac_label, "Error", "Pct Error"]

table = (
    table[show_cols]
    .sort_values(["date", "hour_ending"], ascending=[False, False])
    .reset_index(drop=True)
)


def _color_error(val: float) -> str:
    if pd.isna(val):
        return ""
    if val < 0:
        return "color: #ef4444"
    if val > 0:
        return "color: #34d399"
    return ""


styler = (
    table.style
    .format(
        {
            fc_label: "{:,.2f}",
            ac_label: "{:,.2f}",
            "Error": "{:+,.2f}",
            "Pct Error": "{:+,.2f}%",
        },
        na_rep="—",
    )
    .map(_color_error, subset=["Error", "Pct Error"])
)
st.dataframe(styler, use_container_width=True, hide_index=True)

st.download_button(
    "Download merged hourly CSV",
    data=table.to_csv(index=False).encode("utf-8"),
    file_name=f"verify_{source_name.lower().replace(' ', '_')}.csv",
    mime="text/csv",
)
