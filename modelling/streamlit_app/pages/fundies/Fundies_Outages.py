"""Fundies — Outages: forecast vintage heatmaps + seasonal overlay charts."""
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[2]
_MODELLING_ROOT = _APP_ROOT.parent
for path in (_APP_ROOT, _MODELLING_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from da_models.common import configs  # noqa: E402
from da_models.common.data.loader import (  # noqa: E402
    load_outages_actual,
    load_outages_forecast,
)
from html_reports.fragments.outages import (  # noqa: E402
    _OUTAGE_TYPES,
    _build_seasonal_chart,
    _render_heatmap_table,
)

st.title("Fundies — Outages")
st.caption(
    "Forecast outage vintage heatmaps (Total / Planned / Forced / Maint) "
    "and historical seasonal overlays."
)


@st.cache_data(show_spinner="Loading outages forecast parquet...")
def _load_forecast() -> pd.DataFrame:
    df = load_outages_forecast(cache_dir=configs.CACHE_DIR)
    return df.copy() if df is not None else pd.DataFrame()


@st.cache_data(show_spinner="Loading outages actuals parquet...")
def _load_actual() -> pd.DataFrame:
    df = load_outages_actual(cache_dir=configs.CACHE_DIR)
    if df is None or len(df) == 0:
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["day_of_year"] = df["date"].dt.dayofyear
    return df


with st.sidebar:
    st.header("Outages")
    if st.button("Refresh"):
        _load_forecast.clear()
        _load_actual.clear()
        st.rerun()

forecast_df = _load_forecast()
actual_df = _load_actual()

regions = sorted(
    set(forecast_df.get("region", pd.Series(dtype=str)).dropna().unique())
    | set(actual_df.get("region", pd.Series(dtype=str)).dropna().unique())
)
if not regions:
    st.error("No outage data available — check that the cache parquets exist.")
    st.stop()

default_region = configs.LOAD_REGION if configs.LOAD_REGION in regions else regions[0]
region = st.sidebar.selectbox(
    "Region",
    regions,
    index=regions.index(default_region),
)

available_years = sorted(actual_df["year"].dropna().unique().tolist()) if "year" in actual_df else []
selected_years = st.sidebar.multiselect(
    "Years (seasonal overlay)",
    available_years,
    default=available_years,
    help="Filters the seasonal overlay charts only.",
)

include_seasonal = st.sidebar.checkbox("Include seasonal overlay", value=True)

# ── Forecast vintage heatmaps ─────────────────────────────────
st.subheader(f"Forecast Vintage Heatmaps — {region}")

fcst = forecast_df[forecast_df["region"] == region].copy() if len(forecast_df) else forecast_df
if len(fcst) == 0:
    st.warning(f"No forecast outage data for region {region}.")
else:
    if "forecast_rank" in fcst.columns:
        fcst = fcst.sort_values("forecast_rank", ascending=False)
    else:
        fcst = fcst.sort_values("forecast_execution_date", ascending=False)
    fcst = fcst.drop_duplicates(subset=["forecast_execution_date", "date"], keep="first")

    exec_dates = sorted(fcst["forecast_execution_date"].unique(), reverse=True)[:8]
    fcst = fcst[fcst["forecast_execution_date"].isin(exec_dates)]

    if len(fcst) == 0:
        st.warning("No recent forecast data.")
    else:
        for type_label, col in _OUTAGE_TYPES:
            if col not in fcst.columns:
                continue
            st.markdown(f"**{type_label}**")
            st.html(_render_heatmap_table(fcst, col, type_label, exec_dates))

# ── Seasonal overlay ──────────────────────────────────────────
if include_seasonal:
    st.subheader(f"Seasonal Overlay — {region}")

    seasonal = actual_df[actual_df["region"] == region].copy() if len(actual_df) else actual_df
    if selected_years:
        seasonal = seasonal[seasonal["year"].isin(selected_years)]

    if len(seasonal) == 0:
        st.warning(
            f"No historical outage data for {region}"
            + (f" in years {selected_years}." if selected_years else ".")
        )
    else:
        for type_label, col in _OUTAGE_TYPES:
            if col not in seasonal.columns:
                continue
            fig = _build_seasonal_chart(seasonal, col, f"{type_label} (Seasonal)")
            st.plotly_chart(fig, use_container_width=True)
