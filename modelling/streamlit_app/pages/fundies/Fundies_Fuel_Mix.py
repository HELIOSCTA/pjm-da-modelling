"""Fundies — Fuel Mix: per-fuel hourly profile + ramp over a date range."""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[2]
_MODELLING_ROOT = _APP_ROOT.parent
_REPO_ROOT = _MODELLING_ROOT.parent
for path in (_APP_ROOT, _MODELLING_ROOT, _REPO_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from da_models.common import configs  # noqa: E402
from da_models.common.data.loader import load_fuel_mix  # noqa: E402
from backend.reports._forecast_utils import (  # noqa: E402
    OFFPEAK_HOURS, ONPEAK_HOURS,
)
from html_reports.fragments.fuel_mix import (  # noqa: E402
    _FUEL_TYPES,
    _profile_and_ramp_fig,
    PROFILE_LOOKBACK_DAYS,
)

ALL_HOURS = list(range(1, 25))
LAST_N_DAYS = 3


def _bucketed_row(series: pd.Series) -> dict[str, float]:
    """HE1..HE24 + OnPeak/OffPeak/Flat means for one Series indexed by hour_ending."""
    row: dict[str, float] = {f"HE{h}": series.get(h) for h in ALL_HOURS}
    for label, hours in (
        ("OnPeak", ONPEAK_HOURS),
        ("OffPeak", OFFPEAK_HOURS),
        ("Flat", ALL_HOURS),
    ):
        vals = pd.to_numeric(series.reindex(hours), errors="coerce").dropna()
        row[label] = float(vals.mean()) if not vals.empty else None
    return row


def _last_n_day_tables(
    window: pd.DataFrame, col: str, n: int = LAST_N_DAYS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build outright + ramp tables — one row per of the last `n` days."""
    days = sorted(window["date"].dt.normalize().unique())[-n:]
    profile_rows: dict[str, dict[str, float]] = {}
    ramp_rows: dict[str, dict[str, float]] = {}
    for day in days:
        day_df = window[window["date"].dt.normalize() == day]
        profile = (
            day_df.groupby("hour_ending")[col].mean().reindex(ALL_HOURS)
        )
        label = pd.Timestamp(day).strftime("%a %Y-%m-%d")
        profile_rows[label] = _bucketed_row(profile)
        ramp_rows[label] = _bucketed_row(profile.diff())

    profile_tbl = pd.DataFrame.from_dict(profile_rows, orient="index")
    ramp_tbl = pd.DataFrame.from_dict(ramp_rows, orient="index")
    return profile_tbl, ramp_tbl


def _color_ramp(value):
    """Dark green for positive, dark red for negative."""
    if pd.isna(value):
        return ""
    if value > 0:
        return "background-color: #14532d; color: #f0fdf4"
    if value < 0:
        return "background-color: #7f1d1d; color: #fef2f2"
    return ""

st.title("Fundies — Fuel Mix")
st.caption(
    "Hourly generation profile (left) and hourly ramp (right) per fuel type. "
    "Last 3 days in the window are highlighted; older days hidden in the legend."
)


@st.cache_data(show_spinner="Loading fuel mix parquet...")
def _load() -> pd.DataFrame:
    df = load_fuel_mix(cache_dir=configs.CACHE_DIR)
    if df is None or len(df) == 0:
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["hour_ending"] = df["hour_ending"].replace(0, 24)
    df = df[df["hour_ending"].between(1, 24)]
    return df


with st.sidebar:
    st.header("Fuel Mix")
    if st.button("Refresh"):
        _load.clear()
        st.rerun()

df = _load()
if len(df) == 0:
    st.error("No fuel mix data — check that the cache parquet exists.")
    st.stop()

data_min = df["date"].min().date()
data_max = df["date"].max().date()
default_start = max(data_min, data_max - timedelta(days=PROFILE_LOOKBACK_DAYS - 1))

start_date = st.sidebar.date_input(
    "Start date",
    value=default_start,
    min_value=data_min,
    max_value=data_max,
)
end_date = st.sidebar.date_input(
    "End date",
    value=data_max,
    min_value=data_min,
    max_value=data_max,
)

if start_date > end_date:
    st.error("Start date must be on or before end date.")
    st.stop()

window = df[
    (df["date"] >= pd.Timestamp(start_date))
    & (df["date"] <= pd.Timestamp(end_date))
]

available_labels = [
    label for col, label in _FUEL_TYPES
    if col in window.columns and window[col].abs().sum() > 0
]
selected = st.sidebar.multiselect(
    "Fuel types",
    available_labels,
    default=available_labels,
)

lookback_days = (end_date - start_date).days + 1
st.caption(
    f"Window: {start_date} → {end_date}  ·  {lookback_days} day(s)  ·  {len(window):,} rows"
)

if len(window) == 0:
    st.warning("No fuel mix data in the selected window.")
    st.stop()

for col, label in _FUEL_TYPES:
    if label not in selected:
        continue
    if col not in window.columns or window[col].abs().sum() == 0:
        continue
    st.markdown(f"**{label}**")
    fig = _profile_and_ramp_fig(window, col, label, lookback_days=lookback_days)
    st.plotly_chart(fig, use_container_width=True)

    profile_tbl, ramp_tbl = _last_n_day_tables(window, col)

    st.markdown(f"*Outright (MW) — last {LAST_N_DAYS} days*")
    st.dataframe(
        profile_tbl.style.format("{:,.0f}", na_rep="—"),
        use_container_width=True,
    )

    st.markdown(f"*Ramp (MW/hr) — last {LAST_N_DAYS} days*")
    st.dataframe(
        ramp_tbl.style
            .format("{:+,.0f}", na_rep="—")
            .map(_color_ramp),
        use_container_width=True,
    )
