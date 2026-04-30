"""Fundies — Compare Two Days: overlay two forecast dates from one vintage."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

_APP_ROOT = Path(__file__).resolve().parents[2]
_MODELLING_ROOT = _APP_ROOT.parent
for path in (_APP_ROOT, _MODELLING_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from da_models.common import configs  # noqa: E402
from da_models.common.data.loader import load_meteologica_net_load_forecast  # noqa: E402

PANELS: list[tuple[str, str]] = [
    ("forecast_load_mw",     "Load"),
    ("solar_forecast",       "Solar"),
    ("wind_forecast",        "Wind"),
    ("net_load_forecast_mw", "Net Load"),
]
A_COLOR = "#60a5fa"  # blue — date A across all panels
B_COLOR = "#fb923c"  # orange — date B across all panels

st.title("Fundies — Compare Two Days")
st.caption(
    "Overlay two forecast dates from the same as-of vintage. Defaults to "
    "today vs tomorrow; pick any pair in the sidebar."
)


@st.cache_data(show_spinner="Loading meteologica net_load parquet...")
def _load() -> pd.DataFrame:
    df = load_meteologica_net_load_forecast(cache_dir=configs.CACHE_DIR)
    return df.copy() if df is not None else pd.DataFrame()


with st.sidebar:
    st.header("Compare")
    if st.button("Refresh"):
        _load.clear()
        st.rerun()

df = _load()
if len(df) == 0:
    st.error("No meteologica net_load data — check that the cache parquet exists.")
    st.stop()

if "as_of_date" not in df.columns:
    st.error(
        "Loaded parquet has no as_of_date column. Rebuild the cache from the "
        "`*_historical` mart so vintage filtering is available."
    )
    st.stop()

REGION_ORDER = ["RTO", "MIDATL", "WEST", "SOUTH"]
all_regions = sorted(df["region"].dropna().unique())
ordered_regions = (
    [r for r in REGION_ORDER if r in all_regions]
    + [r for r in all_regions if r not in REGION_ORDER]
)

selected_regions = st.sidebar.multiselect(
    "Regions",
    options=ordered_regions,
    default=ordered_regions,
    help="One card per selected region.",
)
if not selected_regions:
    st.info("Select at least one region in the sidebar.")
    st.stop()

as_of_dates = sorted(df["as_of_date"].dropna().unique(), reverse=True)
as_of = st.sidebar.selectbox(
    "Forecast as-of date",
    as_of_dates,
    index=0,
    format_func=lambda d: pd.Timestamp(d).strftime("%Y-%m-%d (%a)"),
)

snap = df[(df["as_of_date"] == as_of) & (df["region"].isin(selected_regions))].copy()
if len(snap) == 0:
    st.warning("No forecast data for this as_of_date.")
    st.stop()

snap["hour_ending"] = snap["hour_ending"].replace(0, 24)
snap = snap[snap["hour_ending"].between(1, 24)].sort_values(
    ["region", "date", "hour_ending"]
)

forecast_dates = sorted(snap["date"].unique())
if not forecast_dates:
    st.warning("No forecast dates in this snapshot.")
    st.stop()


def _format_fd(d: date) -> str:
    return f"{pd.Timestamp(d).strftime('%Y-%m-%d (%a)')}  ·  D+{(d - as_of).days}"


# Default A = today (as_of), B = tomorrow (as_of + 1d) when available.
default_a = as_of if as_of in forecast_dates else forecast_dates[0]
tomorrow = as_of + timedelta(days=1)
default_b = (
    tomorrow if tomorrow in forecast_dates
    else (forecast_dates[1] if len(forecast_dates) > 1 else forecast_dates[0])
)

date_a = st.sidebar.selectbox(
    "Date A",
    forecast_dates,
    index=forecast_dates.index(default_a),
    format_func=_format_fd,
)
date_b = st.sidebar.selectbox(
    "Date B",
    forecast_dates,
    index=forecast_dates.index(default_b),
    format_func=_format_fd,
)

label_a = f"A: {date_a} (D+{(date_a - as_of).days})"
label_b = f"B: {date_b} (D+{(date_b - as_of).days})"

st.caption(
    f"as_of {as_of}  ·  {label_a}  vs  {label_b}  ·  "
    f"{len(selected_regions)} region(s)"
)


def _compare_fig(
    day_a: pd.DataFrame, day_b: pd.DataFrame,
    label_a: str, label_b: str,
) -> go.Figure:
    """2x4 grid: row 1 outright profiles, row 2 hour-over-hour ramps."""
    titles = tuple(name for _, name in PANELS) + tuple(
        f"{name} Ramp" for _, name in PANELS
    )
    fig = make_subplots(
        rows=2, cols=4,
        subplot_titles=titles,
        horizontal_spacing=0.05, vertical_spacing=0.13,
    )
    hours = list(range(1, 25))

    for i, (col, name) in enumerate(PANELS, start=1):
        a = day_a.set_index("hour_ending")[col].reindex(hours)
        b = day_b.set_index("hour_ending")[col].reindex(hours)
        a_ramp = a.diff()
        b_ramp = b.diff()

        # Row 1 — outright profile lines (A blue, B orange across all panels)
        fig.add_trace(go.Scatter(
            x=hours, y=a, mode="lines+markers", name=label_a,
            line=dict(color=A_COLOR, width=2), marker=dict(size=5),
            legendgroup="A", showlegend=(i == 1),
            hovertemplate=(
                f"<b>{label_a}</b><br>HE %{{x}}<br>"
                f"{name}: %{{y:,.0f}} MW<extra></extra>"
            ),
        ), row=1, col=i)
        fig.add_trace(go.Scatter(
            x=hours, y=b, mode="lines+markers", name=label_b,
            line=dict(color=B_COLOR, width=2),
            marker=dict(size=5, symbol="diamond"),
            legendgroup="B", showlegend=(i == 1),
            hovertemplate=(
                f"<b>{label_b}</b><br>HE %{{x}}<br>"
                f"{name}: %{{y:,.0f}} MW<extra></extra>"
            ),
        ), row=1, col=i)

        # Row 2 — grouped ramp bars (A blue, B orange)
        fig.add_trace(go.Bar(
            x=hours, y=a_ramp, name=label_a,
            marker_color=A_COLOR, opacity=0.85,
            legendgroup="A", showlegend=False,
            hovertemplate=(
                f"<b>{label_a}</b><br>HE %{{x}}<br>"
                f"{name} Ramp: %{{y:+,.0f}} MW/hr<extra></extra>"
            ),
        ), row=2, col=i)
        fig.add_trace(go.Bar(
            x=hours, y=b_ramp, name=label_b,
            marker_color=B_COLOR, opacity=0.85,
            legendgroup="B", showlegend=False,
            hovertemplate=(
                f"<b>{label_b}</b><br>HE %{{x}}<br>"
                f"{name} Ramp: %{{y:+,.0f}} MW/hr<extra></extra>"
            ),
        ), row=2, col=i)
        fig.add_hline(y=0, line_color="#7f8ea3", line_dash="dash",
                      line_width=1, row=2, col=i)

    fig.update_layout(
        template="plotly_dark", height=720,
        margin=dict(l=50, r=20, t=70, b=40),
        hovermode="x unified", barmode="group",
        legend=dict(orientation="h", yanchor="top", y=-0.10, x=0),
    )
    for c in range(1, 5):
        fig.update_xaxes(
            dtick=2, range=[0.5, 24.5], autorange=False, fixedrange=True,
            row=1, col=c,
        )
        fig.update_xaxes(
            dtick=2, range=[0.5, 24.5], autorange=False, fixedrange=True,
            title_text="HE", row=2, col=c,
        )
        fig.update_yaxes(title_text="MW",    tickformat=".1s", row=1, col=c)
        fig.update_yaxes(title_text="MW/hr", tickformat=".1s", row=2, col=c)
    return fig


for region in selected_regions:
    region_snap = snap[snap["region"] == region]
    day_a_r = region_snap[region_snap["date"] == date_a].sort_values("hour_ending")
    day_b_r = region_snap[region_snap["date"] == date_b].sort_values("hour_ending")

    with st.container(border=True):
        st.markdown(f"### {region}")
        if day_a_r.empty or day_b_r.empty:
            missing = []
            if day_a_r.empty:
                missing.append(f"A ({date_a})")
            if day_b_r.empty:
                missing.append(f"B ({date_b})")
            st.warning(f"No data for {' / '.join(missing)} in {region}.")
            continue

        st.plotly_chart(
            _compare_fig(day_a_r, day_b_r, label_a, label_b),
            use_container_width=True,
        )
