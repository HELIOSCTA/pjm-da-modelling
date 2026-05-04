"""Fundies — Meteologica: per-as-of-date forecast profiles by forecast date."""
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
_REPO_ROOT = _MODELLING_ROOT.parent
for path in (_APP_ROOT, _MODELLING_ROOT, _REPO_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from da_models.common import configs  # noqa: E402
from da_models.common.data.loader import load_meteologica_net_load_forecast  # noqa: E402
from backend.reports._forecast_utils import (  # noqa: E402
    COLORS, FILLS, OFFPEAK_HOURS, ONPEAK_HOURS,
)

PANELS: list[tuple[str, str, str]] = [
    ("forecast_load_mw",     "Load",     COLORS["load"]),
    ("solar_forecast",       "Solar",    COLORS["solar"]),
    ("wind_forecast",        "Wind",     COLORS["wind"]),
    ("net_load_forecast_mw", "Net Load", COLORS["net_load"]),
]

st.title("Fundies — Meteologica")
st.caption(
    "Per-vintage Meteologica forecast profiles. Pick an as-of date to lock the "
    "forecast snapshot; each covered forecast date gets its own daily profile."
)


@st.cache_data(show_spinner="Loading meteologica net_load parquet...")
def _load() -> pd.DataFrame:
    df = load_meteologica_net_load_forecast(cache_dir=configs.CACHE_DIR)
    return df.copy() if df is not None else pd.DataFrame()


with st.sidebar:
    st.header("Meteologica")
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

regions = sorted(df["region"].dropna().unique())
default_region = configs.LOAD_REGION if configs.LOAD_REGION in regions else regions[0]
region = st.sidebar.selectbox("Region", regions, index=regions.index(default_region))

as_of_dates = sorted(df["as_of_date"].dropna().unique(), reverse=True)
as_of = st.sidebar.selectbox(
    "Forecast as-of date",
    as_of_dates,
    index=0,
    format_func=lambda d: pd.Timestamp(d).strftime("%Y-%m-%d (%a)"),
)

revision_offsets = st.sidebar.multiselect(
    "Overlay prior revisions",
    options=[1, 2, 3, 7],
    default=[1, 2],
    format_func=lambda n: f"as_of − {n}d",
    help="Overlay the same forecast_date as it was forecast on earlier as_of_dates.",
)

available_as_ofs = set(as_of_dates)
revision_as_ofs: list[date] = [as_of] + [
    as_of - timedelta(days=n) for n in sorted(revision_offsets)
    if (as_of - timedelta(days=n)) in available_as_ofs
]

snap = df[(df["region"] == region) & (df["as_of_date"].isin(revision_as_ofs))].copy()
if len(snap) == 0:
    st.warning("No forecast data for this as_of_date and region.")
    st.stop()

snap["hour_ending"] = snap["hour_ending"].replace(0, 24)
snap = snap[snap["hour_ending"].between(1, 24)].sort_values(
    ["as_of_date", "date", "hour_ending"]
)

current_snap = snap[snap["as_of_date"] == as_of]

forecast_dates = sorted(current_snap["date"].unique())
horizon_days = (max(forecast_dates) - as_of).days
revision_summary = (
    f" · revisions: {', '.join(str(d) for d in revision_as_ofs[1:])}"
    if len(revision_as_ofs) > 1 else ""
)
st.caption(
    f"{len(forecast_dates)} forecast date(s)  ·  "
    f"{forecast_dates[0]} → {forecast_dates[-1]} (D+{horizon_days})  ·  "
    f"{len(current_snap):,} current-vintage rows"
    f"{revision_summary}"
)


def _overview_fig(snap_df: pd.DataFrame, region_label: str) -> go.Figure:
    """Stacked area across all covered forecast dates."""
    df = snap_df.copy()
    df["datetime"] = pd.to_datetime(df["date"]) + pd.to_timedelta(df["hour_ending"], unit="h")
    cd = df[["date", "hour_ending"]].astype(str).values

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["net_load_forecast_mw"],
        mode="lines", name="Net Load", stackgroup="stack",
        line=dict(color=COLORS["net_load"], width=1), fillcolor=FILLS["net_load"],
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Net Load: %{y:,.0f} MW<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["solar_forecast"],
        mode="lines", name="Solar", stackgroup="stack",
        line=dict(color=COLORS["solar"], width=1), fillcolor=FILLS["solar"],
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Solar: %{y:,.0f} MW<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["wind_forecast"],
        mode="lines", name="Wind", stackgroup="stack",
        line=dict(color=COLORS["wind"], width=1), fillcolor=FILLS["wind"],
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Wind: %{y:,.0f} MW<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["forecast_load_mw"],
        mode="lines", name="Load",
        line=dict(color=COLORS["gross_load"], width=2),
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Load: %{y:,.0f} MW<extra></extra>",
    ))
    fig.update_layout(
        title=f"Net Load Breakdown — {region_label} (as of {as_of})",
        height=420, template="plotly_dark",
        legend=dict(orientation="h", yanchor="top", y=-0.15, x=0),
        margin=dict(l=60, r=40, t=60, b=60),
        hovermode="x unified",
    )
    fig.update_xaxes(tickformat="%a %b-%d %I %p", gridcolor="rgba(99,110,250,0.08)")
    fig.update_yaxes(title_text="MW", tickformat=".1s", gridcolor="rgba(99,110,250,0.1)")
    return fig


REVISION_DASH = ["solid", "dash", "dot", "dashdot", "longdash"]
REVISION_OPACITY = [1.0, 0.7, 0.55, 0.45, 0.4]


def _daily_fig(
    revisions: list[tuple[date, pd.DataFrame]],
    label: str,
    show_ramp: bool,
    current_as_of: date,
) -> go.Figure:
    """1x4 grid for one forecast date.

    Each panel overlays one trace per revision (current = solid bold,
    older = dashed/dotted with reduced opacity). When show_ramp is True,
    the current vintage renders as signed bars and older vintages as
    overlay lines so the comparison stays readable.
    """
    fig = make_subplots(
        rows=1, cols=4,
        subplot_titles=tuple(
            f"{name} Ramp" if show_ramp else name for _, name, _ in PANELS
        ),
        horizontal_spacing=0.05,
    )

    for rev_idx, (rev_as_of, day_df) in enumerate(revisions):
        is_current = (rev_as_of == current_as_of)
        legend_label = (
            f"as_of {rev_as_of} (current)" if is_current else f"as_of {rev_as_of}"
        )
        legendgroup = str(rev_as_of)
        dash = REVISION_DASH[min(rev_idx, len(REVISION_DASH) - 1)]
        opacity = REVISION_OPACITY[min(rev_idx, len(REVISION_OPACITY) - 1)]
        hours = day_df["hour_ending"].tolist()

        for i, (col, name, color) in enumerate(PANELS, start=1):
            showlegend = (i == 1)
            if show_ramp and is_current:
                ramp = day_df[col].diff()
                bar_colors = [
                    COLORS["ramp_up"] if (pd.notna(v) and v >= 0) else COLORS["ramp_down"]
                    for v in ramp
                ]
                fig.add_trace(go.Bar(
                    x=hours, y=ramp,
                    name=legend_label, legendgroup=legendgroup,
                    marker_color=bar_colors, opacity=0.85,
                    showlegend=showlegend,
                    hovertemplate=(
                        f"<b>{legend_label}</b><br>HE %{{x}}<br>"
                        f"{name} Ramp: %{{y:+,.0f}} MW/hr<extra></extra>"
                    ),
                ), row=1, col=i)
                if i == 1:
                    fig.add_hline(y=0, line_color="#7f8ea3", line_dash="dash",
                                  line_width=1, row=1, col=i)
            else:
                y = day_df[col].diff() if show_ramp else day_df[col]
                value_fmt = "+,.0f" if show_ramp else ",.0f"
                unit = "MW/hr" if show_ramp else "MW"
                fig.add_trace(go.Scatter(
                    x=hours, y=y,
                    mode="lines+markers" if is_current else "lines",
                    name=legend_label, legendgroup=legendgroup,
                    line=dict(color=color, width=2, dash=dash),
                    marker=dict(size=4) if is_current else None,
                    opacity=opacity,
                    showlegend=showlegend,
                    hovertemplate=(
                        f"<b>{legend_label}</b><br>HE %{{x}}<br>"
                        f"{name}: %{{y:{value_fmt}}} {unit}<extra></extra>"
                    ),
                ), row=1, col=i)

    fig.update_layout(
        title=label,
        template="plotly_dark", height=360,
        margin=dict(l=50, r=20, t=70, b=40),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
    )
    y_title = "MW/hr" if show_ramp else "MW"
    for c in range(1, 5):
        fig.update_xaxes(
            dtick=2, range=[0.5, 24.5], autorange=False, fixedrange=True,
            title_text="HE", row=1, col=c,
        )
        fig.update_yaxes(title_text=y_title, tickformat=".1s", row=1, col=c)
    return fig


def _hourly_table(day_df: pd.DataFrame, ramp: bool) -> pd.DataFrame:
    """Wide table: rows=metric, cols=HE1..HE24 + OnPeak/OffPeak/Flat."""
    indexed = day_df.set_index("hour_ending")
    rows: dict[str, dict[str, float]] = {}
    for col, name, _ in PANELS:
        s = indexed[col].reindex(range(1, 25))
        if ramp:
            s = s.diff()
        row: dict[str, float] = {f"HE{h}": s.get(h) for h in range(1, 25)}
        for label, hours in (("OnPeak", ONPEAK_HOURS),
                             ("OffPeak", OFFPEAK_HOURS),
                             ("Flat", list(range(1, 25)))):
            vals = pd.to_numeric(s.reindex(hours), errors="coerce").dropna()
            row[label] = float(vals.mean()) if not vals.empty else None
        rows[name] = row
    return pd.DataFrame.from_dict(rows, orient="index")


def _color_ramp(value):
    """Dark green for positive, dark red for negative, neutral elsewhere."""
    if pd.isna(value):
        return ""
    if value > 0:
        return "background-color: #14532d; color: #f0fdf4"
    if value < 0:
        return "background-color: #7f1d1d; color: #fef2f2"
    return ""


st.subheader(f"Overview — {region} (as of {as_of})")
st.plotly_chart(_overview_fig(current_snap, region), use_container_width=True)

st.divider()
st.subheader("Per Forecast Date")

for fd in forecast_dates:
    day_df = current_snap[current_snap["date"] == fd].sort_values("hour_ending")
    if day_df.empty:
        continue
    offset = (fd - as_of).days
    suffix = f"D+{offset}" if offset >= 0 else f"D{offset}"
    label = f"{pd.Timestamp(fd).strftime('%a %b %d, %Y')}  ·  {suffix}"

    revisions: list[tuple[date, pd.DataFrame]] = []
    for rev_as_of in revision_as_ofs:
        rev_day = snap[
            (snap["as_of_date"] == rev_as_of) & (snap["date"] == fd)
        ].sort_values("hour_ending")
        if not rev_day.empty:
            revisions.append((rev_as_of, rev_day))

    st.markdown(f"### {label}")
    show_ramp = st.toggle(
        "Show ramps",
        key=f"ramp-{region}-{as_of}-{fd}",
        help="Toggle the current-vintage chart between hourly profile and hour-over-hour ramp.",
    )
    st.plotly_chart(
        _daily_fig(revisions, label, show_ramp=show_ramp, current_as_of=as_of),
        use_container_width=True,
    )

    outright_tbl = _hourly_table(day_df, ramp=False)
    ramp_tbl = _hourly_table(day_df, ramp=True)

    st.markdown("**Hourly profile (MW)**")
    st.dataframe(
        outright_tbl.style.format("{:,.0f}", na_rep="—"),
        use_container_width=True,
    )

    st.markdown("**Hourly ramp (MW/hr)**")
    st.dataframe(
        ramp_tbl.style
            .format("{:+,.0f}", na_rep="—")
            .map(_color_ramp),
        use_container_width=True,
    )

    st.divider()
