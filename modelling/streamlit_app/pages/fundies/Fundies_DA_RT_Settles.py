"""Fundies — DA vs RT LMP: hourly profile for a single date, all hubs stacked."""
from __future__ import annotations

from datetime import date as date_type
from pathlib import Path
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[2]
_MODELLING_ROOT = _APP_ROOT.parent
for path in (_APP_ROOT, _MODELLING_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from da_models.common import configs  # noqa: E402
from da_models.common.data.loader import load_lmps_da, load_lmps_rt  # noqa: E402

ON_PEAK_HOURS = list(range(8, 24))          # HE 8–23
OFF_PEAK_HOURS = list(range(1, 8)) + [24]   # HE 1–7, 24
ALL_HOURS = list(range(1, 25))

# Display order — edit here to reorder/add hubs.
HUBS_ORDERED = (
    "WESTERN HUB",
    "DOMINION HUB",
    "AEP-DAYTON HUB",
    "AEP GEN HUB",
)

st.title("Fundies — DA vs RT LMP")
st.caption(
    "Hourly DA / RT / DART (DA − RT) for a single date, "
    "stacked by hub: Western → Dominion → AEP."
)


@st.cache_data(show_spinner="Loading DA LMPs parquet...")
def _load_da() -> pd.DataFrame:
    df = load_lmps_da(cache_dir=configs.CACHE_DIR)
    if df is None or len(df) == 0:
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = df["hour_ending"].replace(0, 24)
    df = df[df["hour_ending"].between(1, 24)]
    return df


@st.cache_data(show_spinner="Loading RT LMPs parquet...")
def _load_rt() -> pd.DataFrame:
    df = load_lmps_rt(cache_dir=configs.CACHE_DIR)
    if df is None or len(df) == 0:
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = df["hour_ending"].replace(0, 24)
    df = df[df["hour_ending"].between(1, 24)]
    return df


# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("DA vs RT LMP")
    if st.button("Refresh"):
        _load_da.clear()
        _load_rt.clear()
        st.rerun()

da_df = _load_da()
rt_df = _load_rt()

if len(da_df) == 0 or len(rt_df) == 0:
    st.error("Missing LMP data — check that pjm_lmps_hourly parquet exists in cache.")
    st.stop()

available_hubs = set(da_df["region"].unique()) & set(rt_df["region"].unique())
hubs_to_show = [h for h in HUBS_ORDERED if h in available_hubs]
missing_hubs = [h for h in HUBS_ORDERED if h not in available_hubs]
if missing_hubs:
    st.sidebar.warning(f"Not in data: {', '.join(missing_hubs)}")

data_min = max(da_df["date"].min(), rt_df["date"].min())
data_max = min(da_df["date"].max(), rt_df["date"].max())

target_date: date_type = st.sidebar.date_input(
    "Date",
    value=data_max,
    min_value=data_min,
    max_value=data_max,
)

st.caption(f"Date: {target_date}  ·  {len(hubs_to_show)} hub(s)")


# ── Per-hub view ───────────────────────────────────────────────────────────
def _hourly_for_hub(df: pd.DataFrame, hub: str) -> pd.Series:
    """Return a Series indexed HE1..HE24 of mean LMP for the slice."""
    sub = df[(df["region"] == hub) & (df["date"] == target_date)]
    if len(sub) == 0:
        return pd.Series(dtype=float, index=ALL_HOURS, name="lmp")
    by_he = sub.groupby("hour_ending")["lmp"].mean().reindex(ALL_HOURS)
    by_he.name = "lmp"
    return by_he


def _bucket_avgs(series: pd.Series) -> tuple[float, float, float]:
    on = series.reindex(ON_PEAK_HOURS).mean()
    off = series.reindex(OFF_PEAK_HOURS).mean()
    flat = series.reindex(ALL_HOURS).mean()
    return on, off, flat


def _hub_chart(da_he: pd.Series, rt_he: pd.Series, hub: str) -> go.Figure:
    dart = da_he - rt_he
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ALL_HOURS, y=da_he.values, mode="lines+markers", name="DA",
        line=dict(color="#4cc9f0", width=2), marker=dict(size=5),
    ))
    fig.add_trace(go.Scatter(
        x=ALL_HOURS, y=rt_he.values, mode="lines+markers", name="RT",
        line=dict(color="#f0b429", width=2), marker=dict(size=5),
    ))
    fig.add_trace(go.Bar(
        x=ALL_HOURS, y=dart.values, name="DART",
        marker=dict(
            color=["#34d399" if pd.notna(v) and v >= 0 else "#ef4444" for v in dart.values],
        ),
        opacity=0.55, yaxis="y2",
    ))
    # Shade on-peak block (HE 8–23).
    fig.add_vrect(x0=7.5, x1=23.5, fillcolor="gold", opacity=0.10, layer="below", line_width=0)
    fig.update_layout(
        title=dict(text=hub, font=dict(size=14)),
        height=320,
        template="plotly_dark",
        legend=dict(orientation="h", yanchor="top", y=-0.18, xanchor="left", x=0),
        margin=dict(l=60, r=60, t=40, b=40),
        xaxis=dict(title="Hour ending", tick0=1, dtick=2, range=[0.5, 24.5]),
        yaxis=dict(title="LMP ($/MWh)"),
        yaxis2=dict(title="DART ($/MWh)", overlaying="y", side="right", showgrid=False),
        bargap=0.15,
    )
    return fig


def _hourly_table(da_he: pd.Series, rt_he: pd.Series) -> "pd.io.formats.style.Styler":
    """Wide table: rows = DA/RT/DART, cols = HE1..HE24 + OnPeak/OffPeak/Flat."""
    dart = da_he - rt_he
    rows = []
    for label, series in (("DA", da_he), ("RT", rt_he), ("DART", dart)):
        on, off, flat = _bucket_avgs(series)
        row: dict[str, object] = {"Market": label}
        for he in ALL_HOURS:
            row[f"HE{he}"] = series.get(he)
        row["OnPeak"] = on
        row["OffPeak"] = off
        row["Flat"] = flat
        rows.append(row)
    table = pd.DataFrame(rows)

    he_cols = [f"HE{h}" for h in ALL_HOURS]
    bucket_cols = ["OnPeak", "OffPeak", "Flat"]
    numeric_cols = he_cols + bucket_cols
    on_peak_he_cols = [f"HE{h}" for h in ON_PEAK_HOURS]
    on_peak_highlight = on_peak_he_cols + ["OnPeak"]

    def _color_dart_row(s: pd.Series) -> list[str]:
        if s["Market"] != "DART":
            return [""] * len(s)
        out = []
        for col in s.index:
            if col == "Market":
                out.append("")
                continue
            v = s[col]
            if pd.isna(v):
                out.append("")
            elif v < 0:
                out.append("color: #ef4444")
            elif v > 0:
                out.append("color: #34d399")
            else:
                out.append("")
        return out

    return (
        table.style
        .format("{:,.2f}", subset=numeric_cols, na_rep="—")
        .set_properties(
            subset=on_peak_highlight,
            **{"background-color": "rgba(255, 215, 0, 0.10)"},
        )
        .apply(_color_dart_row, axis=1)
    )


for hub in hubs_to_show:
    st.divider()
    st.subheader(hub)

    da_he = _hourly_for_hub(da_df, hub)
    rt_he = _hourly_for_hub(rt_df, hub)

    if da_he.dropna().empty and rt_he.dropna().empty:
        st.warning(f"No DA or RT LMPs for {hub} on {target_date}.")
        continue

    st.plotly_chart(_hub_chart(da_he, rt_he, hub), use_container_width=True)
    st.dataframe(_hourly_table(da_he, rt_he), use_container_width=True, hide_index=True)
