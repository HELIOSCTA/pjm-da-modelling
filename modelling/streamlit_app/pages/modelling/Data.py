"""Inspect inputs (load forecast, DA LMPs) for a target date + lookback window."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
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

from da_models.common.data import loader  # noqa: E402
from da_models.like_day_model_knn import _shared  # noqa: E402
from da_models.like_day_model_knn import configs as knn_configs  # noqa: E402
from lib.ui import linked_date_pair  # noqa: E402

DEFAULT_LOOKBACK_DAYS = 7
OVERLAY_DAYS = 60

HE_COLS = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HE_COLS = [f"HE{h}" for h in range(8, 24)]
OFFPEAK_HE_COLS = [c for c in HE_COLS if c not in ONPEAK_HE_COLS]
ORDERED_LOAD_COLS = [
    "Source", "As of Date", "Date", "Region",
    "OnPeak", "OffPeak", "Flat", *HE_COLS,
]
LOAD_FMT = {col: "{:,.0f}" for col in ["OnPeak", "OffPeak", "Flat", *HE_COLS]}
ORDERED_RTO_COLS = [
    "Source", "As of Date", "Date",
    "OnPeak", "OffPeak", "Flat", *HE_COLS,
]
ORDERED_LMP_COLS = [
    "Source", "Date", "Hub",
    "OnPeak", "OffPeak", "Flat", *HE_COLS,
]
LMP_FMT = {col: "{:,.2f}" for col in ["OnPeak", "OffPeak", "Flat", *HE_COLS]}

GAS_HUBS: tuple[tuple[str, str], ...] = (
    ("gas_m3", "M3"),
    ("gas_tco", "TCO"),
    ("gas_tz6", "TZ6"),
    ("gas_dom_south", "Dom South"),
)
ORDERED_GAS_COLS = [
    "Date", "OnPeak", "OffPeak", "Flat", *HE_COLS,
]
GAS_FMT = "{:,.3f}"

LOAD_REGION_LABELS = {
    "RTO": "PJM RTO Load",
    "MIDATL": "PJM Mid-Atlantic Load",
    "WEST": "PJM Western Load",
    "SOUTH": "PJM Southern Load",
}
LOAD_REGION_ORDER = ("RTO", "MIDATL", "WEST", "SOUTH")

_LMP_HUB_OVERRIDES = {
    "AEP DAYTON HUB": "AEP-Dayton Hub",
    "N ILLINOIS HUB": "Northern Illinois Hub",
    "NEW JERSEY HUB": "New Jersey Hub",
}


def _load_region_label(region: str) -> str:
    return LOAD_REGION_LABELS.get(region, region)


def _lmp_hub_label(hub: str) -> str:
    return _LMP_HUB_OVERRIDES.get(hub, hub.title())


def _pivot_load_wide(df: pd.DataFrame, value_col: str, index_cols: list[str]) -> pd.DataFrame:
    if len(df) == 0:
        return pd.DataFrame(columns=index_cols + HE_COLS + ["OnPeak", "OffPeak", "Flat"])
    pivot = (
        df.pivot_table(
            index=index_cols, columns="hour_ending",
            values=value_col, aggfunc="mean",
        )
        .reindex(columns=range(1, 25))
    )
    pivot.columns = [f"HE{h}" for h in pivot.columns]
    pivot["OnPeak"] = pivot[ONPEAK_HE_COLS].mean(axis=1)
    pivot["OffPeak"] = pivot[OFFPEAK_HE_COLS].mean(axis=1)
    pivot["Flat"] = pivot[HE_COLS].mean(axis=1)
    return pivot.reset_index()


def _pivot_coalesced_wide(
    coalesced_window: pd.DataFrame,
    forecast_label: str = "Forecast",
    value_col: str = "load_mw",
) -> pd.DataFrame:
    """Pivot a coalesced regional frame to canonical wide form.

    Output columns: Source | As of Date | Date | Region | OnPeak | OffPeak | Flat | HE1..HE24.
    Forecast rows reconstruct As of Date as date - 1; RT rows leave it NaT.
    Sorted Date desc. ``forecast_label`` controls the display name for the
    non-RT source (e.g. ``"Forecast"`` for PJM, ``"Meteologica"`` for the
    alt-source loader). ``value_col`` is the per-row metric to pivot —
    ``"load_mw"``, ``"wind_mw"``, ``"solar_mw"`` etc. The output column
    schema (HE1..HE24, OnPeak, OffPeak, Flat) is the same regardless.
    """
    pivot = _pivot_load_wide(
        coalesced_window, value_col, ["date", "region", "source"],
    )
    if pivot.empty:
        return pd.DataFrame(columns=ORDERED_LOAD_COLS)

    date_ts = pd.to_datetime(pivot["date"])
    pivot["as_of_date"] = date_ts - pd.Timedelta(days=1)
    pivot.loc[pivot["source"] == "rt", "as_of_date"] = pd.NaT
    pivot["as_of_date"] = pivot["as_of_date"].dt.date

    pivot = pivot.rename(columns={
        "as_of_date": "As of Date",
        "date": "Date",
        "region": "Region",
        "source": "Source",
    })
    pivot["Source"] = pivot["Source"].apply(
        lambda s: "RT" if s == "rt" else forecast_label
    )
    return (
        pivot[ORDERED_LOAD_COLS]
        .sort_values("Date", ascending=False)
        .reset_index(drop=True)
    )


def _pivot_rto_coalesced_wide(
    coalesced_window: pd.DataFrame,
    value_col: str,
) -> pd.DataFrame:
    """Pivot a RTO-only coalesced frame (wind/solar) to canonical wide form.

    Mirrors ``check_loaders/pjm_wind.py::_pjm_wind_wide`` and
    ``pjm_solar.py::_pjm_solar_wide`` so streamlit and the check-loader
    output are byte-identical when given the same coalesced frame.

    Output columns: Source | As of Date | Date | OnPeak | OffPeak | Flat | HE1..HE24.
    Forecast rows reconstruct As of Date as date - 1; RT rows leave it NaT.
    Sorted Date desc.
    """
    if len(coalesced_window) == 0:
        return pd.DataFrame(columns=ORDERED_RTO_COLS)

    pivot = (
        coalesced_window.pivot_table(
            index=["date", "source"],
            columns="hour_ending",
            values=value_col,
            aggfunc="mean",
        )
        .reindex(columns=range(1, 25))
    )
    pivot.columns = [f"HE{h}" for h in pivot.columns]
    pivot["OnPeak"] = pivot[ONPEAK_HE_COLS].mean(axis=1)
    pivot["OffPeak"] = pivot[OFFPEAK_HE_COLS].mean(axis=1)
    pivot["Flat"] = pivot[HE_COLS].mean(axis=1)
    pivot = pivot.reset_index()

    date_ts = pd.to_datetime(pivot["date"])
    pivot["As of Date"] = date_ts - pd.Timedelta(days=1)
    pivot.loc[pivot["source"] != "forecast", "As of Date"] = pd.NaT
    pivot["As of Date"] = pivot["As of Date"].dt.date

    pivot = pivot.rename(columns={"date": "Date", "source": "Source"})
    pivot["Source"] = pivot["Source"].map({"forecast": "Forecast", "rt": "RT"})

    return (
        pivot[ORDERED_RTO_COLS]
        .sort_values("Date", ascending=False)
        .reset_index(drop=True)
    )


def _build_lmp_wide_pair(
    da_window: pd.DataFrame, rt_window: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """DA and RT LMP frames pivoted to canonical wide form.

    Output columns: Source | Date | Hub | OnPeak | OffPeak | Flat | HE1..HE24.
    """
    da_wide = _pivot_load_wide(da_window, "lmp", ["date", "region"])
    da_wide.insert(0, "Source", "DA")
    rt_wide = _pivot_load_wide(rt_window, "lmp", ["date", "region"])
    rt_wide.insert(0, "Source", "RT")
    rename = {"date": "Date", "region": "Hub"}
    return da_wide.rename(columns=rename), rt_wide.rename(columns=rename)

st.title("Inspect Inputs")
st.caption(
    "Lookback window of forecast vs actual for the model's two key inputs. "
    "Plot and table cover lookback → target; the forecast date (target − 1) "
    "is highlighted."
)


_KEY = ["date", "hour_ending", "region"]


def _coerce_and_dedupe(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Coerce types and dedupe by (date, hour_ending, region).

    The historical forecast parquet has multiple forecast vintages per key;
    the LMP and load actual parquets have stray duplicates. Without dedup
    every joined view multiplies rows.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df["region"] = df["region"].astype(str)
    return df.drop_duplicates(subset=_KEY, keep="first").reset_index(drop=True)


# ── Loaders ────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading coalesced load parquet...")
def _load_load_coalesced() -> pd.DataFrame:
    """Forecast-first hourly load — strict 24-HE coverage gate.

    Single source of truth for the load section: matches what the KNN pool
    builder consumes via ``loader.load_load_coalesced``. No UI-side coalesce
    rule.
    """
    return loader.load_load_coalesced(cache_dir=knn_configs.CACHE_DIR)


@st.cache_data(show_spinner="Loading Meteologica coalesced load parquet...")
def _load_meteologica_load_coalesced() -> pd.DataFrame:
    """Meteologica-first hourly load — strict 24-HE coverage gate.

    Alt-source signal for the model (visualization only for now). Mirrors
    ``_load_load_coalesced`` but uses Meteologica's DA-cutoff vintage as
    the forecast source, with PJM RT actuals as fallback.
    """
    return loader.load_meteologica_load_coalesced(cache_dir=knn_configs.CACHE_DIR)


@st.cache_data(show_spinner="Loading Meteologica coalesced wind parquet...")
def _load_meteologica_wind_coalesced() -> pd.DataFrame:
    """Meteologica-first hourly wind — strict 24-HE coverage gate.

    Regional (RTO/MIDATL/WEST/SOUTH). RT fallback uses PJM
    ``net_load_actual.wind_gen_mw``.
    """
    return loader.load_meteologica_wind_coalesced(cache_dir=knn_configs.CACHE_DIR)


@st.cache_data(show_spinner="Loading Meteologica coalesced solar parquet...")
def _load_meteologica_solar_coalesced() -> pd.DataFrame:
    """Meteologica-first hourly solar — strict 24-HE coverage gate.

    Regional (RTO/MIDATL/WEST/SOUTH). RT fallback uses PJM
    ``net_load_actual.solar_gen_mw``; pre-2019-04-02 dates have no actuals.
    """
    return loader.load_meteologica_solar_coalesced(cache_dir=knn_configs.CACHE_DIR)


@st.cache_data(show_spinner="Loading coalesced wind parquet...")
def _load_wind_coalesced() -> pd.DataFrame:
    """Forecast-first hourly RTO wind — strict 24-HE coverage gate.

    PJM wind forecast is system-wide and actuals are filtered to RTO, so
    the output is RTO-only (no region column). Mirrors
    ``loader.load_wind_coalesced`` — no UI-side coalesce rule.
    """
    return loader.load_wind_coalesced(cache_dir=knn_configs.CACHE_DIR)


@st.cache_data(show_spinner="Loading coalesced solar parquet...")
def _load_solar_coalesced() -> pd.DataFrame:
    """Forecast-first hourly RTO solar — strict 24-HE coverage gate.

    RTO-only (no region column). Pre-2019-04-02 dates have no solar
    actuals or forecast and are absent from the series.
    """
    return loader.load_solar_coalesced(cache_dir=knn_configs.CACHE_DIR)


@st.cache_data(show_spinner="Loading ICE next-day gas parquet...")
def _load_gas_prices_hourly() -> pd.DataFrame:
    """Hourly ICE next-day gas prices, 4 PJM-relevant hubs.

    Single-source — no forecast vs RT split, no coalesce. Hubs: M3 (Tetco),
    TCO (Columbia), TZ6 (Transco Z6 NY), Dom South (Dominion South). Most
    days carry intra-day price variation reflecting the gas trading-day
    rollover at HE14.
    """
    return loader.load_gas_prices_hourly(cache_dir=knn_configs.CACHE_DIR)


@st.cache_data(show_spinner="Loading DA LMPs parquet...")
def _load_lmps_da() -> pd.DataFrame:
    df = _shared.load_lmp_da(cache_dir=knn_configs.CACHE_DIR)
    return _coerce_and_dedupe(df, "lmp")


@st.cache_data(show_spinner="Loading RT LMPs parquet...")
def _load_lmps_rt() -> pd.DataFrame:
    df = loader.load_lmps_rt(cache_dir=knn_configs.CACHE_DIR)
    return _coerce_and_dedupe(df, "lmp")


DATES_PARQUET_NAME = "pjm_dates_daily.parquet"


def _dates_path() -> Path:
    return Path(knn_configs.CACHE_DIR) / DATES_PARQUET_NAME


def _file_age(path: Path) -> str:
    if not path.exists():
        return "missing"
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    age = datetime.now(timezone.utc) - mtime
    days, rem = divmod(int(age.total_seconds()), 86400)
    hours, _ = divmod(rem, 3600)
    if days:
        return f"{days}d {hours}h ago"
    minutes = (age.total_seconds() % 3600) // 60
    return f"{hours}h {int(minutes)}m ago" if hours else f"{int(minutes)}m ago"


def _to_dt(df: pd.DataFrame) -> pd.Series:
    """Build a Timestamp from date + (hour_ending - 1)."""
    return pd.to_datetime(df["date"]) + pd.to_timedelta(
        df["hour_ending"].astype("Int64").astype(int) - 1, unit="h"
    )


def _highlight_forecast_date(fig: go.Figure, forecast_date: date) -> None:
    """Shade the forecast_date region (0:00 to next-day 0:00) on a datetime plot."""
    start = pd.Timestamp(forecast_date)
    end = start + pd.Timedelta(days=1)
    fig.add_vrect(
        x0=start, x1=end,
        fillcolor="gold", opacity=0.18, layer="below", line_width=0,
        annotation_text=f"Forecast date {forecast_date}",
        annotation_position="top left",
        annotation_font=dict(size=10, color="#d4a017"),
    )


# ── Sidebar ────────────────────────────────────────────────────────────────
if st.sidebar.button("Refresh data"):
    _load_load_coalesced.clear()
    _load_meteologica_load_coalesced.clear()
    _load_meteologica_wind_coalesced.clear()
    _load_meteologica_solar_coalesced.clear()
    _load_wind_coalesced.clear()
    _load_solar_coalesced.clear()
    _load_gas_prices_hourly.clear()
    _load_lmps_da.clear()
    _load_lmps_rt.clear()
    st.rerun()

coalesced = _load_load_coalesced()
meteologica = _load_meteologica_load_coalesced()
meteologica_wind = _load_meteologica_wind_coalesced()
meteologica_solar = _load_meteologica_solar_coalesced()
wind_coalesced = _load_wind_coalesced()
solar_coalesced = _load_solar_coalesced()
gas = _load_gas_prices_hourly()
lmps_da = _load_lmps_da()
lmps_rt = _load_lmps_rt()

st.sidebar.header("Inputs")
forecast_date, target_date = linked_date_pair(key_prefix="_data_dates")

lookback_days = st.sidebar.number_input(
    "Lookback (days)",
    min_value=7, max_value=1825, value=OVERLAY_DAYS, step=1,
    key="_data_lookback_days",
    help="Window size for chart and table — applies to all cards.",
)

_all_years = sorted(
    {
        *(pd.Timestamp(d).year for d in coalesced["date"].unique()),
        *(pd.Timestamp(d).year for d in lmps_da["date"].unique()),
    },
    reverse=True,
)
years_pick = st.sidebar.multiselect(
    "Years",
    options=_all_years,
    default=_all_years,
    key="_data_years",
    help="Restrict overlay/table to selected years (empty = all).",
)
years_filter = years_pick or _all_years

load_regions_present = set(coalesced["region"].dropna().unique().tolist())
load_regions_to_render = [r for r in LOAD_REGION_ORDER if r in load_regions_present]

hubs = sorted(lmps_da["region"].dropna().unique().tolist())
default_hub = knn_configs.HUB if knn_configs.HUB in hubs else (hubs[0] if hubs else "")
hub = st.sidebar.selectbox(
    "DA LMP hub",
    hubs,
    index=hubs.index(default_hub) if default_hub in hubs else 0,
)

st.caption(
    f"Lookback **{int(lookback_days)}d** ending at target **{target_date}**  ·  "
    f"forecast date **{forecast_date}**  ·  "
    f"hub **{_lmp_hub_label(hub)}**"
)


# ── Source parquets (collapsible) ─────────────────────────────────────────
with st.expander("Source Parquets", expanded=False):
    forecast_paths = _shared.resolved_load_forecast_paths(knn_configs.CACHE_DIR)
    lmp_path = _shared.resolved_lmp_da_path(knn_configs.CACHE_DIR)

    freshness_rows = []
    for p in forecast_paths:
        freshness_rows.append({
            "kind": "load_forecast",
            "file": p.name,
            "age": _file_age(p),
            "size_mb": round(p.stat().st_size / (1024 * 1024), 2),
        })
    if lmp_path is not None:
        freshness_rows.append({
            "kind": "da_lmp",
            "file": lmp_path.name,
            "age": _file_age(lmp_path),
            "size_mb": round(lmp_path.stat().st_size / (1024 * 1024), 2),
        })
    dates_path = _dates_path()
    if dates_path.exists():
        freshness_rows.append({
            "kind": "calendar",
            "file": dates_path.name,
            "age": _file_age(dates_path),
            "size_mb": round(dates_path.stat().st_size / (1024 * 1024), 2),
        })
    if freshness_rows:
        st.dataframe(
            pd.DataFrame(freshness_rows),
            use_container_width=True,
            hide_index=True,
        )


# ── DA LMPs: hourly overlay + modelling-input table · hub ─────────────────
with st.expander(
    f"DA LMP · **{_lmp_hub_label(hub)}**",
    expanded=True,
):
    overlay_start_lmp = target_date - timedelta(days=int(lookback_days))
    da_overlay = lmps_da[
        (lmps_da["region"] == hub)
        & (lmps_da["date"] >= overlay_start_lmp)
        & (lmps_da["date"] <= target_date)
        & (pd.to_datetime(lmps_da["date"]).dt.year.isin(years_filter))
    ][["date", "hour_ending", "lmp"]]
    da_dates = set(da_overlay["date"].unique())
    rt_overlay_lmp = lmps_rt[
        (lmps_rt["region"] == hub)
        & (lmps_rt["date"] >= overlay_start_lmp)
        & (lmps_rt["date"] <= target_date)
        & (~lmps_rt["date"].isin(da_dates))
        & (pd.to_datetime(lmps_rt["date"]).dt.year.isin(years_filter))
    ][["date", "hour_ending", "lmp"]]
    overlay_lmp = pd.concat([da_overlay, rt_overlay_lmp], ignore_index=True)

    if overlay_lmp.empty:
        st.warning(f"No DA or RT LMPs in window for {hub}.")
    else:
        st.caption(
            f"Hourly overlay ending at target **{target_date}** "
            "(DA where settled, else RT)."
        )
        fig = go.Figure()
        for d in sorted(overlay_lmp["date"].unique()):
            sub = overlay_lmp[overlay_lmp["date"] == d].sort_values("hour_ending")
            fig.add_trace(go.Scatter(
                x=sub["hour_ending"], y=sub["lmp"],
                mode="lines",
                line=dict(color="#94a3b8", width=1),
                opacity=0.4,
                showlegend=False,
                hovertemplate=(
                    f"<b>{d}</b><br>HE %{{x}}<br>LMP: $%{{y:,.2f}}<extra></extra>"
                ),
            ))
        fig.update_layout(
            template="plotly_dark",
            height=420,
            xaxis_title="Hour Ending",
            yaxis_title="LMP ($/MWh)",
            legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
            margin=dict(l=60, r=20, t=30, b=40),
            hovermode="closest",
        )
        fig.update_xaxes(dtick=1, range=[0.5, 24.5])
        st.plotly_chart(fig, use_container_width=True)

    da_window = lmps_da[
        (lmps_da["region"] == hub)
        & (lmps_da["date"] >= overlay_start_lmp)
        & (lmps_da["date"] <= target_date)
        & (pd.to_datetime(lmps_da["date"]).dt.year.isin(years_filter))
    ].copy()
    rt_window = lmps_rt[
        (lmps_rt["region"] == hub)
        & (lmps_rt["date"] >= overlay_start_lmp)
        & (lmps_rt["date"] <= target_date)
        & (pd.to_datetime(lmps_rt["date"]).dt.year.isin(years_filter))
    ].copy()

    if len(da_window) == 0 and len(rt_window) == 0:
        st.warning(f"No DA or RT LMPs in table window for {hub}.")
    else:
        da_wide, rt_wide_lmp = _build_lmp_wide_pair(da_window, rt_window)

        st.markdown(
            "**Modelling Inputs** — coalesced per (Date, Hub): "
            "DA where settled, otherwise the RT fallback."
        )
        da_keys = (
            set(zip(da_wide["Date"], da_wide["Hub"]))
            if not da_wide.empty
            else set()
        )
        rt_fallback_lmp = (
            rt_wide_lmp
            if rt_wide_lmp.empty
            else rt_wide_lmp[
                ~rt_wide_lmp.apply(
                    lambda r: (r["Date"], r["Hub"]) in da_keys, axis=1
                )
            ]
        )
        model_table_lmp = (
            pd.concat([da_wide, rt_fallback_lmp], ignore_index=True)
            .sort_values(["Date"], ascending=[False])
            .reset_index(drop=True)
        )[ORDERED_LMP_COLS]
        st.dataframe(
            model_table_lmp.style.format(LMP_FMT, na_rep="—"),
            use_container_width=True,
            hide_index=True,
            height=400,
        )


# ── Load: forecast vs RT actuals for [lookback, target] · region ──────────
st.divider()
st.header("Features")


def _render_load_region(
    region_code: str,
    frame: pd.DataFrame,
    forecast_label: str = "Forecast",
    value_col: str = "load_mw",
    axis_label: str = "Load (MW)",
    hover_label: str = "Load",
) -> None:
    overlay_start = target_date - timedelta(days=int(lookback_days))
    coalesced_window = frame[
        (frame["region"] == region_code)
        & (frame["date"] >= overlay_start)
        & (frame["date"] <= target_date)
        & (pd.to_datetime(frame["date"]).dt.year.isin(years_filter))
    ].copy()

    if coalesced_window.empty:
        st.warning(f"No {hover_label.lower()} data in window for {region_code}.")
        return

    overlay = coalesced_window[["date", "hour_ending", value_col]]

    fig = go.Figure()
    historical_dates = sorted(d for d in overlay["date"].unique() if d != target_date)
    for d in historical_dates:
        sub = overlay[overlay["date"] == d].sort_values("hour_ending")
        fig.add_trace(go.Scatter(
            x=sub["hour_ending"], y=sub[value_col],
            mode="lines",
            line=dict(color="#94a3b8", width=1),
            opacity=0.4,
            showlegend=False,
            hovertemplate=(
                f"<b>{d}</b><br>HE %{{x}}<br>{hover_label}: "
                "%{y:,.0f} MW<extra></extra>"
            ),
        ))
    forecast_day = overlay[overlay["date"] == target_date].sort_values("hour_ending")
    if not forecast_day.empty:
        fig.add_trace(go.Scatter(
            x=forecast_day["hour_ending"], y=forecast_day[value_col],
            mode="lines+markers",
            name=f"Target date {target_date}",
            line=dict(color="#ef4444", width=2.5),
            marker=dict(size=5),
            hovertemplate=(
                f"<b>{target_date}</b><br>HE %{{x}}<br>"
                f"{hover_label}: %{{y:,.0f}} MW<extra></extra>"
            ),
        ))
    fig.update_layout(
        template="plotly_dark",
        height=420,
        xaxis_title="Hour Ending",
        yaxis_title=axis_label,
        legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
        margin=dict(l=60, r=20, t=30, b=40),
        hovermode="closest",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])
    st.plotly_chart(fig, use_container_width=True)

    model_table = _pivot_coalesced_wide(
        coalesced_window, forecast_label=forecast_label, value_col=value_col,
    )
    st.dataframe(
        model_table.style.format(LOAD_FMT, na_rep="—"),
        use_container_width=True,
        hide_index=True,
        height=400,
    )


with st.expander("**PJM Load**", expanded=True):
    st.caption(
        "Hourly overlay using `loader.load_load_coalesced` — the same "
        "forecast-first signal the KNN pool consumes (DA-cutoff forecast "
        "where 24-HE coverage exists, else RT actuals). Target date "
        f"**{target_date}** in red."
    )
    if not load_regions_to_render:
        st.warning("No regions present in coalesced load frame.")
    else:
        load_tabs = st.tabs(list(load_regions_to_render))
        for tab, region_code in zip(load_tabs, load_regions_to_render):
            with tab:
                _render_load_region(region_code, coalesced, forecast_label="Forecast")


# ── Wind / Solar: RTO-only coalesced (no region) ──────────────────────────
def _render_rto_series(
    frame: pd.DataFrame,
    value_col: str,
    y_axis_label: str,
    hover_label: str,
) -> None:
    """Hourly overlay + wide table for an RTO-only coalesced frame.

    Mirrors ``_render_load_region`` but skips the region filter — wind and
    solar coalesced frames are RTO-only by construction. Target date is
    highlighted in red. Wide table matches ``check_loaders/pjm_wind`` and
    ``pjm_solar`` byte-for-byte.
    """
    overlay_start = target_date - timedelta(days=int(lookback_days))
    coalesced_window = frame[
        (frame["date"] >= overlay_start)
        & (frame["date"] <= target_date)
        & (pd.to_datetime(frame["date"]).dt.year.isin(years_filter))
    ].copy()

    if coalesced_window.empty:
        st.warning(f"No {hover_label.lower()} data in window.")
        return

    overlay = coalesced_window[["date", "hour_ending", value_col]]

    fig = go.Figure()
    historical_dates = sorted(d for d in overlay["date"].unique() if d != target_date)
    for d in historical_dates:
        sub = overlay[overlay["date"] == d].sort_values("hour_ending")
        fig.add_trace(go.Scatter(
            x=sub["hour_ending"], y=sub[value_col],
            mode="lines",
            line=dict(color="#94a3b8", width=1),
            opacity=0.4,
            showlegend=False,
            hovertemplate=(
                f"<b>{d}</b><br>HE %{{x}}<br>{hover_label}: "
                "%{y:,.0f} MW<extra></extra>"
            ),
        ))
    forecast_day = overlay[overlay["date"] == target_date].sort_values("hour_ending")
    if not forecast_day.empty:
        fig.add_trace(go.Scatter(
            x=forecast_day["hour_ending"], y=forecast_day[value_col],
            mode="lines+markers",
            name=f"Target date {target_date}",
            line=dict(color="#ef4444", width=2.5),
            marker=dict(size=5),
            hovertemplate=(
                f"<b>{target_date}</b><br>HE %{{x}}<br>"
                f"{hover_label}: %{{y:,.0f}} MW<extra></extra>"
            ),
        ))
    fig.update_layout(
        template="plotly_dark",
        height=420,
        xaxis_title="Hour Ending",
        yaxis_title=y_axis_label,
        legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
        margin=dict(l=60, r=20, t=30, b=40),
        hovermode="closest",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])
    st.plotly_chart(fig, use_container_width=True)

    model_table = _pivot_rto_coalesced_wide(coalesced_window, value_col)
    st.dataframe(
        model_table.style.format(LOAD_FMT, na_rep="—"),
        use_container_width=True,
        hide_index=True,
        height=400,
    )


with st.expander("**PJM RTO Wind**", expanded=False):
    st.caption(
        "Hourly overlay using `loader.load_wind_coalesced` — DA-cutoff PJM "
        "wind forecast where 24-HE coverage exists, else RT actuals from "
        "`net_load_actual.wind_gen_mw` filtered to RTO. RTO-only (PJM wind "
        f"forecast is system-wide). Target date **{target_date}** in red."
    )
    _render_rto_series(
        wind_coalesced,
        value_col="wind_mw",
        y_axis_label="Wind (MW)",
        hover_label="Wind",
    )


with st.expander("**PJM RTO Solar**", expanded=False):
    st.caption(
        "Hourly overlay using `loader.load_solar_coalesced` — DA-cutoff PJM "
        "solar forecast where 24-HE coverage exists, else RT actuals from "
        "`net_load_actual.solar_gen_mw` filtered to RTO. RTO-only. "
        "Pre-2019-04-02 dates absent (no PJM solar actuals or forecast). "
        f"Target date **{target_date}** in red."
    )
    _render_rto_series(
        solar_coalesced,
        value_col="solar_mw",
        y_axis_label="Solar (MW)",
        hover_label="Solar",
    )


# ── ICE next-day gas: 4 hubs in one card, one tab per hub ────────────────
def _pivot_gas_hub_wide(window: pd.DataFrame, hub_col: str) -> pd.DataFrame:
    """Pivot the gas window to wide for a single hub.

    Output: Date | OnPeak | OffPeak | Flat | HE1..HE24, sorted Date desc.
    Mirrors ``check_loaders/pjm_gas.py::_pjm_gas_wide_for_hub``.
    """
    if window.empty or hub_col not in window.columns:
        return pd.DataFrame(columns=ORDERED_GAS_COLS)
    pivot = (
        window.pivot_table(
            index="date", columns="hour_ending", values=hub_col, aggfunc="mean",
        )
        .reindex(columns=range(1, 25))
    )
    pivot.columns = [f"HE{h}" for h in pivot.columns]
    pivot["OnPeak"] = pivot[ONPEAK_HE_COLS].mean(axis=1)
    pivot["OffPeak"] = pivot[OFFPEAK_HE_COLS].mean(axis=1)
    pivot["Flat"] = pivot[HE_COLS].mean(axis=1)
    pivot = pivot.reset_index().rename(columns={"date": "Date"})
    return (
        pivot[ORDERED_GAS_COLS]
        .sort_values("Date", ascending=False)
        .reset_index(drop=True)
    )


def _render_gas_hub(window: pd.DataFrame, hub_col: str, hub_label: str) -> None:
    """Gray-historical + red-target overlay chart + hourly wide table.

    Mirrors ``_render_rto_series`` but for the gas single-source frame
    (no Source / As of Date columns since gas has no forecast vs RT split).
    """
    sub = window[["date", "hour_ending", hub_col]].dropna(subset=[hub_col])
    if sub.empty:
        st.warning(f"No gas data in window for {hub_label}.")
        return

    fig = go.Figure()
    historical_dates = sorted(d for d in sub["date"].unique() if d != target_date)
    for d in historical_dates:
        day = sub[sub["date"] == d].sort_values("hour_ending")
        fig.add_trace(go.Scatter(
            x=day["hour_ending"], y=day[hub_col],
            mode="lines",
            line=dict(color="#94a3b8", width=1),
            opacity=0.4,
            showlegend=False,
            hovertemplate=(
                f"<b>{d}</b><br>HE %{{x}}<br>"
                "$%{y:,.3f}/MMBtu<extra></extra>"
            ),
        ))
    target_day = sub[sub["date"] == target_date].sort_values("hour_ending")
    if not target_day.empty:
        fig.add_trace(go.Scatter(
            x=target_day["hour_ending"], y=target_day[hub_col],
            mode="lines+markers",
            name=f"Target date {target_date}",
            line=dict(color="#ef4444", width=2.5),
            marker=dict(size=5),
            hovertemplate=(
                f"<b>{target_date}</b><br>HE %{{x}}<br>"
                "$%{y:,.3f}/MMBtu<extra></extra>"
            ),
        ))
    fig.update_layout(
        template="plotly_dark",
        height=420,
        xaxis_title="Hour Ending",
        yaxis_title="$/MMBtu",
        legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
        margin=dict(l=60, r=20, t=30, b=40),
        hovermode="closest",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5])
    st.plotly_chart(fig, use_container_width=True)

    table = _pivot_gas_hub_wide(window, hub_col)
    fmt = {c: GAS_FMT for c in table.columns if c != "Date"}
    st.dataframe(
        table.style.format(fmt, na_rep="—"),
        use_container_width=True,
        hide_index=True,
        height=400,
    )


with st.expander("**ICE Next-Day Gas (4 hubs)**", expanded=False):
    st.caption(
        "Hourly ICE next-day gas via `loader.load_gas_prices_hourly` — "
        "single-source (no forecast vs RT split). Most days carry intra-day "
        "variation reflecting the gas trading-day rollover at HE14. "
        f"Target date **{target_date}** in red."
    )
    overlay_start = target_date - timedelta(days=int(lookback_days))
    gas_window = gas[
        (gas["date"] >= overlay_start)
        & (gas["date"] <= target_date)
        & (pd.to_datetime(gas["date"]).dt.year.isin(years_filter))
    ].copy()
    if gas_window.empty:
        st.warning("No gas data in window.")
    else:
        gas_tabs = st.tabs([label for _, label in GAS_HUBS])
        for tab, (hub_col, hub_label) in zip(gas_tabs, GAS_HUBS):
            with tab:
                _render_gas_hub(gas_window, hub_col, hub_label)


# ── Alt sources: Meteologica load / wind / solar ─────────────────────────
st.divider()

meteo_regions_present = set(meteologica["region"].dropna().unique().tolist())
meteo_regions_to_render = [r for r in LOAD_REGION_ORDER if r in meteo_regions_present]
with st.expander("**Alt source: Meteologica Load**", expanded=False):
    st.caption(
        "Vendor-published forecast — alt input for the model (visualization "
        "only for now). Same strict 24-HE rule as PJM via "
        "`loader.load_meteologica_load_coalesced`: Meteologica DA-cutoff "
        "vintage where it covers all 24 HEs, PJM RT actuals as fallback for "
        f"partial / missing days. Target date **{target_date}** in red."
    )
    if not meteo_regions_to_render:
        st.warning("No regions present in Meteologica load frame.")
    else:
        meteo_load_tabs = st.tabs(list(meteo_regions_to_render))
        for tab, region_code in zip(meteo_load_tabs, meteo_regions_to_render):
            with tab:
                _render_load_region(
                    region_code, meteologica, forecast_label="Meteologica",
                )


meteo_wind_regions_present = set(meteologica_wind["region"].dropna().unique().tolist())
meteo_wind_regions_to_render = [r for r in LOAD_REGION_ORDER if r in meteo_wind_regions_present]
with st.expander("**Alt source: Meteologica Wind**", expanded=False):
    st.caption(
        "Vendor-published wind forecast — alt input for the model "
        "(visualization only for now). Strict 24-HE rule via "
        "`loader.load_meteologica_wind_coalesced`: Meteologica DA-cutoff "
        "vintage where it covers all 24 HEs, PJM `net_load_actual.wind_gen_mw` "
        f"as fallback for partial / missing days. Target date **{target_date}** "
        "in red."
    )
    if not meteo_wind_regions_to_render:
        st.warning("No regions present in Meteologica wind frame.")
    else:
        meteo_wind_tabs = st.tabs(list(meteo_wind_regions_to_render))
        for tab, region_code in zip(meteo_wind_tabs, meteo_wind_regions_to_render):
            with tab:
                _render_load_region(
                    region_code,
                    meteologica_wind,
                    forecast_label="Meteologica",
                    value_col="wind_mw",
                    axis_label="Wind (MW)",
                    hover_label="Wind",
                )


meteo_solar_regions_present = set(meteologica_solar["region"].dropna().unique().tolist())
meteo_solar_regions_to_render = [r for r in LOAD_REGION_ORDER if r in meteo_solar_regions_present]
with st.expander("**Alt source: Meteologica Solar**", expanded=False):
    st.caption(
        "Vendor-published solar forecast — alt input for the model "
        "(visualization only for now). Strict 24-HE rule via "
        "`loader.load_meteologica_solar_coalesced`: Meteologica DA-cutoff "
        "vintage where it covers all 24 HEs, PJM `net_load_actual.solar_gen_mw` "
        "as fallback for partial / missing days. Pre-2019-04-02 dates have no "
        "actuals (PJM did not publish solar generation back then). Target date "
        f"**{target_date}** in red."
    )
    if not meteo_solar_regions_to_render:
        st.warning("No regions present in Meteologica solar frame.")
    else:
        meteo_solar_tabs = st.tabs(list(meteo_solar_regions_to_render))
        for tab, region_code in zip(meteo_solar_tabs, meteo_solar_regions_to_render):
            with tab:
                _render_load_region(
                    region_code,
                    meteologica_solar,
                    forecast_label="Meteologica",
                    value_col="solar_mw",
                    axis_label="Solar (MW)",
                    hover_label="Solar",
                )
