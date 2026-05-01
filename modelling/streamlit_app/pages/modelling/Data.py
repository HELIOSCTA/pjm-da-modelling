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
@st.cache_data(show_spinner="Loading load forecast parquet...")
def _load_forecast() -> pd.DataFrame:
    df = _shared.load_pjm_load_forecast(cache_dir=knn_configs.CACHE_DIR)
    return _coerce_and_dedupe(df, "forecast_load_mw")


@st.cache_data(show_spinner="Loading RT load parquet...")
def _load_load_rt() -> pd.DataFrame:
    df = loader.load_load_rt(cache_dir=knn_configs.CACHE_DIR)
    return _coerce_and_dedupe(df, "rt_load_mw")


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
    _load_forecast.clear()
    _load_load_rt.clear()
    _load_lmps_da.clear()
    _load_lmps_rt.clear()
    st.rerun()

forecast = _load_forecast()
load_rt = _load_load_rt()
lmps_da = _load_lmps_da()
lmps_rt = _load_lmps_rt()

st.sidebar.header("Inputs")
forecast_date, target_date = linked_date_pair(key_prefix="_data_dates")

default_lookback = target_date - timedelta(days=DEFAULT_LOOKBACK_DAYS)
lookback_date = st.sidebar.date_input(
    "Lookback date",
    value=st.session_state.get("_data_lookback", default_lookback),
    help=f"Start of the inspection window. Default: target − {DEFAULT_LOOKBACK_DAYS} days.",
    key="_data_lookback",
)
if lookback_date > target_date:
    st.sidebar.error("Lookback must be on or before target.")
    st.stop()

regions = sorted(forecast["region"].dropna().unique().tolist())
default_region = (
    knn_configs.LOAD_REGION if knn_configs.LOAD_REGION in regions else regions[0]
)
region = st.sidebar.selectbox(
    "Forecast region",
    regions,
    index=regions.index(default_region),
)
hubs = sorted(lmps_da["region"].dropna().unique().tolist())
default_hub = knn_configs.HUB if knn_configs.HUB in hubs else (hubs[0] if hubs else "")
hub = st.sidebar.selectbox(
    "DA LMP hub",
    hubs,
    index=hubs.index(default_hub) if default_hub in hubs else 0,
)

st.caption(
    f"Window: **{lookback_date}** → **{target_date}**  ·  "
    f"forecast date **{forecast_date}** highlighted  ·  "
    f"region **{region}**  ·  hub **{hub}**"
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


# ── DA LMPs: DA vs RT for [lookback, target] · hub ─────────────────────────
with st.expander(
    f"DA vs RT LMPs — {lookback_date} → {target_date} · {hub}", expanded=True
):
    da_window = lmps_da[
        (lmps_da["region"] == hub)
        & (lmps_da["date"] >= lookback_date)
        & (lmps_da["date"] <= target_date)
    ].copy()
    rt_window = lmps_rt[
        (lmps_rt["region"] == hub)
        & (lmps_rt["date"] >= lookback_date)
        & (lmps_rt["date"] <= target_date)
    ].copy()

    if len(da_window) == 0 and len(rt_window) == 0:
        st.warning(f"No DA or RT LMPs in window for {hub}.")
    else:
        da_window["dt"] = _to_dt(da_window)
        rt_window["dt"] = _to_dt(rt_window)
        da_window = da_window.sort_values("dt")
        rt_window = rt_window.sort_values("dt")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=da_window["dt"], y=da_window["lmp"],
            mode="lines+markers", name="DA (Forecast)",
            line=dict(color="#4cc9f0", width=1.8), marker=dict(size=4),
        ))
        fig.add_trace(go.Scatter(
            x=rt_window["dt"], y=rt_window["lmp"],
            mode="lines", name="RT (Actual)",
            line=dict(color="#f0b429", width=1.4),
        ))
        _highlight_forecast_date(fig, forecast_date)
        fig.update_layout(
            template="plotly_dark",
            height=380,
            xaxis_title="Datetime (HE)",
            yaxis_title="LMP ($/MWh)",
            legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
            margin=dict(l=60, r=20, t=30, b=40),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

        da_long = da_window[["date", "hour_ending", "region", "lmp"]].assign(
            Type="DA (Forecast)",
        )
        rt_long = rt_window[["date", "hour_ending", "region", "lmp"]].assign(
            Type="RT (Actual)",
        )
        long_table = (
            pd.concat([da_long, rt_long], ignore_index=True)
            .rename(columns={"lmp": "LMP ($/MWh)", "region": "Hub"})
            .sort_values(["date", "hour_ending", "Type"], ascending=[False, False, True])
            .reset_index(drop=True)
        )
        long_table = long_table[["date", "hour_ending", "Hub", "Type", "LMP ($/MWh)"]]

        st.dataframe(
            long_table.style.format({"LMP ($/MWh)": "{:,.2f}"}, na_rep="—"),
            use_container_width=True,
            hide_index=True,
        )


# ── Load: forecast vs RT actuals for [lookback, target] · region ──────────
st.divider()
st.header("Features")

with st.expander(
    f"Load Forecast vs RT — {lookback_date} → {target_date} · {region}", expanded=True
):
    fcst_window = forecast[
        (forecast["region"] == region)
        & (forecast["date"] >= lookback_date)
        & (forecast["date"] <= target_date)
    ].copy()
    load_rt_window = load_rt[
        (load_rt["region"] == region)
        & (load_rt["date"] >= lookback_date)
        & (load_rt["date"] <= target_date)
    ].copy()

    if len(fcst_window) == 0 and len(load_rt_window) == 0:
        st.warning(f"No load forecast or RT load in window for {region}.")
    else:
        fcst_window["dt"] = _to_dt(fcst_window)
        load_rt_window["dt"] = _to_dt(load_rt_window)
        fcst_window = fcst_window.sort_values("dt")
        load_rt_window = load_rt_window.sort_values("dt")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=fcst_window["dt"], y=fcst_window["forecast_load_mw"],
            mode="lines+markers", name="Forecast",
            line=dict(color="#4cc9f0", width=1.8), marker=dict(size=4),
        ))
        fig.add_trace(go.Scatter(
            x=load_rt_window["dt"], y=load_rt_window["rt_load_mw"],
            mode="lines", name="Actual (RT)",
            line=dict(color="#f0b429", width=1.4),
        ))
        _highlight_forecast_date(fig, forecast_date)
        fig.update_layout(
            template="plotly_dark",
            height=380,
            xaxis_title="Datetime (HE)",
            yaxis_title="Load (MW)",
            legend=dict(orientation="h", yanchor="top", y=-0.18, x=0),
            margin=dict(l=60, r=20, t=30, b=40),
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

        fcst_long = (
            fcst_window[["date", "hour_ending", "region", "forecast_load_mw"]]
            .rename(columns={"forecast_load_mw": "Load (MW)"})
            .assign(Type="Forecast")
        )
        actual_long = (
            load_rt_window[["date", "hour_ending", "region", "rt_load_mw"]]
            .rename(columns={"rt_load_mw": "Load (MW)"})
            .assign(Type="Actual")
        )
        long_table = (
            pd.concat([fcst_long, actual_long], ignore_index=True)
            .rename(columns={"region": "Region"})
            .sort_values(["date", "hour_ending", "Type"], ascending=[False, False, True])
            .reset_index(drop=True)
        )
        long_table = long_table[["date", "hour_ending", "Region", "Type", "Load (MW)"]]

        st.dataframe(
            long_table.style.format({"Load (MW)": "{:,.0f}"}, na_rep="—"),
            use_container_width=True,
            hide_index=True,
        )
