"""Inspect inputs (load forecast, DA LMPs) for a target date."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[1]
_MODELLING_ROOT = _APP_ROOT.parent
for path in (_APP_ROOT, _MODELLING_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from da_models.knn_model_only_load import _shared  # noqa: E402
from da_models.knn_model_only_load import configs as knn_configs  # noqa: E402
from lib.ui import linked_date_pair, shade_onpeak, styled_summary, wide_summary_row  # noqa: E402

st.title("Inspect Inputs")
st.caption(
    "What does the model see for a target date? Load-forecast inputs and "
    "(if the date is past) realized DA LMPs."
)


@st.cache_data(show_spinner="Loading load forecast parquet...")
def _load_forecast() -> pd.DataFrame:
    df = _shared.load_pjm_load_forecast(cache_dir=knn_configs.CACHE_DIR)
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["forecast_load_mw"] = pd.to_numeric(df["forecast_load_mw"], errors="coerce")
    df["region"] = df["region"].astype(str)
    return df


@st.cache_data(show_spinner="Loading DA LMPs parquet...")
def _load_lmps() -> pd.DataFrame:
    df = _shared.load_lmp_da(cache_dir=knn_configs.CACHE_DIR)
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["hour_ending"] = pd.to_numeric(df["hour_ending"], errors="coerce").astype("Int64")
    df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")
    df["region"] = df["region"].astype(str)
    return df


DATES_PARQUET_NAME = "pjm_dates_daily.parquet"


def _dates_path() -> Path:
    return Path(knn_configs.CACHE_DIR) / DATES_PARQUET_NAME


@st.cache_data(show_spinner="Loading PJM dates parquet...")
def _load_dates() -> pd.DataFrame:
    path = _dates_path()
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path).copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


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


if st.sidebar.button("Refresh data"):
    _load_forecast.clear()
    _load_lmps.clear()
    _load_dates.clear()
    st.rerun()

forecast = _load_forecast()
lmps = _load_lmps()
dates = _load_dates()

st.sidebar.header("Inputs")
forecast_date, target_date = linked_date_pair(key_prefix="_data_dates")

regions = sorted(forecast["region"].dropna().unique().tolist())
default_region = (
    knn_configs.LOAD_REGION if knn_configs.LOAD_REGION in regions else regions[0]
)
region = st.sidebar.selectbox(
    "Forecast region",
    regions,
    index=regions.index(default_region),
)
hubs = sorted(lmps["region"].dropna().unique().tolist())
default_hub = knn_configs.HUB if knn_configs.HUB in hubs else (hubs[0] if hubs else "")
hub = st.sidebar.selectbox(
    "DA LMP hub",
    hubs,
    index=hubs.index(default_hub) if default_hub in hubs else 0,
)

# ── Source parquets (collapsible) ─────────────────────────────
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

# ── DA LMPs for target date (the target the model predicts) ───
with st.expander(f"DA LMPs — {target_date} · {hub}", expanded=True):
    lmp_target = lmps[(lmps["date"] == target_date) & (lmps["region"] == hub)].copy()

    if len(lmp_target) == 0:
        if target_date >= date.today():
            st.info(
                f"No DA LMPs published for {target_date} yet — "
                "DA results land the day before delivery."
            )
        else:
            st.warning(f"No DA LMPs found for {target_date} · {hub}.")
    else:
        lmp_target = lmp_target.sort_values("hour_ending").reset_index(drop=True)

        fig = px.bar(
            lmp_target,
            x="hour_ending",
            y="lmp",
            labels={"hour_ending": "Hour ending", "lmp": "DA LMP ($/MWh)"},
            title="Hourly DA LMP",
        )
        shade_onpeak(fig)
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            styled_summary(
                wide_summary_row(
                    lmp_target,
                    source="DA LMP",
                    region=hub,
                    target_date=target_date,
                    value_col="lmp",
                ),
                decimals=2,
            ),
            use_container_width=True,
            hide_index=True,
        )

# ── Features ──────────────────────────────────────────────────
st.divider()
st.header("Features")

with st.expander(f"Load Forecast — {forecast_date} · {region}", expanded=True):
    fcst_target = forecast[
        (forecast["date"] == forecast_date) & (forecast["region"] == region)
    ].copy()

    if len(fcst_target) == 0:
        st.warning(f"No load forecast rows for {forecast_date} · {region}.")
    else:
        fcst_target = fcst_target.sort_values("hour_ending").reset_index(drop=True)

        fig = px.line(
            fcst_target,
            x="hour_ending",
            y="forecast_load_mw",
            markers=True,
            labels={
                "hour_ending": "Hour ending",
                "forecast_load_mw": "Forecast load (MW)",
            },
            title="Hourly Load Forecast",
        )
        shade_onpeak(fig)
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            styled_summary(
                wide_summary_row(
                    fcst_target,
                    source="Load Forecast",
                    region=region,
                    target_date=forecast_date,
                    value_col="forecast_load_mw",
                ),
                decimals=0,
            ),
            use_container_width=True,
            hide_index=True,
        )
