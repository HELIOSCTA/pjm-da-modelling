"""Inspect the candidate pool that an analog run would draw from.

Sits between Configs and Run in the operator workflow. Lets the user verify
whether their (target_date, season_window_days, min_pool_size) combination
yields a sensible eligible-day pool BEFORE clicking Run.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

_APP_ROOT = Path(__file__).resolve().parents[1]
_MODELLING_ROOT = _APP_ROOT.parent
for path in (_APP_ROOT, _MODELLING_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from da_models.like_day_model_knn import _shared  # noqa: E402
from da_models.like_day_model_knn import configs as knn_configs  # noqa: E402
from lib import config_io  # noqa: E402
from lib.ui import (  # noqa: E402
    ALL_HOURS,
    linked_date_pair,
    shade_onpeak,
    styled_summary,
    wide_summary_row,
)


st.title("Candidate Pool Inspection")
st.caption(
    "What historical days would the model draw from for this target date and "
    "config? Before clicking Run, check pool size, recency, and load-shape "
    "diversity to see if the season window is right."
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


# TODO: dedupe with da_models.like_day_model_knn._shared once that family-shared
# helper exists. Identical logic to analog_store._candidate_pool and the three
# engines' _candidate_pool helpers.
def _filter_pool(
    df: pd.DataFrame,
    target_date: date,
    season_window_days: int,
    min_pool_size: int,
) -> tuple[pd.DataFrame, bool]:
    """Apply (date < target_date) AND (DOY-distance ≤ window), with fallback to
    the full pre-target history if filtered pool < min_pool_size.

    Returns (filtered_df, fallback_triggered).
    """
    work = df[pd.to_datetime(df["date"]).dt.date < target_date].copy()
    if len(work) == 0 or season_window_days <= 0:
        return work, False

    target_doy = pd.Timestamp(target_date).dayofyear
    doys = pd.to_datetime(work["date"]).dt.dayofyear.to_numpy(dtype=float)
    direct = np.abs(doys - float(target_doy))
    keep = np.minimum(direct, 366.0 - direct) <= float(season_window_days)
    candidates = work[keep]
    if len(candidates) >= min_pool_size:
        return candidates.copy(), False
    return work, True


# ── Sidebar ───────────────────────────────────────────────────
if st.sidebar.button("Refresh data"):
    _load_forecast.clear()
    _load_lmps.clear()
    st.rerun()

forecast = _load_forecast()
lmps = _load_lmps()

st.sidebar.header("Inputs")
forecast_date, target_date = linked_date_pair(key_prefix="_candidates_dates")

regions = sorted(forecast["region"].dropna().unique().tolist())
default_region = (
    knn_configs.LOAD_REGION if knn_configs.LOAD_REGION in regions else regions[0]
)
region = st.sidebar.selectbox(
    "Region",
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

st.sidebar.subheader("Config")
DEFAULTS_LABEL = "(model defaults)"
saved = config_io.list_configs()
saved_names = [c["name"] for c in saved]
choice = st.sidebar.selectbox(
    "Use config",
    [DEFAULTS_LABEL] + saved_names,
)
selected_config = (
    None if choice == DEFAULTS_LABEL
    else next((c for c in saved if c["name"] == choice), None)
)

config_window = (
    int(selected_config["season_window_days"]) if selected_config
    else int(config_io.DEFAULT_PAYLOAD["season_window_days"])
)
config_min_pool = (
    int(selected_config["min_pool_size"]) if selected_config
    else int(config_io.DEFAULT_PAYLOAD["min_pool_size"])
)

st.sidebar.subheader("Preview overrides")
st.sidebar.caption("Slider changes are preview-only — not saved to the config.")
season_window_days = st.sidebar.slider(
    "season_window_days",
    min_value=7,
    max_value=180,
    value=config_window,
)
min_pool_size = st.sidebar.slider(
    "min_pool_size",
    min_value=10,
    max_value=500,
    value=config_min_pool,
)

# ── Filter the pool ───────────────────────────────────────────
forecast_region = forecast[forecast["region"] == region]
all_dates_df = (
    forecast_region[["date"]]
    .drop_duplicates()
    .sort_values("date")
    .reset_index(drop=True)
)

filtered_df, fallback = _filter_pool(
    all_dates_df,
    target_date=forecast_date,
    season_window_days=season_window_days,
    min_pool_size=min_pool_size,
)
candidate_dates = filtered_df["date"].tolist()

# ── Pool Summary ──────────────────────────────────────────────
st.subheader("Pool Summary")

if len(candidate_dates) == 0:
    st.error("No eligible candidate dates. The forecast parquet may be empty.")
    st.stop()

if fallback:
    st.error(
        f"Fallback to full history triggered — "
        f"{len(candidate_dates)} candidates after filter ≥ "
        f"min_pool_size={min_pool_size} fails. The model would use the entire "
        f"pre-forecast history. Consider widening `season_window_days`."
    )
else:
    st.success(
        f"{len(candidate_dates)} candidate dates pass the season window."
    )

cols = st.columns(4)
cols[0].metric("Pool size", f"{len(candidate_dates):,}")
cols[1].metric("Min date", str(min(candidate_dates)))
cols[2].metric("Max date", str(max(candidate_dates)))
cols[3].metric("Fallback", "yes" if fallback else "no")

# ── Pivots used by the chart sections below ───────────────────
candidate_set = set(candidate_dates)

pool_forecast = forecast_region[forecast_region["date"].isin(candidate_set)]
load_pivot = (
    pool_forecast
    .dropna(subset=["hour_ending"])
    .assign(hour_ending=lambda d: d["hour_ending"].astype(int))
    .pivot_table(
        index="date",
        columns="hour_ending",
        values="forecast_load_mw",
        aggfunc="mean",
    )
    .reindex(columns=ALL_HOURS)
)

lmps_hub = lmps[lmps["region"] == hub]
pool_lmps = lmps_hub[lmps_hub["date"].isin(candidate_set)]
lmp_pivot = (
    pool_lmps
    .dropna(subset=["hour_ending"])
    .assign(hour_ending=lambda d: d["hour_ending"].astype(int))
    .pivot_table(
        index="date",
        columns="hour_ending",
        values="lmp",
        aggfunc="mean",
    )
    .reindex(columns=ALL_HOURS)
)


def _spaghetti_xy(pivot: pd.DataFrame) -> tuple[list, list]:
    xs: list[float | None] = []
    ys: list[float | None] = []
    for _, row in pivot.iterrows():
        for h in ALL_HOURS:
            xs.append(h)
            ys.append(row.get(h, None))
        xs.append(None)
        ys.append(None)
    return xs, ys


def _add_pool_distribution_traces(fig: go.Figure, pivot: pd.DataFrame) -> None:
    """Overlay P25-P75 band, median, and mean on a pool spaghetti chart."""
    p25 = pivot.quantile(0.25, axis=0).reindex(ALL_HOURS)
    p75 = pivot.quantile(0.75, axis=0).reindex(ALL_HOURS)
    median = pivot.median(axis=0).reindex(ALL_HOURS)
    mean = pivot.mean(axis=0).reindex(ALL_HOURS)

    # Invisible upper bound + filled lower bound = shaded band.
    fig.add_trace(go.Scatter(
        x=ALL_HOURS, y=list(p75.values),
        mode="lines", line=dict(width=0),
        showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=ALL_HOURS, y=list(p25.values),
        mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(30, 144, 255, 0.18)",
        name="P25-P75", hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=ALL_HOURS, y=list(median.values),
        mode="lines+markers",
        line=dict(color="dodgerblue", width=3),
        name="Pool median",
    ))
    fig.add_trace(go.Scatter(
        x=ALL_HOURS, y=list(mean.values),
        mode="lines",
        line=dict(color="mediumseagreen", width=2, dash="dash"),
        name="Pool mean",
    ))


# ── Hourly DA LMP across Candidates ──────────────────────────
with st.expander(f"Hourly DA LMP across Candidates · {hub}", expanded=True):
    if len(lmp_pivot) == 0:
        st.warning(f"No DA LMPs for the candidate pool · {hub}.")
    else:
        spag_x, spag_y = _spaghetti_xy(lmp_pivot)

        target_lmp_rows = lmps_hub[lmps_hub["date"] == target_date].copy()
        target_lmp_y: list[float | None] = []
        if len(target_lmp_rows):
            target_lmp_rows = target_lmp_rows.dropna(subset=["hour_ending"]).copy()
            target_lmp_rows["hour_ending"] = target_lmp_rows["hour_ending"].astype(int)
            target_lmp_y = list(
                target_lmp_rows.set_index("hour_ending")["lmp"].reindex(ALL_HOURS).values
            )

        lmp_fig = go.Figure()
        lmp_fig.add_trace(go.Scatter(
            x=spag_x, y=spag_y, mode="lines",
            line=dict(color="gray", width=1), opacity=0.3,
            name=f"Candidates (n={len(lmp_pivot)})", hoverinfo="skip",
        ))
        _add_pool_distribution_traces(lmp_fig, lmp_pivot)
        if target_lmp_y and any(v is not None and not pd.isna(v) for v in target_lmp_y):
            lmp_fig.add_trace(go.Scatter(
                x=ALL_HOURS, y=target_lmp_y, mode="lines+markers",
                name=f"Target date ({target_date})",
                line=dict(color="crimson", width=3),
            ))
        else:
            st.caption(
                f"No DA LMPs for the target date {target_date} · {hub} — "
                "overlay omitted."
            )

        lmp_fig.update_layout(
            title="Hourly DA LMP — Candidate Pool",
            xaxis_title="Hour ending", yaxis_title="DA LMP ($/MWh)",
        )
        shade_onpeak(lmp_fig)
        st.plotly_chart(lmp_fig, use_container_width=True)

# ── Hourly Load Shape ─────────────────────────────────────────
with st.expander("Hourly Load Shape across Candidates", expanded=True):
    if len(load_pivot) == 0:
        st.warning("No forecast rows for the selected candidate pool.")
    else:
        spag_x, spag_y = _spaghetti_xy(load_pivot)

        target_rows = forecast_region[forecast_region["date"] == forecast_date].copy()
        target_y: list[float | None] = []
        if len(target_rows):
            target_rows = target_rows.dropna(subset=["hour_ending"]).copy()
            target_rows["hour_ending"] = target_rows["hour_ending"].astype(int)
            target_y = list(
                target_rows.set_index("hour_ending")["forecast_load_mw"]
                .reindex(ALL_HOURS).values
            )

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=spag_x, y=spag_y, mode="lines",
            line=dict(color="gray", width=1), opacity=0.3,
            name=f"Candidates (n={len(load_pivot)})", hoverinfo="skip",
        ))
        _add_pool_distribution_traces(fig, load_pivot)
        if target_y and any(v is not None and not pd.isna(v) for v in target_y):
            fig.add_trace(go.Scatter(
                x=ALL_HOURS, y=target_y, mode="lines+markers",
                name=f"Forecast date ({forecast_date})",
                line=dict(color="crimson", width=3),
            ))
        else:
            st.caption(
                f"No load forecast available for the forecast date "
                f"{forecast_date} · {region} — overlay omitted."
            )

        fig.update_layout(
            title="Hourly Load Forecast — Candidate Pool",
            xaxis_title="Hour ending", yaxis_title="Forecast load (MW)",
        )
        shade_onpeak(fig)
        st.plotly_chart(fig, use_container_width=True)

        # ── Predictive power: load forecast → DA LMP ──────────
        st.divider()
        st.markdown("**Feature → Target Predictive Power**")

        common_dates = load_pivot.index.intersection(lmp_pivot.index)
        if len(common_dates) < 3:
            st.info(
                "Need at least 3 candidates with both load forecast and DA LMP "
                f"data for correlation. Current: {len(common_dates)}."
            )
        else:
            load_for_corr = load_pivot.loc[common_dates]
            lmp_for_corr = lmp_pivot.loc[common_dates]

            method = st.radio(
                "Correlation",
                ["spearman", "pearson"],
                horizontal=True,
                key="_load_lmp_corr_method",
            )

            corrs: list[float | None] = []
            for h in ALL_HOURS:
                xs = load_for_corr[h]
                ys = lmp_for_corr[h]
                valid = xs.notna() & ys.notna()
                if (
                    valid.sum() >= 3
                    and xs[valid].nunique() > 1
                    and ys[valid].nunique() > 1
                ):
                    corrs.append(float(xs[valid].corr(ys[valid], method=method)))
                else:
                    corrs.append(None)

            corr_fig = px.bar(
                x=ALL_HOURS,
                y=corrs,
                labels={"x": "Hour ending", "y": f"{method.title()} correlation"},
                title=(
                    f"Same-Hour Correlation: Load Forecast vs DA LMP "
                    f"(n={len(common_dates)} candidates)"
                ),
            )
            corr_fig.update_yaxes(range=[-1, 1])
            shade_onpeak(corr_fig)
            st.plotly_chart(corr_fig, use_container_width=True)

            # Scatter for selected HE.
            he_choice = st.selectbox(
                "HE for scatter",
                ALL_HOURS,
                index=16,  # default HE17 (peak)
                key="_load_lmp_scatter_he",
            )
            xs = load_for_corr[he_choice]
            ys = lmp_for_corr[he_choice]
            valid = xs.notna() & ys.notna()
            x_clean = xs[valid].astype(float)
            y_clean = ys[valid].astype(float)

            scatter_fig = go.Figure()
            scatter_fig.add_trace(go.Scatter(
                x=x_clean.values,
                y=y_clean.values,
                mode="markers",
                marker=dict(color="dodgerblue", size=6, opacity=0.6),
                text=[str(d) for d in x_clean.index],
                hovertemplate=(
                    "%{text}<br>load=%{x:,.0f} MW<br>"
                    "lmp=%{y:.2f} $/MWh<extra></extra>"
                ),
                name="Candidates",
            ))

            if len(x_clean) >= 2 and x_clean.nunique() >= 2 and y_clean.nunique() >= 2:
                slope, intercept = np.polyfit(x_clean.values, y_clean.values, 1)
                r = float(np.corrcoef(x_clean.values, y_clean.values)[0, 1])
                fit_x = np.array([float(x_clean.min()), float(x_clean.max())])
                fit_y = slope * fit_x + intercept
                scatter_fig.add_trace(go.Scatter(
                    x=fit_x, y=fit_y, mode="lines",
                    line=dict(color="orange", width=2),
                    name=f"OLS fit (R²={r ** 2:.3f})",
                ))

            # Target marker: load(forecast_date) at HE × LMP(target_date) at HE.
            target_load_he = forecast_region.loc[
                (forecast_region["date"] == forecast_date)
                & (forecast_region["hour_ending"].astype("Int64") == he_choice),
                "forecast_load_mw",
            ]
            target_lmp_he = lmps_hub.loc[
                (lmps_hub["date"] == target_date)
                & (lmps_hub["hour_ending"].astype("Int64") == he_choice),
                "lmp",
            ]
            if len(target_load_he) and len(target_lmp_he):
                tl = target_load_he.iloc[0]
                tp = target_lmp_he.iloc[0]
                if pd.notna(tl) and pd.notna(tp):
                    scatter_fig.add_trace(go.Scatter(
                        x=[float(tl)],
                        y=[float(tp)],
                        mode="markers",
                        marker=dict(
                            symbol="star", color="gold", size=18,
                            line=dict(color="crimson", width=2),
                        ),
                        name=f"Target ({forecast_date} → {target_date})",
                    ))

            scatter_fig.update_layout(
                title=f"HE{he_choice}: Load Forecast → DA LMP",
                xaxis_title=f"Load forecast at HE{he_choice} (MW)",
                yaxis_title=f"DA LMP at HE{he_choice} ($/MWh)",
            )
            st.plotly_chart(scatter_fig, use_container_width=True)
