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
from da_models.like_day_model_knn import calendar as knn_calendar  # noqa: E402
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


# Thin Streamlit cache wrappers around _shared.* — the underlying loaders
# already return canonical types (date=datetime.date, hour_ending=int,
# region=str, value=float64) via da_models.common.data.loader normalization.
# No need to re-coerce here.
@st.cache_data(show_spinner="Loading load forecast parquet...")
def _load_forecast() -> pd.DataFrame:
    return _shared.load_pjm_load_forecast(cache_dir=knn_configs.CACHE_DIR)


@st.cache_data(show_spinner="Loading DA LMPs parquet...")
def _load_lmps() -> pd.DataFrame:
    return _shared.load_lmp_da(cache_dir=knn_configs.CACHE_DIR)


@st.cache_data(show_spinner="Loading pjm_dates_daily parquet...")
def _load_dates_daily() -> pd.DataFrame:
    return _shared.load_dates_daily(cache_dir=knn_configs.CACHE_DIR)


def _season_window_step(
    df: pd.DataFrame,
    target_date: date,
    season_window_days: int,
    min_pool_size: int,
) -> tuple[pd.DataFrame, bool]:
    """Mirror of the engines' DOY-distance leg. Falls back to full history
    when the filtered pool drops below ``min_pool_size``."""
    if len(df) == 0 or season_window_days <= 0:
        return df, False
    target_doy = pd.Timestamp(target_date).dayofyear
    doys = pd.to_datetime(df["date"]).dt.dayofyear.to_numpy(dtype=float)
    direct = np.abs(doys - float(target_doy))
    keep = np.minimum(direct, 366.0 - direct) <= float(season_window_days)
    candidates = df[keep]
    if len(candidates) >= min_pool_size:
        return candidates.copy(), False
    return df, True


def _drop_meta_cols(df: pd.DataFrame) -> pd.DataFrame:
    drop = [c for c in (
        "day_of_week_number", "is_weekend", "is_nerc_holiday",
        "is_federal_holiday", "summer_winter",
    ) if c in df.columns]
    return df.drop(columns=drop, errors="ignore") if drop else df


def _stage_filter(
    base: pd.DataFrame,
    target_date: date,
    dates_meta: pd.DataFrame,
    *,
    same_dow_group: bool,
    exclude_holidays: bool,
    exclude_dates: list[str],
    max_age_years: int | None,
    season_window_days: int,
    min_pool_size: int,
) -> tuple[pd.DataFrame, list[dict], bool]:
    """Run the candidate-pool filters one stage at a time so the UI can show
    where each candidate was dropped. Final stage matches what the engines'
    ``_candidate_pool`` helper produces.
    """
    stages: list[dict] = []
    pre = base[pd.to_datetime(base["date"]).dt.date < target_date].copy()
    stages.append({"stage": "Before target date", "n": len(pre)})

    after_excl = pre
    if exclude_dates:
        excl_set = {pd.to_datetime(s).date() for s in exclude_dates}
        after_excl = pre[~pre["date"].isin(excl_set)].copy()
    stages.append({"stage": f"exclude_dates ({len(exclude_dates)})", "n": len(after_excl)})

    after_age = knn_calendar.apply_calendar_filter(
        pool=after_excl, target_date=target_date, dates_meta=dates_meta,
        same_dow_group=False, exclude_holidays=False, exclude_dates=[],
        max_age_years=max_age_years, min_pool_size=min_pool_size,
    )
    stages.append({
        "stage": f"max_age_years ({max_age_years})" if max_age_years else "max_age_years (off)",
        "n": len(after_age),
    })

    after_hol = knn_calendar.apply_calendar_filter(
        pool=_drop_meta_cols(after_age),
        target_date=target_date, dates_meta=dates_meta,
        same_dow_group=False, exclude_holidays=exclude_holidays,
        exclude_dates=[], min_pool_size=min_pool_size,
    )
    stages.append({"stage": "exclude_holidays", "n": len(after_hol)})

    after_dow = knn_calendar.apply_calendar_filter(
        pool=_drop_meta_cols(after_hol),
        target_date=target_date, dates_meta=dates_meta,
        same_dow_group=same_dow_group, exclude_holidays=False,
        exclude_dates=[], min_pool_size=min_pool_size,
    )
    stages.append({"stage": "same_dow_group", "n": len(after_dow)})

    final, fallback = _season_window_step(
        after_dow, target_date, season_window_days, min_pool_size,
    )
    stages.append({"stage": "season_window_days", "n": len(final)})
    return final, stages, fallback


# ── Sidebar ───────────────────────────────────────────────────
if st.sidebar.button("Refresh data"):
    _load_forecast.clear()
    _load_lmps.clear()
    _load_dates_daily.clear()
    st.rerun()

forecast = _load_forecast()
lmps = _load_lmps()
dates_meta = _load_dates_daily()

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

def _cfg(key: str):
    if selected_config and key in selected_config:
        return selected_config[key]
    return config_io.DEFAULT_PAYLOAD[key]

config_window = int(_cfg("season_window_days"))
config_min_pool = int(_cfg("min_pool_size"))
config_n_analogs = int(_cfg("n_analogs"))
config_same_dow = bool(_cfg("same_dow_group"))
config_excl_holidays = bool(_cfg("exclude_holidays"))
config_excl_dates = list(_cfg("exclude_dates") or [])
config_use_profiles = bool(_cfg("use_day_type_profiles"))
config_max_age = _cfg("max_age_years")
config_max_age = int(config_max_age) if config_max_age else 0
config_half_life = _cfg("recency_half_life_years")
config_half_life = float(config_half_life) if config_half_life else 0.0

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
same_dow_group = st.sidebar.checkbox(
    "same_dow_group",
    value=config_same_dow,
    help="Restrict candidates to the same DOW bucket (weekday/saturday/sunday).",
)
exclude_holidays = st.sidebar.checkbox(
    "exclude_holidays",
    value=config_excl_holidays,
    help="Drop NERC holidays from the pool unless the target itself is a holiday.",
)
exclude_dates_raw = st.sidebar.text_area(
    "exclude_dates",
    value="\n".join(config_excl_dates),
    help="One YYYY-MM-DD per line. Always dropped from the candidate pool.",
    height=90,
)
exclude_dates = [s.strip() for s in exclude_dates_raw.splitlines() if s.strip()]
use_day_type_profiles = st.sidebar.checkbox(
    "use_day_type_profiles",
    value=config_use_profiles,
    help="Apply the Saturday/Sunday narrowing profile from KnnModelConfig.",
)

st.sidebar.markdown("**Recency**")
max_age_years_raw = st.sidebar.slider(
    "max_age_years (0 = off)",
    min_value=0, max_value=10, value=config_max_age,
    help=(
        "Hard cap: drop candidates older than target_date - N years. "
        "0 disables. Below 3 years, weekend pools may starve."
    ),
)
max_age_years: int | None = int(max_age_years_raw) if max_age_years_raw > 0 else None
recency_half_life_raw = st.sidebar.slider(
    "recency_half_life_years (0 = off)",
    min_value=0.0, max_value=8.0, value=float(config_half_life), step=0.5,
    help=(
        "Soft exponential decay on analog weight after selection. "
        "weight *= 0.5 ** (age_years / half_life). 0 disables."
    ),
)
recency_half_life_years: float | None = (
    float(recency_half_life_raw) if recency_half_life_raw > 0 else None
)

# ── Day-type profile preview ──────────────────────────────────
# Build the preview config by overlaying the sidebar's preview overrides on
# top of the saved config (or the dataclass defaults when none is selected).
import dataclasses as _dc  # noqa: E402  (local import to keep top of file clean)

_base_cfg = config_io.payload_to_config(
    selected_config,
    forecast_date=str(forecast_date),
)
preview_cfg = _dc.replace(
    _base_cfg,
    n_analogs=config_n_analogs,
    season_window_days=season_window_days,
    min_pool_size=min_pool_size,
    same_dow_group=same_dow_group,
    exclude_holidays=exclude_holidays,
    exclude_dates=list(exclude_dates),
    use_day_type_profiles=use_day_type_profiles,
    max_age_years=max_age_years,
    recency_half_life_years=recency_half_life_years,
)
resolved_cfg, day_type = preview_cfg.with_day_type_overrides(forecast_date)
# Honor the resolved profile so the candidate filter matches what Run will do.
season_window_days = int(resolved_cfg.season_window_days)
min_pool_size = int(resolved_cfg.min_pool_size)
same_dow_group = bool(resolved_cfg.same_dow_group)

with st.sidebar.expander(f"Day-type profile · {day_type}", expanded=False):
    st.caption(
        "What `KnnModelConfig.with_day_type_overrides()` would resolve for this "
        "target date — these values, not the sliders above, are what Run actually uses."
    )
    st.write({
        "day_type": day_type,
        "season_window_days": resolved_cfg.season_window_days,
        "min_pool_size": resolved_cfg.min_pool_size,
        "n_analogs": resolved_cfg.n_analogs,
        "same_dow_group": resolved_cfg.same_dow_group,
        "exclude_holidays": resolved_cfg.exclude_holidays,
        "max_age_years": resolved_cfg.max_age_years,
        "recency_half_life_years": resolved_cfg.recency_half_life_years,
    })

# ── Filter the pool ───────────────────────────────────────────
forecast_region = forecast[forecast["region"] == region]
all_dates_df = (
    forecast_region[["date"]]
    .drop_duplicates()
    .sort_values("date")
    .reset_index(drop=True)
)

filtered_df, stages, fallback = _stage_filter(
    all_dates_df,
    target_date=forecast_date,
    dates_meta=dates_meta,
    same_dow_group=same_dow_group,
    exclude_holidays=exclude_holidays,
    exclude_dates=exclude_dates,
    max_age_years=max_age_years,
    season_window_days=season_window_days,
    min_pool_size=min_pool_size,
)
candidate_dates = filtered_df["date"].tolist()

# ── Pool Summary ──────────────────────────────────────────────
st.subheader("Pool Summary")

if len(candidate_dates) == 0:
    st.error("No eligible candidate dates. The forecast parquet may be empty.")
    st.stop()

target_meta = knn_calendar.resolve_target_day_metadata(forecast_date, dates_meta)
target_holiday = bool(int(target_meta.get("is_nerc_holiday", 0) or 0))

if fallback:
    st.error(
        f"Fallback to full history triggered — "
        f"{len(candidate_dates)} candidates after filter ≥ "
        f"min_pool_size={min_pool_size} fails. The model would use the entire "
        f"pre-forecast history. Consider widening `season_window_days`."
    )
else:
    st.success(
        f"{len(candidate_dates)} candidate dates pass all filters."
    )

cols = st.columns(5)
cols[0].metric("Pool size", f"{len(candidate_dates):,}")
cols[1].metric("Min date", str(min(candidate_dates)))
cols[2].metric("Max date", str(max(candidate_dates)))
cols[3].metric("Day type", day_type)
cols[4].metric("Fallback", "yes" if fallback else "no")

# Per-stage drop attribution
stage_rows = []
prev_n = stages[0]["n"]
for i, s in enumerate(stages):
    delta = s["n"] - prev_n if i > 0 else 0
    stage_rows.append({
        "Stage": s["stage"],
        "Pool size": s["n"],
        "Δ from prev": delta,
    })
    prev_n = s["n"]
stage_df = pd.DataFrame(stage_rows)
with st.expander("Filter stages — where candidates were dropped", expanded=True):
    st.dataframe(stage_df, hide_index=True, use_container_width=True)
    if exclude_holidays and target_holiday:
        st.caption(
            f"Target {forecast_date} is itself a NERC holiday "
            f"({target_meta.get('holiday_name') or 'unnamed'}) — holiday "
            "exclusion is intentionally a no-op so a non-empty pool remains."
        )

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


def _weighted_corr(x: np.ndarray, y: np.ndarray, w: np.ndarray, method: str) -> float:
    """Weighted Pearson on raw values or on ranks (= weighted Spearman)."""
    if method == "spearman":
        x = pd.Series(x).rank(method="average").to_numpy()
        y = pd.Series(y).rank(method="average").to_numpy()
    w = w / w.sum() if w.sum() > 0 else np.full_like(w, 1.0 / len(w))
    mu_x = float((w * x).sum())
    mu_y = float((w * y).sum())
    dx, dy = x - mu_x, y - mu_y
    cov = float((w * dx * dy).sum())
    var_x = float((w * dx * dx).sum())
    var_y = float((w * dy * dy).sum())
    if var_x <= 0 or var_y <= 0:
        return float("nan")
    return cov / float(np.sqrt(var_x * var_y))


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

            ctrl_cols = st.columns([2, 2, 2])
            method = ctrl_cols[0].radio(
                "Correlation",
                ["spearman", "pearson"],
                horizontal=True,
                key="_load_lmp_corr_method",
            )
            weight_recency = ctrl_cols[1].checkbox(
                "Weight by recency",
                value=recency_half_life_years is not None,
                disabled=recency_half_life_years is None,
                help=(
                    "Use the recency_half_life_years half-life to weight the "
                    "correlation. Disabled when the half-life is off (0)."
                ),
                key="_load_lmp_corr_weight",
            )

            # Per-candidate recency weight (1.0 when half-life is off)
            recency_w_per_date = pd.Series(
                knn_calendar.age_decay_weights(
                    pd.to_datetime(common_dates), forecast_date,
                    recency_half_life_years,
                ),
                index=common_dates,
            )

            corrs: list[float | None] = []
            for h in ALL_HOURS:
                xs = load_for_corr[h]
                ys = lmp_for_corr[h]
                valid = xs.notna() & ys.notna()
                if not (
                    valid.sum() >= 3
                    and xs[valid].nunique() > 1
                    and ys[valid].nunique() > 1
                ):
                    corrs.append(None)
                    continue
                xv = xs[valid].astype(float).to_numpy()
                yv = ys[valid].astype(float).to_numpy()
                if weight_recency and recency_half_life_years:
                    wv = recency_w_per_date.loc[xs[valid].index].to_numpy()
                    corrs.append(float(_weighted_corr(xv, yv, wv, method)))
                else:
                    corrs.append(float(xs[valid].corr(ys[valid], method=method)))

            label_suffix = (
                f" (recency-weighted, half-life={recency_half_life_years}y)"
                if weight_recency and recency_half_life_years else ""
            )
            corr_fig = px.bar(
                x=ALL_HOURS,
                y=corrs,
                labels={"x": "Hour ending", "y": f"{method.title()} correlation"},
                title=(
                    f"Same-Hour Correlation: Load Forecast vs DA LMP "
                    f"(n={len(common_dates)} candidates){label_suffix}"
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
            ages = (
                pd.Timestamp(forecast_date) - pd.to_datetime(x_clean.index)
            ).days / 365.25
            scatter_w = recency_w_per_date.loc[x_clean.index].to_numpy()

            scatter_fig = go.Figure()
            color_use_recency = bool(weight_recency and recency_half_life_years)
            scatter_fig.add_trace(go.Scatter(
                x=x_clean.values,
                y=y_clean.values,
                mode="markers",
                marker=dict(
                    color=(scatter_w if color_use_recency else ages.values),
                    colorscale="Viridis" if color_use_recency else "Viridis_r",
                    cmin=(0.0 if color_use_recency else float(ages.min())),
                    cmax=(1.0 if color_use_recency else float(ages.max())),
                    showscale=True,
                    colorbar=dict(
                        title=("Recency<br>weight" if color_use_recency else "Age (yrs)"),
                    ),
                    size=6, opacity=0.7,
                ),
                text=[
                    f"{d} (age={a:.1f}y, w={w:.2f})"
                    for d, a, w in zip(x_clean.index, ages, scatter_w)
                ],
                hovertemplate=(
                    "%{text}<br>load=%{x:,.0f} MW<br>"
                    "lmp=%{y:.2f} $/MWh<extra></extra>"
                ),
                name="Candidates",
            ))

            if len(x_clean) >= 2 and x_clean.nunique() >= 2 and y_clean.nunique() >= 2:
                xv = x_clean.values
                yv = y_clean.values
                if color_use_recency:
                    slope, intercept = np.polyfit(xv, yv, 1, w=scatter_w)
                    r = _weighted_corr(xv, yv, scatter_w, "pearson")
                    fit_label = f"WLS fit (weighted R²={r ** 2:.3f})"
                else:
                    slope, intercept = np.polyfit(xv, yv, 1)
                    r = float(np.corrcoef(xv, yv)[0, 1])
                    fit_label = f"OLS fit (R²={r ** 2:.3f})"
                fit_x = np.array([float(x_clean.min()), float(x_clean.max())])
                fit_y = slope * fit_x + intercept
                scatter_fig.add_trace(go.Scatter(
                    x=fit_x, y=fit_y, mode="lines",
                    line=dict(color="orange", width=2),
                    name=fit_label,
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
