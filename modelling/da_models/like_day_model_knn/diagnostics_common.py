"""Shared figure helpers for the three model dashboards.

Each model's single_day.py composes a dashboard from the helpers below.
The helpers are split into:

  * Day-level helpers (used by per_day_daily_features and per_day_hourly_features):
    consume an analogs DataFrame keyed by day.
  * Hour-level helpers (used by per_hour): consume an analogs DataFrame
    keyed by (hour_ending, date).
  * Common helpers (target hourly values, hourly forecast/error/quantile
    figures): operate on a forecast_table that's identical in shape across
    all three models.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from da_models.like_day_model_knn import configs

Section = tuple[str, Any, str | None]

PLOTLY_TEMPLATE = "plotly_dark"
HOURS = list(range(1, 25))
LMP_COLS = [f"lmp_h{h}" for h in HOURS]
COLOR_TARGET = "#8dd9ff"
COLOR_FORECAST = "#f87171"
COLOR_ACTUAL = "#34d399"
COLOR_ANALOG = "#60a5fa"
COLOR_ERROR = "#f59e0b"

_YEAR_PALETTE = [
    "#60a5fa", "#34d399", "#f59e0b", "#f87171", "#a78bfa",
    "#22d3ee", "#f472b6", "#facc15", "#84cc16", "#fb923c",
]


# ─────────────────────────── target hourly values ───────────────────────


def hourly_load_table(target_date: date, hourly_rto: pd.DataFrame) -> pd.DataFrame:
    """Hourly load forecast for the target date plus first-difference ramp."""
    out = hourly_rto[hourly_rto["date"] == target_date].copy()
    if len(out) == 0:
        return pd.DataFrame(columns=["hour_ending", "forecast_load_mw", "ramp_mw"])
    out = out.sort_values("hour_ending")
    out["ramp_mw"] = out["forecast_load_mw"].diff()
    return out[["hour_ending", "forecast_load_mw", "ramp_mw"]].reset_index(drop=True)


def hourly_values_fig(df: pd.DataFrame) -> go.Figure:
    """Hourly load + ramp, with daily summary features overlaid."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(
        x=df["hour_ending"], y=df["forecast_load_mw"],
        mode="lines+markers", name="Forecast load",
        line=dict(color=COLOR_TARGET, width=2.5),
        hovertemplate="HE %{x}<br>%{y:,.0f} MW<extra></extra>",
    ), secondary_y=False)

    ramp = df["ramp_mw"].fillna(-np.inf) if len(df) else pd.Series(dtype=float)
    ramp_max_idx = int(ramp.idxmax()) if len(ramp) and ramp.notna().any() else None
    bar_colors = [COLOR_ERROR] * len(df)
    if ramp_max_idx is not None and 0 <= ramp_max_idx < len(bar_colors):
        bar_colors[ramp_max_idx] = "#ef4444"
    fig.add_trace(go.Bar(
        x=df["hour_ending"], y=df["ramp_mw"],
        name="Hourly ramp", marker_color=bar_colors, opacity=0.55,
        hovertemplate="HE %{x}<br>Ramp %{y:+,.0f} MW<extra></extra>",
    ), secondary_y=True)

    if len(df) and df["forecast_load_mw"].notna().any():
        load = df["forecast_load_mw"]
        he = df["hour_ending"]
        peak_pos = int(load.idxmax())
        valley_pos = int(load.idxmin())
        for y, name, dash in [
            (float(load.mean()), f"daily_avg {load.mean():,.0f}", "dot"),
            (float(load.iloc[peak_pos]),
             f"daily_peak {load.iloc[peak_pos]:,.0f} (HE{int(he.iloc[peak_pos])})", "dash"),
            (float(load.iloc[valley_pos]),
             f"daily_valley {load.iloc[valley_pos]:,.0f} (HE{int(he.iloc[valley_pos])})", "dash"),
        ]:
            fig.add_trace(go.Scatter(
                x=[1, 24], y=[y, y], mode="lines",
                name=name, line=dict(color="#9ca3af", width=1, dash=dash),
                hoverinfo="skip",
            ), secondary_y=False)

        def _ramp_segment(h_start: int, h_end: int, label: str, color: str) -> None:
            a = df.loc[df["hour_ending"] == h_start, "forecast_load_mw"]
            b = df.loc[df["hour_ending"] == h_end, "forecast_load_mw"]
            if a.empty or b.empty or pd.isna(a.iloc[0]) or pd.isna(b.iloc[0]):
                return
            y0, y1 = float(a.iloc[0]), float(b.iloc[0])
            delta = y1 - y0
            fig.add_trace(go.Scatter(
                x=[h_start, h_end], y=[y0, y1],
                mode="lines+markers+text",
                name=f"{label} {delta:+,.0f}",
                text=["", f"Δ {delta:+,.0f}"], textposition="top center",
                line=dict(color=color, width=3),
                marker=dict(size=8, color=color),
                hovertemplate=f"{label}<br>HE %{{x}}: %{{y:,.0f}} MW<extra></extra>",
            ), secondary_y=False)

        _ramp_segment(5, 8, "morning_ramp", "#22d3ee")
        _ramp_segment(15, 20, "evening_ramp", "#f472b6")

    fig.update_layout(
        title="Hourly load forecast with daily features overlaid",
        template=PLOTLY_TEMPLATE, height=460,
        margin=dict(l=60, r=60, t=60, b=60), hovermode="x unified",
    )
    fig.update_xaxes(title_text="Hour Ending", dtick=1, range=[0.5, 24.5])
    fig.update_yaxes(title_text="MW", secondary_y=False)
    fig.update_yaxes(title_text="Ramp MW", secondary_y=True)
    return fig


# ─── day-level analog helpers (per_day_daily_features / per_day_hourly_features) ─


def analog_weights_fig_day(analogs: pd.DataFrame) -> go.Figure:
    """Horizontal bar of selected day-level analogs - weight by date, color by year."""
    if len(analogs) == 0:
        return go.Figure()
    df = analogs[["rank", "date", "distance", "weight"]].copy()
    dt = pd.to_datetime(df["date"])
    df["label"] = dt.dt.strftime("%a %Y-%m-%d")
    df["year"] = dt.dt.year.astype(int)
    df["weight_pct"] = df["weight"].astype(float) * 100.0
    df = df.sort_values("weight", ascending=True).reset_index(drop=True)

    years = sorted(df["year"].unique())
    color_map = {y: _YEAR_PALETTE[i % len(_YEAR_PALETTE)] for i, y in enumerate(years)}

    fig = go.Figure()
    for year in years:
        part = df[df["year"] == year]
        fig.add_trace(go.Bar(
            x=part["weight_pct"], y=part["label"], orientation="h",
            marker_color=color_map[year], name=str(year),
            text=[f"{w:.2f}% (rank #{r})" for w, r in zip(part["weight_pct"], part["rank"])],
            textposition="outside",
            customdata=np.stack(
                [part["rank"].to_numpy(), part["distance"].to_numpy()], axis=-1,
            ),
            hovertemplate=(
                "%{y}<br>weight=%{x:.2f}%<br>"
                "rank=#%{customdata[0]}<br>distance=%{customdata[1]:.4f}<extra></extra>"
            ),
        ))
    fig.update_layout(
        title="Selected analogs: weight by analog day (color = year)",
        template=PLOTLY_TEMPLATE,
        height=max(360, 26 * len(df) + 160),
        margin=dict(l=170, r=80, t=60, b=60), bargap=0.25,
        legend=dict(title="Year"),
    )
    fig.update_xaxes(title_text="Analog weight (%)")
    fig.update_yaxes(
        categoryorder="array",
        categoryarray=df["label"].tolist(),
        automargin=True,
    )
    return fig


def analog_load_overlay_fig_day(
    analogs: pd.DataFrame, target_date: date, hourly_rto: pd.DataFrame,
) -> go.Figure:
    """Hourly load curves: target day vs each selected analog (width/opacity ~ weight)."""
    fig = go.Figure()
    weights = pd.to_numeric(analogs["weight"], errors="coerce").fillna(0.0)
    w_max = float(weights.max()) if len(weights) and weights.max() > 0 else 1.0

    for _, row in analogs.sort_values("weight", ascending=True).iterrows():
        analog_date = pd.to_datetime(row["date"]).date()
        analog_df = hourly_rto[hourly_rto["date"] == analog_date].sort_values("hour_ending")
        if len(analog_df) == 0:
            continue
        w = float(row["weight"])
        w_norm = w / w_max
        opacity = 0.18 + 0.72 * w_norm
        width = 0.8 + 3.7 * w_norm
        rank = int(row["rank"])
        label = pd.to_datetime(row["date"]).strftime("%a %Y-%m-%d")
        fig.add_trace(go.Scatter(
            x=analog_df["hour_ending"], y=analog_df["forecast_load_mw"],
            mode="lines",
            name=f"#{rank} {label} ({w * 100:.1f}%)",
            line=dict(color=COLOR_ANALOG, width=width),
            opacity=opacity,
            hovertemplate=f"#{rank} {label}<br>HE %{{x}}: %{{y:,.0f}} MW<extra></extra>",
        ))

    target_df = hourly_rto[hourly_rto["date"] == target_date].sort_values("hour_ending")
    if len(target_df):
        fig.add_trace(go.Scatter(
            x=target_df["hour_ending"], y=target_df["forecast_load_mw"],
            mode="lines+markers", name=f"Target {target_date}",
            line=dict(color=COLOR_TARGET, width=4),
            marker=dict(size=7),
            hovertemplate=f"Target {target_date}<br>HE %{{x}}: %{{y:,.0f}} MW<extra></extra>",
        ))

    fig.update_layout(
        title="Analog load curves vs target (line width / opacity proportional to analog weight)",
        template=PLOTLY_TEMPLATE, height=480,
        margin=dict(l=60, r=40, t=60, b=60), hovermode="x unified",
    )
    fig.update_xaxes(title_text="Hour Ending", dtick=1, range=[0.5, 24.5])
    fig.update_yaxes(title_text="MW")
    return fig


# ─────────────────────────── per-hour analog helpers (per_hour) ─────────


def analog_picks_heatmap_hour(analogs: pd.DataFrame) -> go.Figure:
    """Per-(hour, rank) heatmap of analog dates picked by the per_hour engine.

    Color intensity = analog weight; hover shows the actual analog date.
    Lets you see at a glance whether per-hour picks cluster around the same
    days or spread across many.
    """
    if len(analogs) == 0:
        return go.Figure()
    df = analogs.copy()
    df["weight_pct"] = df["weight"].astype(float) * 100.0
    df["date_label"] = pd.to_datetime(df["date"]).dt.strftime("%a %Y-%m-%d")

    pivot_w = df.pivot_table(
        index="hour_ending", columns="rank", values="weight_pct", aggfunc="first",
    ).sort_index()
    pivot_d = df.pivot_table(
        index="hour_ending", columns="rank", values="date_label", aggfunc="first",
    ).reindex(index=pivot_w.index, columns=pivot_w.columns)

    fig = go.Figure(data=go.Heatmap(
        z=pivot_w.to_numpy(dtype=float),
        x=[f"#{int(c)}" for c in pivot_w.columns],
        y=[f"HE{int(h)}" for h in pivot_w.index],
        colorscale="Viridis",
        colorbar=dict(title="Weight (%)"),
        customdata=pivot_d.to_numpy(),
        hovertemplate="%{y} %{x}<br>%{customdata}<br>weight=%{z:.2f}%<extra></extra>",
    ))
    fig.update_layout(
        title="per_hour analog picks: rank × HE (color = weight %, hover = analog date)",
        template=PLOTLY_TEMPLATE,
        height=max(420, 22 * 24 + 140),
        margin=dict(l=70, r=40, t=70, b=60),
    )
    return fig


def analog_date_frequency_fig_hour(analogs: pd.DataFrame) -> go.Figure:
    """Bar chart of how many HEs each candidate date appears in (per_hour only).

    For day-level matching, each analog date contributes to all 24 hours by
    construction. For per-hour matching, a candidate date may appear in any
    subset of HEs - this chart shows how concentrated picks are across days.
    """
    if len(analogs) == 0:
        return go.Figure()
    df = analogs.copy()
    df["date_str"] = pd.to_datetime(df["date"]).dt.strftime("%a %Y-%m-%d")
    summary = (
        df.groupby("date_str")
          .agg(n_hours=("hour_ending", "nunique"),
               total_weight=("weight", "sum"))
          .reset_index()
          .sort_values("total_weight", ascending=True)
    )
    fig = go.Figure(go.Bar(
        x=summary["total_weight"] * 100.0,
        y=summary["date_str"],
        orientation="h",
        marker_color=COLOR_ANALOG,
        text=[f"{n} HEs" for n in summary["n_hours"]],
        textposition="outside",
        customdata=summary["n_hours"].to_numpy(),
        hovertemplate=(
            "%{y}<br>summed weight=%{x:.2f}%<br>"
            "appeared in %{customdata} HEs<extra></extra>"
        ),
    ))
    fig.update_layout(
        title="per_hour analog date frequency: summed weight (across all HEs) per candidate date",
        template=PLOTLY_TEMPLATE,
        height=max(400, 18 * len(summary) + 140),
        margin=dict(l=170, r=80, t=70, b=60),
    )
    fig.update_xaxes(title_text="Summed weight (%) across all HEs picked")
    fig.update_yaxes(automargin=True)
    return fig


# ─────────────────────────── shared forecast/error figs ─────────────────


def hourly_forecast_table(
    df_forecast: pd.DataFrame,
    actuals: dict[int, float] | None,
) -> pd.DataFrame:
    """Annotate the per-hour forecast table with actual / error columns."""
    if len(df_forecast) == 0:
        return pd.DataFrame()
    out = df_forecast.copy()
    out["hour_ending"] = out["hour_ending"].astype(int)
    if actuals is not None:
        out["actual_lmp"] = out["hour_ending"].map(actuals)
        out["error"] = out["point_forecast"] - out["actual_lmp"]
        out["abs_error"] = out["error"].abs()
    else:
        out["actual_lmp"] = np.nan
        out["error"] = np.nan
        out["abs_error"] = np.nan
    first = ["hour_ending", "actual_lmp", "point_forecast", "error", "abs_error"]
    rest = [c for c in out.columns if c not in first]
    return out[first + rest]


def forecast_fig(forecast_table: pd.DataFrame, hub: str) -> go.Figure:
    """Actual + point forecast + P10-P90 band per HE."""
    fig = go.Figure()
    if "q_0.10" in forecast_table.columns and "q_0.90" in forecast_table.columns:
        fig.add_trace(go.Scatter(
            x=forecast_table["hour_ending"], y=forecast_table["q_0.90"],
            mode="lines", line=dict(color="#a78bfa", width=0),
            showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=forecast_table["hour_ending"], y=forecast_table["q_0.10"],
            mode="lines", line=dict(color="#a78bfa", width=0),
            fill="tonexty", fillcolor="rgba(167,139,250,0.20)",
            name="P10-P90",
            hovertemplate="P10 %{y:.2f}<extra></extra>",
        ))

    fig.add_trace(go.Scatter(
        x=forecast_table["hour_ending"], y=forecast_table["point_forecast"],
        mode="lines+markers", name="Forecast",
        line=dict(color=COLOR_FORECAST, width=2.8, dash="dash"),
        marker=dict(size=5),
        hovertemplate="Forecast<br>HE %{x}: $%{y:.2f}/MWh<extra></extra>",
    ))
    if forecast_table["actual_lmp"].notna().any():
        fig.add_trace(go.Scatter(
            x=forecast_table["hour_ending"], y=forecast_table["actual_lmp"],
            mode="lines+markers", name="Actual",
            line=dict(color=COLOR_ACTUAL, width=2.8),
            marker=dict(size=5),
            hovertemplate="Actual<br>HE %{x}: $%{y:.2f}/MWh<extra></extra>",
        ))

    fig.update_layout(
        title=f"Hourly forecast verification: {hub} DA LMP",
        template=PLOTLY_TEMPLATE, height=480,
        margin=dict(l=60, r=40, t=60, b=60), hovermode="x unified",
    )
    fig.update_xaxes(title_text="Hour Ending", dtick=1, range=[0.5, 24.5])
    fig.update_yaxes(title_text="$/MWh")
    return fig


def hourly_error_fig(forecast_table: pd.DataFrame) -> go.Figure:
    """Signed error bars + abs error line."""
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=forecast_table["hour_ending"], y=forecast_table["error"],
        name="Error forecast minus actual",
        marker_color=np.where(forecast_table["error"].fillna(0) >= 0, "#f87171", "#60a5fa"),
        hovertemplate="HE %{x}<br>Error $%{y:+.2f}/MWh<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=forecast_table["hour_ending"], y=forecast_table["abs_error"],
        mode="lines+markers", name="Abs error",
        line=dict(color=COLOR_ERROR, width=2),
        hovertemplate="HE %{x}<br>Abs error $%{y:.2f}/MWh<extra></extra>",
    ))
    fig.update_layout(
        title="Hourly miss verification: forecast error by hour",
        template=PLOTLY_TEMPLATE, height=420,
        margin=dict(l=60, r=40, t=60, b=60), hovermode="x unified",
    )
    fig.update_xaxes(title_text="Hour Ending", dtick=1, range=[0.5, 24.5])
    fig.update_yaxes(title_text="$/MWh")
    return fig


def summary_html(
    target_date: date,
    spec_name: str,
    spec_description: str,
    n_pool: int,
    n_analogs_total: int,
    forecast_table: pd.DataFrame,
    hub: str,
    season_window_days: int,
) -> str:
    has_actuals = (
        len(forecast_table) > 0 and forecast_table["actual_lmp"].notna().any()
    )
    if has_actuals:
        mae = float(forecast_table["abs_error"].mean())
        rmse = float(np.sqrt((forecast_table["error"] ** 2).mean()))
        bias = float(forecast_table["error"].mean())
        worst = forecast_table.sort_values("abs_error", ascending=False).head(1)
        worst_text = ""
        if len(worst):
            r = worst.iloc[0]
            worst_text = f"HE{int(r['hour_ending'])} {r['error']:+.2f}"
    else:
        mae = rmse = bias = float("nan")
        worst_text = ""

    return f"""
    <div style="padding:14px 16px;color:#dbe7ff;line-height:1.5;">
      <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;">
        {_metric("Model", spec_name.upper())}
        {_metric("Target date", str(target_date))}
        {_metric("Hub", hub)}
        {_metric("Season window", f"+/-{season_window_days}d")}
        {_metric("Pool rows", f"{n_pool:,}")}
        {_metric("Analog rows", f"{n_analogs_total:,}")}
        {_metric("MAE", _fmt(mae, ".2f"))}
        {_metric("RMSE", _fmt(rmse, ".2f"), worst_text)}
      </div>
      <div style="margin-top:10px;color:#9eb4d3;font-size:13px;">
        {spec_description}
      </div>
      <div style="margin-top:6px;color:#6f8db1;font-size:12px;">
        Bias: {_fmt(bias, "+.2f")}
      </div>
    </div>
    """


def _metric(label: str, value: str, subvalue: str = "") -> str:
    sub = (
        f"<div style='font-size:11px;color:#6f8db1;margin-top:2px;'>{subvalue}</div>"
        if subvalue else ""
    )
    return (
        "<div style='background:#111d31;border:1px solid #253b59;border-radius:6px;padding:10px;'>"
        f"<div style='font-size:11px;color:#6f8db1;text-transform:uppercase;font-weight:700;'>{label}</div>"
        f"<div style='font-size:20px;color:#e6efff;font-weight:700;'>{value}</div>{sub}</div>"
    )


def _fmt(value: float, fmt: str) -> str:
    if value != value:  # NaN
        return "-"
    return format(value, fmt)


def empty_fragment(msg: str) -> str:
    return f"<div style='padding:14px;color:#f87171;font-family:monospace;'>{msg}</div>"


# Note: load_hourly_rto now lives in ``_shared.py`` (call sites import it directly).
