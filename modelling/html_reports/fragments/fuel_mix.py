"""Fuel mix generation charts — per-fuel-type profile + ramp side-by-side.

One section per fuel type (dispatch stack order), each with hourly generation
profile (left) and hourly ramp (right) as side-by-side subplots.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from da_models.common import configs
from da_models.common.data.loader import load_fuel_mix
from utils.logging_utils import get_logger

logger = get_logger()

PLOTLY_TEMPLATE = "plotly_dark"
PROFILE_LOOKBACK_DAYS = 30

_FUEL_TYPES = [
    ("nuclear", "Nuclear Gen"),
    ("coal", "Coal Gen"),
    ("gas", "Gas Gen"),
    ("oil", "Oil Gen"),
    ("hydro", "Hydro Gen"),
    ("solar", "Solar Gen"),
    ("wind", "Wind Gen"),
]

Section = tuple[str, Any, str | None]

LAST_N_COLORS = ["#EF553B", "#636EFA", "#00CC96"]


def build_fragments(
    schema: str | None = None,
    cache_dir: Path | None = None,
    cache_enabled: bool = True,
    cache_ttl_hours: float = 24.0,
    force_refresh: bool = False,
) -> list[Section]:
    """Load fuel mix data from parquet cache, return per-fuel profile + ramp sections.

    The registry passes (schema, cache_dir, cache_enabled, cache_ttl_hours,
    force_refresh) — only `cache_dir` is meaningful here since the loader
    reads parquet directly; the others are accepted for signature parity.
    """
    logger.info("Building fuel mix report fragments...")

    try:
        df = load_fuel_mix(cache_dir=cache_dir or configs.CACHE_DIR)
    except FileNotFoundError as exc:
        logger.warning(f"Fuel mix parquet not found: {exc}")
        return [("Fuel Mix", _empty("Fuel mix parquet not available."), None)]

    if df is None or len(df) == 0:
        return [("Fuel Mix", _empty("Fuel mix parquet is empty."), None)]

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    recent = _prep_recent(df)
    if len(recent) == 0:
        return [("Fuel Mix", _empty("No fuel mix data in lookback window."), None)]

    sections: list[Section] = []
    for col, label in _FUEL_TYPES:
        if col not in recent.columns:
            continue
        if recent[col].abs().sum() == 0:
            continue
        sections.append((
            label,
            _profile_and_ramp_fig(recent, col, label, PROFILE_LOOKBACK_DAYS),
            None,
        ))

    return sections


def _prep_recent(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to last 30 days and remap hour 0 → HE 24."""
    cutoff = df["date"].max() - pd.Timedelta(days=PROFILE_LOOKBACK_DAYS)
    recent = df[df["date"] >= cutoff].copy()
    recent["hour_ending"] = recent["hour_ending"].replace(0, 24)
    recent = recent[recent["hour_ending"].between(1, 24)]
    return recent


def _empty(text: str) -> str:
    return f"<div style='padding:16px;color:#e74c3c;'>{text}</div>"


def _profile_and_ramp_fig(
    recent: pd.DataFrame,
    col: str,
    label: str,
    lookback_days: int = PROFILE_LOOKBACK_DAYS,
) -> go.Figure:
    """Side-by-side: hourly gen profile (left) + hourly ramp (right)."""
    window_label = f"{lookback_days}d"
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(f"{label} — Hourly Profile", f"{label} — Hourly Ramp"),
        horizontal_spacing=0.08,
    )

    ramp_data = recent.sort_values(["date", "hour_ending"]).copy()
    ramp_col = f"{col}_ramp"
    ramp_data[ramp_col] = ramp_data.groupby("date")[col].diff()
    ramp_data = ramp_data.dropna(subset=[ramp_col])

    all_dates = sorted(recent["date"].unique())
    last_3 = all_dates[-3:]
    other_dates = all_dates[:-3]
    last_3_rev = list(reversed(last_3))
    other_dates_rev = list(reversed(other_dates))

    hourly_stats = recent.groupby("hour_ending")[col].agg(["min", "max"])

    fig.add_trace(go.Scatter(
        x=hourly_stats.index, y=hourly_stats["max"],
        mode="lines", name=f"{window_label} Max",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
        showlegend=True, legendgroup="envelope",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=hourly_stats.index, y=hourly_stats["min"],
        mode="lines", name=f"{window_label} Min/Max Range",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
        fill="tonexty", fillcolor="rgba(99, 110, 250, 0.15)",
        showlegend=True, legendgroup="envelope",
    ), row=1, col=1)

    for i, dt in enumerate(last_3_rev):
        day = recent[recent["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Scatter(
            x=day["hour_ending"], y=day[col],
            mode="lines+markers", name=day_label,
            line=dict(color=LAST_N_COLORS[i % len(LAST_N_COLORS)], width=2),
            marker=dict(size=4),
            showlegend=True, legendgroup=day_label,
        ), row=1, col=1)

    for dt in other_dates_rev:
        day = recent[recent["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Scatter(
            x=day["hour_ending"], y=day[col],
            mode="lines", name=day_label,
            line=dict(color="rgba(160, 174, 200, 0.4)", width=0.8),
            visible="legendonly",
            showlegend=True, legendgroup=day_label,
        ), row=1, col=1)

    ramp_stats = ramp_data.groupby("hour_ending")[ramp_col].agg(["min", "max"])

    fig.add_trace(go.Scatter(
        x=ramp_stats.index, y=ramp_stats["max"],
        mode="lines", name=f"{window_label} Max",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
        showlegend=False, legendgroup="envelope",
    ), row=1, col=2)
    fig.add_trace(go.Scatter(
        x=ramp_stats.index, y=ramp_stats["min"],
        mode="lines", name=f"{window_label} Min/Max Range",
        line=dict(color="rgba(99, 110, 250, 0.3)", width=0),
        fill="tonexty", fillcolor="rgba(99, 110, 250, 0.15)",
        showlegend=False, legendgroup="envelope",
    ), row=1, col=2)

    ramp_dates = sorted(ramp_data["date"].unique())
    ramp_last_3 = ramp_dates[-3:]
    ramp_other = ramp_dates[:-3]
    ramp_last_3_rev = list(reversed(ramp_last_3))
    ramp_other_rev = list(reversed(ramp_other))

    for i, dt in enumerate(ramp_last_3_rev):
        day = ramp_data[ramp_data["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Bar(
            x=day["hour_ending"], y=day[ramp_col],
            name=day_label,
            marker_color=LAST_N_COLORS[i % len(LAST_N_COLORS)],
            opacity=0.8,
            showlegend=False, legendgroup=day_label,
        ), row=1, col=2)

    for dt in ramp_other_rev:
        day = ramp_data[ramp_data["date"] == dt].sort_values("hour_ending")
        day_label = str(dt.date()) if hasattr(dt, "date") else str(dt)
        fig.add_trace(go.Bar(
            x=day["hour_ending"], y=day[ramp_col],
            name=day_label,
            marker_color="rgba(160, 174, 200, 0.5)",
            visible="legendonly",
            showlegend=False, legendgroup=day_label,
        ), row=1, col=2)

    fig.add_hline(y=0, line_dash="dash", line_color="rgba(255, 255, 255, 0.5)",
                  line_width=1, row=1, col=2)

    fig.update_layout(
        title=f"{label} — Last {lookback_days} Days",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(font=dict(size=10)),
        barmode="group",
    )
    fig.update_xaxes(dtick=1, range=[0.5, 24.5], title_text="Hour Ending", col=1)
    fig.update_xaxes(dtick=1, range=[1.5, 24.5], title_text="Hour Ending", col=2)
    fig.update_yaxes(title_text="MW", col=1)
    fig.update_yaxes(title_text="MW/hr", col=2)
    return fig
