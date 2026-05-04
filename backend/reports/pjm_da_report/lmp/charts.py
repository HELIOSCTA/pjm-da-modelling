"""Plotly chart builders for the DA LMP report.

Two builders:
- `summary_panels_chart` — 3-panel subplot (Total / System / Congestion) with
  shared legend; legendgroup="<hub>" so a click toggles all 3 panels.
- `he_components_chart`  — one hub, 3 component traces (per-hub block).
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.graph_objects as go
from plotly import colors as pc
from plotly.subplots import make_subplots

from backend.reports._forecast_utils import PLOTLY_LOCKED_CONFIG, empty_html
from backend.reports.pjm_da_report.lmp.configs import PLOTLY_TEMPLATE

_RESPONSIVE_CONFIG = {**PLOTLY_LOCKED_CONFIG, "responsive": True}


def gradient_palette(
    scale_name: str,
    n: int,
    start: float = 0.4,
    end: float = 0.95,
) -> list[str]:
    """Sample n colors from a named Plotly sequential colorscale.

    `start` / `end` clamp away from the very-pale and very-dark extremes so
    every shade is readable against the dark theme.
    """
    if n == 0:
        return []
    if n == 1:
        return list(pc.sample_colorscale(scale_name, [end]))
    positions = [start + i * (end - start) / (n - 1) for i in range(n)]
    return list(pc.sample_colorscale(scale_name, positions))


def summary_panels_chart(
    df: pd.DataFrame,
    *,
    target_date: date,
    hubs: list[str],
    hub_colors: dict[str, str],
    components: list[tuple[str, str]],
    panel_hubs: dict[str, list[str]] | None = None,
    div_id: str,
) -> str:
    """Single figure with one panel per component.

    `components` is a list of (parquet_column, panel_label) in display order.
    `panel_hubs` (optional) maps a column name to the hubs to render for that
    panel — used to drop System lines on hubs other than the anchor since the
    System price is uniform. Missing keys default to `hubs`.

    Each hub adds traces sharing `legendgroup=hub` and `hub_colors[hub]`, so
    each hub appears in the same dedicated color across panels and a single
    legend click toggles its line in every panel where it's plotted.
    """
    panel_hubs = panel_hubs or {}
    n_panels = len(components)
    fig = make_subplots(
        rows=1, cols=n_panels,
        subplot_titles=tuple(f"{label}" for _col, label in components),
        horizontal_spacing=0.05,
        shared_xaxes=False,
    )

    sub_all = df[df["date"] == target_date]

    for hub in hubs:
        hub_df = sub_all[sub_all["hub"] == hub].sort_values("hour_ending")
        if hub_df.empty:
            continue
        color = hub_colors.get(hub, "#9ca3af")
        legend_shown = False
        for panel_idx, (value_col, _label) in enumerate(components):
            allowed = panel_hubs.get(value_col, hubs)
            if hub not in allowed:
                continue
            showlegend = not legend_shown
            legend_shown = True
            fig.add_trace(
                go.Scatter(
                    x=hub_df["hour_ending"],
                    y=hub_df[value_col],
                    mode="lines+markers",
                    name=hub,
                    legendgroup=hub,
                    showlegend=showlegend,
                    line=dict(color=color, width=2),
                    marker=dict(size=4),
                    hovertemplate=f"<b>{hub}</b><br>HE %{{x}}<br>%{{y:.2f}} $/MWh<extra></extra>",
                ),
                row=1, col=panel_idx + 1,
            )

    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        height=460,
        margin=dict(l=60, r=20, t=60, b=80),
        legend=dict(
            orientation="h",
            yanchor="top", y=-0.18,
            xanchor="center", x=0.5,
        ),
        hovermode="x unified",
    )
    for col in range(1, n_panels + 1):
        fig.update_xaxes(
            title_text="Hour Ending",
            dtick=2, range=[0.5, 24.5],
            autorange=False, fixedrange=True,
            row=1, col=col,
        )
        fig.update_yaxes(
            gridcolor="rgba(99,110,250,0.10)",
            row=1, col=col,
        )
    fig.update_yaxes(title_text="$/MWh", row=1, col=1)

    return fig.to_html(
        include_plotlyjs="cdn", full_html=False,
        div_id=div_id, config=_RESPONSIVE_CONFIG,
    )


def he_components_chart(
    df: pd.DataFrame,
    *,
    target_date: date,
    hub: str,
    title: str,
    div_id: str,
    color_total: str,
    color_system: str,
    color_cong: str,
    y_title: str = "$/MWh",
) -> str:
    """Per-hub chart: Total + System lines on primary y; Congestion bars on
    secondary y. Co-locating Congestion (bars) with Total/System (lines) on
    one panel makes it easy to read what's driving the regional spread.
    """
    sub = df[(df["date"] == target_date) & (df["hub"] == hub)].sort_values("hour_ending")
    if sub.empty:
        return empty_html(f"No data for {title}.")

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    line_traces = (
        ("Total",  "lmp_total",                color_total),
        ("System", "lmp_system_energy_price",  color_system),
    )
    for name, col, color in line_traces:
        fig.add_trace(
            go.Scatter(
                x=sub["hour_ending"],
                y=sub[col],
                mode="lines+markers",
                name=name,
                line=dict(color=color, width=2),
                marker=dict(size=4),
                hovertemplate=(
                    f"<b>{name}</b><br>HE %{{x}}<br>%{{y:.2f}} {y_title}<extra></extra>"
                ),
            ),
            secondary_y=False,
        )

    # Per-bar coloring: green when positive, red when negative — matches
    # trader convention (positive cong = export-constrained, negative =
    # import-constrained at this hub).
    cong_vals = sub["lmp_congestion_price"]
    bar_colors = [
        "#34d399" if (v is not None and not pd.isna(v) and float(v) > 0)
        else color_cong
        for v in cong_vals
    ]
    fig.add_trace(
        go.Bar(
            x=sub["hour_ending"],
            y=cong_vals,
            name="Congestion",
            marker=dict(color=bar_colors, opacity=0.65, line=dict(width=0)),
            hovertemplate=(
                f"<b>Congestion</b><br>HE %{{x}}<br>%{{y:.2f}} {y_title}<extra></extra>"
            ),
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title=title,
        template=PLOTLY_TEMPLATE,
        height=380,
        margin=dict(l=60, r=60, t=60, b=60),
        legend=dict(orientation="h", yanchor="top", y=-0.15, x=0),
        hovermode="x unified",
        autosize=True,
        bargap=0.20,
    )
    fig.update_xaxes(
        title_text="Hour Ending",
        dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True,
    )
    fig.update_yaxes(
        title_text=f"Total / System ({y_title})",
        gridcolor="rgba(99,110,250,0.10)",
        secondary_y=False,
    )
    fig.update_yaxes(
        title_text=f"Congestion ({y_title})",
        gridcolor="rgba(248,113,113,0.06)",
        zeroline=True, zerolinecolor="rgba(248,113,113,0.30)",
        secondary_y=True,
    )

    return fig.to_html(
        include_plotlyjs="cdn", full_html=False,
        div_id=div_id, config=_RESPONSIVE_CONFIG,
    )
