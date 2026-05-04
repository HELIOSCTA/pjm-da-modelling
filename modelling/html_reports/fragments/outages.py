"""Outages fragments.

Primary section: four vintage heatmap tables (Total / Forced / Planned / Maint)
indexed by forecast execution date × forecast date, for the RTO region.

Optional: seasonal yearly overlay charts over day-of-year, one per outage type.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go

from da_models.common import configs
from da_models.common.data.loader import (
    load_outages_actual,
    load_outages_forecast_history,
)
from utils.logging_utils import get_logger

logger = get_logger()

PLOTLY_TEMPLATE = "plotly_dark"

Section = tuple[str, Any, str | None]

_OUTAGE_TYPES = [
    ("Total Outages", "total_outages_mw"),
    ("Planned Outages", "planned_outages_mw"),
    ("Forced Outages", "forced_outages_mw"),
    ("Maint Outages", "maintenance_outages_mw"),
]

_YEAR_COLORS = [
    "#00cc96", "#636efa", "#ef553b", "#ab63fa", "#ffa15a", "#19d3f3", "#ff6692",
]

_MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
_MONTH_STARTS = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
_SEASON_MAP = {
    1: "WINTER", 2: "WINTER", 3: "WINTER",
    4: "SUMMER", 5: "SUMMER", 6: "SUMMER", 7: "SUMMER",
    8: "SUMMER", 9: "SUMMER", 10: "SUMMER",
    11: "WINTER", 12: "WINTER",
}


def build_fragments(
    schema: str | None = None,
    cache_dir: Path | None = None,
    cache_enabled: bool = True,
    cache_ttl_hours: float = 24.0,
    force_refresh: bool = False,
    include_seasonal: bool = True,
) -> list[Section]:
    """Build outages fragments — forecast vintage heatmaps + optional seasonal overlay."""
    logger.info("Building outages report fragments...")
    cache_dir = cache_dir or configs.CACHE_DIR

    sections: list[Section] = []
    sections.append("Forecast Vintage Heatmaps")
    sections.extend(_build_forecast_sections(cache_dir))

    if include_seasonal:
        sections.append("Seasonal Overlay")
        sections.extend(_build_seasonal_sections(cache_dir))

    return sections


def _build_forecast_sections(cache_dir: Path) -> list[Section]:
    try:
        df = load_outages_forecast_history(cache_dir=cache_dir, lead_days=None)
    except FileNotFoundError as exc:
        logger.warning(f"Outages forecast parquet not found: {exc}")
        return [("Forecast Outages RTO", _empty("No outage forecast data available."), None)]

    if df is None or len(df) == 0:
        return [("Forecast Outages RTO", _empty("No outage forecast data available."), None)]

    df = df[df["region"] == configs.LOAD_REGION].copy()
    if len(df) == 0:
        return [("Forecast Outages RTO", _empty(f"No forecast data for region {configs.LOAD_REGION}."), None)]

    # Heatmap renderer expects a `date` column (forecast target date).
    if "date" not in df.columns and "forecast_date" in df.columns:
        df["date"] = df["forecast_date"]

    # Derive forecast_rank locally — newest vintage per forecast_date is rank N.
    if "forecast_date" in df.columns and "as_of_date" in df.columns:
        df["forecast_rank"] = (
            df.groupby("forecast_date")["as_of_date"]
            .rank(method="dense", ascending=True)
            .astype(int)
        )

    if "forecast_rank" in df.columns:
        df = df.sort_values("forecast_rank", ascending=False)
    else:
        df = df.sort_values("forecast_execution_date", ascending=False)
    df = df.drop_duplicates(subset=["forecast_execution_date", "date"], keep="first")

    exec_dates = sorted(df["forecast_execution_date"].unique(), reverse=True)[:8]
    df = df[df["forecast_execution_date"].isin(exec_dates)].copy()

    if len(df) == 0:
        return [("Forecast Outages RTO", _empty("No recent forecast data."), None)]

    sections: list[Section] = []
    for type_label, col in _OUTAGE_TYPES:
        if col not in df.columns:
            continue
        html = _render_heatmap_table(df, col, type_label, exec_dates)
        sections.append((type_label, html, None))
    return sections


def _build_seasonal_sections(cache_dir: Path) -> list[Section]:
    try:
        df = load_outages_actual(cache_dir=cache_dir)
    except FileNotFoundError as exc:
        logger.warning(f"Outages actual parquet not found: {exc}")
        return [("Seasonal Outages RTO", _empty("No historical outage data available."), None)]

    if df is None or len(df) == 0:
        return [("Seasonal Outages RTO", _empty("No historical outage data available."), None)]

    df = df[df["region"] == configs.LOAD_REGION].copy()
    if len(df) == 0:
        return [("Seasonal Outages RTO", _empty(f"No actuals for region {configs.LOAD_REGION}."), None)]

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["day_of_year"] = df["date"].dt.dayofyear

    sections: list[Section] = []
    for type_label, col in _OUTAGE_TYPES:
        if col not in df.columns:
            continue
        fig = _build_seasonal_chart(df, col, f"{type_label} (Seasonal)")
        sections.append((f"{type_label} (Seasonal)", fig, None))
    return sections


def _empty(text: str) -> str:
    return f"<div style='padding:16px;color:#e74c3c;'>{text}</div>"


def _label_exec_date(d, exec_dates_sorted: list) -> str:
    idx = exec_dates_sorted.index(d)
    if idx == 0:
        return "Current Forecast"
    if idx == 1:
        return "24hrs Ago"
    return pd.Timestamp(d).strftime("%a %b-%d")


def _heatmap_color(value: float, vmin: float, vmax: float) -> str:
    """Map value to green (high) → red (low) background color."""
    if pd.isna(value) or vmax == vmin:
        return "#ffffff"

    t = (value - vmin) / (vmax - vmin)

    if t < 0.5:
        ratio = t / 0.5
        r = int(248 - 8 * ratio)
        g = int(180 + 52 * ratio)
        b = int(180 - 30 * ratio)
    else:
        ratio = (t - 0.5) / 0.5
        r = int(240 - 82 * ratio)
        g = int(232 - 14 * ratio)
        b = int(150 + 30 * ratio)

    return f"rgb({r}, {g}, {b})"


def _render_heatmap_table(
    df: pd.DataFrame,
    value_col: str,
    type_label: str,
    exec_dates_sorted: list,
) -> str:
    """Build an HTML heatmap table for one outage type.

    Rows: forecast execution dates (newest first).
    Columns: forecast target dates.
    """
    forecast_dates = sorted(df["date"].unique())
    vmin = df[value_col].min()
    vmax = df[value_col].max()

    header_cells = (
        '<th class="oh-hdr oh-sticky-col">Forecast Exec.</th>'
        '<th class="oh-hdr">Forecast Label</th>'
    )
    for fd in forecast_dates:
        label = pd.Timestamp(fd).strftime("%a %b-%d")
        header_cells += f'<th class="oh-hdr">{label}</th>'

    rows_html = []
    for exec_dt in exec_dates_sorted:
        exec_label = _label_exec_date(exec_dt, exec_dates_sorted)
        exec_str = pd.Timestamp(exec_dt).strftime("%a %b-%d")

        cells = f'<td class="oh-dt oh-sticky-col">{exec_str}</td>'
        cells += f'<td class="oh-label">{exec_label}</td>'

        row_data = df[df["forecast_execution_date"] == exec_dt]
        for fd in forecast_dates:
            match = row_data[row_data["date"] == fd]
            if len(match) == 0:
                cells += '<td class="oh-cell oh-empty"></td>'
            else:
                val = match.iloc[0][value_col]
                if pd.isna(val):
                    cells += '<td class="oh-cell oh-empty"></td>'
                else:
                    bg = _heatmap_color(val, vmin, vmax)
                    cells += (
                        f'<td class="oh-cell" style="background:{bg};">'
                        f'{val:,.0f}</td>'
                    )

        rows_html.append(f"<tr>{cells}</tr>")

    table_html = f"""
<div class="oh-section-title">{type_label}</div>
<div class="oh-wrap">
<table class="oh-table">
<thead><tr>{header_cells}</tr></thead>
<tbody>
<tr><td colspan="{len(forecast_dates) + 2}" class="oh-sub-hdr">Forecast Date (Forecasts)</td></tr>
{"".join(rows_html)}
</tbody>
</table>
</div>
"""
    return _STYLE + table_html


def _build_seasonal_chart(df: pd.DataFrame, col: str, type_label: str) -> go.Figure:
    years = sorted(df["year"].unique())

    fig = go.Figure()

    for i, year in enumerate(years):
        yr_data = df[df["year"] == year].sort_values("day_of_year")
        color = _YEAR_COLORS[i % len(_YEAR_COLORS)]
        fig.add_trace(go.Scatter(
            x=yr_data["day_of_year"],
            y=yr_data[col],
            mode="lines",
            name=str(year),
            line=dict(color=color, width=1.5),
        ))

    for m_idx, (m_start, m_name) in enumerate(zip(_MONTH_STARTS, _MONTHS)):
        season = _SEASON_MAP[m_idx + 1]
        fig.add_vline(
            x=m_start, line_dash="dot",
            line_color="rgba(100, 130, 170, 0.3)", line_width=1,
        )
        mid_x = (m_start + (_MONTH_STARTS[m_idx + 1] if m_idx < 11 else 366)) / 2
        fig.add_annotation(
            x=mid_x, y=1.06, yref="paper",
            text=f"<b>{m_name[:3]}</b><br><span style='font-size:9px;color:#7a94b5'>{season}</span>",
            showarrow=False,
            font=dict(size=10, color="#9eb4d3"),
            align="center",
        )

    fig.update_layout(
        title=f"{type_label} — RTO Seasonal Overlay",
        xaxis_title="Day of Year",
        yaxis_title="MW",
        height=500,
        template=PLOTLY_TEMPLATE,
        legend=dict(
            font=dict(size=11),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        xaxis=dict(range=[1, 366], dtick=30),
        margin=dict(t=100),
    )
    return fig


_STYLE = """
<style>
.oh-section-title {
    font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, sans-serif;
    font-size: 14px;
    font-weight: 700;
    color: #c5d8f2;
    padding: 14px 12px 6px;
    letter-spacing: 0.3px;
}
.oh-wrap {
    overflow-x: auto;
    padding: 0 12px 16px;
    background: #ffffff;
    border-radius: 6px;
    border: 1px solid #d0d7de;
}
.oh-table {
    border-collapse: collapse;
    width: 100%;
    font-family: 'IBM Plex Sans', 'Segoe UI', Tahoma, sans-serif;
    font-size: 13px;
}
.oh-table th, .oh-table td {
    padding: 5px 12px;
    text-align: right;
    white-space: nowrap;
}
.oh-hdr {
    background: #f6f8fa;
    color: #24292f;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    border-bottom: 2px solid #d0d7de;
    position: sticky;
    top: 0;
    z-index: 2;
}
.oh-sub-hdr {
    text-align: center;
    color: #656d76;
    font-size: 11px;
    font-weight: 600;
    background: #f6f8fa;
    border-bottom: 1px solid #d0d7de;
    padding: 4px 0;
}
.oh-dt {
    text-align: left;
    color: #24292f;
    font-weight: 500;
    font-size: 12px;
    background: #f6f8fa;
}
.oh-sticky-col {
    position: sticky;
    left: 0;
    z-index: 1;
}
.oh-label {
    text-align: left;
    color: #656d76;
    font-size: 12px;
    font-style: italic;
}
.oh-cell {
    color: #24292f;
    font-variant-numeric: tabular-nums;
    font-size: 12px;
    font-weight: 500;
}
.oh-empty {
    color: #c0c0c0;
    background: #ffffff;
}
.oh-table tbody tr {
    border-bottom: 1px solid #e0e4e8;
}
.oh-table tbody tr:hover {
    outline: 1px solid #a0b0c0;
}
.oh-table tbody tr:hover .oh-dt {
    background: #eaeef2;
}
</style>
"""
