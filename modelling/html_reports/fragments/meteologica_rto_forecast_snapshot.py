"""Meteologica RTO + zonal forecast snapshot — latest forecast only.

Ported from helioscta-pjm-da meteologica_rto_forecast_snapshot.py, with vintages dropped.
Unlike PJM, Meteologica provides regional solar + wind for all four load regions,
so every region gets the full load+solar+wind+net-load stack.

Entry points:
    build_fragments          — RTO
    build_fragments_west     — Western
    build_fragments_midatl   — Mid-Atlantic
    build_fragments_south    — Southern
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from da_models.common import configs
from da_models.common.data.loader import (
    load_meteologica_load_forecast,
    load_meteologica_net_load_forecast,
    load_meteologica_solar_forecast,
    load_meteologica_wind_forecast,
)
from html_reports.fragments._forecast_utils import (
    COLORS, HE_COLS, OFFPEAK_HOURS, ONPEAK_HOURS, PLOTLY_LOCKED_CONFIG, PLOTLY_TEMPLATE,
    SNAPSHOT_STYLE, SUMMARY_COLS,
    cell_class, date_key, day_series, empty_html, fmt_cell, prep_hours,
    render_ramp_toggle, single_day_chart,
)
from utils.logging_utils import get_logger

logger = get_logger()

ET = ZoneInfo("America/New_York")

Section = tuple[str, Any, str | None]


# ══════════════════════════════════════════════════════════════════════
# Public entry points
# ══════════════════════════════════════════════════════════════════════


def build_fragments(
    schema: str | None = None,
    cache_dir: Path | None = None,
    cache_enabled: bool = True,
    cache_ttl_hours: float = 24.0,
    force_refresh: bool = False,
) -> list[Section]:
    return _build_for_region("RTO", "RTO", cache_dir=cache_dir or configs.CACHE_DIR)


def build_fragments_west(**kwargs) -> list[Section]:
    return _build_for_region("WEST", "Western", cache_dir=kwargs.get("cache_dir") or configs.CACHE_DIR)


def build_fragments_midatl(**kwargs) -> list[Section]:
    return _build_for_region("MIDATL", "Mid-Atlantic", cache_dir=kwargs.get("cache_dir") or configs.CACHE_DIR)


def build_fragments_south(**kwargs) -> list[Section]:
    return _build_for_region("SOUTH", "Southern", cache_dir=kwargs.get("cache_dir") or configs.CACHE_DIR)


# ══════════════════════════════════════════════════════════════════════
# Orchestration
# ══════════════════════════════════════════════════════════════════════


def _build_for_region(region_key: str, region_label: str, *, cache_dir: Path) -> list[Section]:
    logger.info(f"Building Meteologica {region_label} forecast snapshot...")

    # Prefer the pre-combined net_load parquet; fall back to joining the three
    # component parquets if it's missing.
    net_df = _maybe_load(load_meteologica_net_load_forecast, cache_dir, "Meteologica net_load")
    if not net_df.empty:
        region_df = prep_hours(net_df[net_df["region"] == region_key].copy())
        region_df = _pin_latest_as_of(region_df)
        if region_df.empty:
            return [(f"Meteologica {region_label} — No Data",
                     empty_html(f"No net_load rows for {region_label}."), None)]

        load_df = region_df[["date", "hour_ending", "forecast_load_mw"]].copy()
        solar_df = region_df[["date", "hour_ending", "solar_forecast"]].copy()
        wind_df = region_df[["date", "hour_ending", "wind_forecast"]].copy()
    else:
        load_df = _maybe_regional(load_meteologica_load_forecast, cache_dir, region_key, "load")
        solar_df = _maybe_regional(load_meteologica_solar_forecast, cache_dir, region_key, "solar")
        wind_df = _maybe_regional(load_meteologica_wind_forecast, cache_dir, region_key, "wind")

    if load_df.empty:
        return [(f"Meteologica {region_label} — No Data",
                 empty_html(f"No load forecast for {region_label}."), None)]

    available_dates = sorted(load_df["date"].unique())
    sections: list[Section] = []

    overview = _overview_chart(load_df, solar_df, wind_df, available_dates, region_label)
    sections.append((f"All Forecast Dates — {region_label}", overview, None))

    today = pd.Timestamp.now(ET).date()
    for dt in available_dates:
        day_offset = (dt - today).days
        dt_label = pd.Timestamp(dt).strftime("%a %b %d")
        suffix = f"(Day +{day_offset})" if day_offset >= 0 else f"(Day {day_offset})"
        section_name = f"{dt_label} {suffix}"
        dt_key = f"meteo-{region_key.lower()}-{date_key(dt)}"

        day_df = _build_day_frame(load_df, solar_df, wind_df, dt)

        content = SNAPSHOT_STYLE
        content += _render_snapshot_table(day_df, dt_key)
        content += _render_component_row(day_df, dt_key, region_label)
        content += _render_net_load_row(day_df, dt_key, region_label)

        sections.append((section_name, content, None))

    return sections


def _maybe_load(loader_fn, cache_dir: Path, label: str) -> pd.DataFrame:
    try:
        return loader_fn(cache_dir=cache_dir)
    except FileNotFoundError as exc:
        logger.warning(f"{label} parquet not found: {exc}")
        return pd.DataFrame()


def _maybe_regional(loader_fn, cache_dir: Path, region_key: str, label: str) -> pd.DataFrame:
    df = _maybe_load(loader_fn, cache_dir, f"Meteologica {label}")
    if df.empty:
        return df
    return _pin_latest_as_of(prep_hours(df[df["region"] == region_key].copy()))


def _pin_latest_as_of(df: pd.DataFrame) -> pd.DataFrame:
    """Historical parquets carry every as_of_date snapshot; pin to the newest
    so the snapshot fragment shows one consistent vintage."""
    if df.empty or "as_of_date" not in df.columns:
        return df
    latest = df["as_of_date"].max()
    return df[df["as_of_date"] == latest].copy()


# ══════════════════════════════════════════════════════════════════════
# Overview across all dates — stacked area + grouped ramp bars
# ══════════════════════════════════════════════════════════════════════


def _overview_chart(
    load_df: pd.DataFrame, solar_df: pd.DataFrame, wind_df: pd.DataFrame,
    dates: list, region_label: str,
) -> str:
    rows = []
    for dt in dates:
        load_s = day_series(load_df, dt, "forecast_load_mw")
        solar_s = day_series(solar_df, dt, "solar_forecast")
        wind_s = day_series(wind_df, dt, "wind_forecast")
        if load_s.empty:
            continue
        for h in range(1, 25):
            ld = load_s.get(h)
            sl = solar_s.get(h)
            wn = wind_s.get(h)
            if pd.isna(ld):
                continue
            sl = 0.0 if pd.isna(sl) else sl
            wn = 0.0 if pd.isna(wn) else wn
            rows.append({
                "datetime": pd.Timestamp(dt) + pd.Timedelta(hours=h),
                "date_label": pd.Timestamp(dt).strftime("%a %b-%d"),
                "he": h, "load": ld, "solar": sl, "wind": wn,
                "net_load": ld - sl - wn,
            })
    if not rows:
        return empty_html(f"No net load data for {region_label}.")

    df = pd.DataFrame(rows).sort_values("datetime")
    for col in ["load", "solar", "wind", "net_load"]:
        df[f"{col}_ramp"] = df[col].diff()
    day_boundaries = df["date_label"] != df["date_label"].shift(1)
    for col in ["load_ramp", "solar_ramp", "wind_ramp", "net_load_ramp"]:
        df.loc[day_boundaries, col] = None

    fig = go.Figure()
    cd = df[["date_label", "he"]].values

    outright_indices: list[int] = []
    ramp_indices: list[int] = []

    stack_traces = [
        ("Net Load", "net_load", "#60a5fa", "rgba(96, 165, 250, 0.50)"),
        ("Solar",    "solar",    "#fbbf24", "rgba(251, 191, 36, 0.40)"),
        ("Wind",     "wind",     "#34d399", "rgba(52, 211, 153, 0.35)"),
    ]
    for name, col, line_c, fill_c in stack_traces:
        outright_indices.append(len(fig.data))
        fig.add_trace(go.Scatter(
            x=df["datetime"], y=df[col], mode="lines", name=name,
            stackgroup="stack",
            line=dict(color=line_c, width=1), fillcolor=fill_c,
            customdata=cd,
            hovertemplate=f"<b>%{{customdata[0]}}</b> HE %{{customdata[1]}}<br>{name}: %{{y:,.0f}} MW<extra></extra>",
        ))

    outright_indices.append(len(fig.data))
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df["load"], mode="lines", name="Load",
        line=dict(color="#f8fafc", width=2),
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Load: %{y:,.0f} MW<extra></extra>",
    ))

    ramp_traces = [
        ("Load Ramp",     "load_ramp",     "#f8fafc"),
        ("Net Load Ramp", "net_load_ramp", "#60a5fa"),
        ("Solar Ramp",    "solar_ramp",    "#fbbf24"),
        ("Wind Ramp",     "wind_ramp",     "#34d399"),
    ]
    for name, col, color in ramp_traces:
        ramp_indices.append(len(fig.data))
        fig.add_trace(go.Bar(
            x=df["datetime"], y=df[col],
            name=name, marker_color=color, opacity=0.8,
            customdata=cd, visible=False,
            hovertemplate=f"<b>%{{customdata[0]}}</b> HE %{{customdata[1]}}<br>{name}: %{{y:+,.0f}} MW/hr<extra></extra>",
        ))

    fig.update_layout(
        title=f"Net Load Breakdown — {region_label}",
        height=500, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=60, b=60),
        hovermode="x unified", barmode="group", bargap=0.15,
    )
    fig.update_xaxes(tickformat="%a %b-%d %I %p", gridcolor="rgba(99,110,250,0.08)")
    fig.update_yaxes(title_text="MW", tickformat=".1s", gridcolor="rgba(99,110,250,0.1)")

    suffix = region_label.lower().replace(" ", "").replace("-", "")
    return render_ramp_toggle(
        fig, div_id=f"meteo-allnetload-{suffix}",
        outright_indices=outright_indices,
        ramp_indices=ramp_indices,
    )


# ══════════════════════════════════════════════════════════════════════
# Per-date helpers
# ══════════════════════════════════════════════════════════════════════


def _build_day_frame(
    load_df: pd.DataFrame, solar_df: pd.DataFrame, wind_df: pd.DataFrame,
    target_date,
) -> pd.DataFrame:
    load_s = day_series(load_df, target_date, "forecast_load_mw")
    solar_s = day_series(solar_df, target_date, "solar_forecast")
    wind_s = day_series(wind_df, target_date, "wind_forecast")
    out = pd.DataFrame({"hour_ending": list(range(1, 25))})
    out["load_mw"] = out["hour_ending"].map(load_s)
    out["solar_mw"] = out["hour_ending"].map(solar_s)
    out["wind_mw"] = out["hour_ending"].map(wind_s)
    renew = out[["solar_mw", "wind_mw"]].fillna(0).sum(axis=1)
    out["net_load_mw"] = out["load_mw"] - renew
    return out


def _render_snapshot_table(day_df: pd.DataFrame, dt_key: str) -> str:
    if day_df.empty:
        return empty_html("No rows.")

    tid = f"rs-tbl-{dt_key}"

    outright_rows = [
        _tbl_row("Load", "MW", day_df["load_mw"]),
        _tbl_row("Wind", "MW", day_df["wind_mw"]),
        _tbl_row("Solar", "MW", day_df["solar_mw"]),
    ]
    outright_net = [_tbl_row("Net Load", "MW", day_df["net_load_mw"])]
    ramp_rows = [
        _tbl_row("Load Ramp", "MW/hr", day_df["load_mw"].diff(), signed=True, sign_colors=True),
        _tbl_row("Wind Ramp", "MW/hr", day_df["wind_mw"].diff(), signed=True, sign_colors=True),
        _tbl_row("Solar Ramp", "MW/hr", day_df["solar_mw"].diff(), signed=True, sign_colors=True),
    ]
    ramp_net = [_tbl_row("Net Load Ramp", "MW/hr", day_df["net_load_mw"].diff(), signed=True, sign_colors=True)]
    outright_body = _tbl_body(outright_rows, outright_net)
    ramp_body = _tbl_body(ramp_rows, ramp_net)

    cols = ["Metric", "Unit"] + HE_COLS + SUMMARY_COLS
    header = '<thead><tr>'
    for col in cols:
        cls = ' class="metric"' if col == "Metric" else ' class="unit"' if col == "Unit" else ""
        header += f'<th{cls}>{col}</th>'
    header += '</tr></thead>'

    toggle_btn = (
        f'<div class="rs-toggle-bar">'
        f'<button class="rs-toggle" onclick="rsToggle(\'{tid}\')" id="{tid}-btn">SHOW RAMP</button>'
        f'</div>'
    )

    return (
        f'<div class="rs-wrap">{toggle_btn}'
        f'<div class="rs-tw"><table class="rs-t" id="{tid}">{header}'
        f'<tbody id="{tid}-outright">{outright_body}</tbody>'
        f'<tbody style="display:none;" id="{tid}-ramp">{ramp_body}</tbody>'
        f'</table></div></div>'
        f'''<script>
function rsToggle(tid) {{
  var o = document.getElementById(tid + '-outright');
  var r = document.getElementById(tid + '-ramp');
  var b = document.getElementById(tid + '-btn');
  if (o.style.display === 'none') {{
    o.style.display = ''; r.style.display = 'none'; b.textContent = 'SHOW RAMP';
  }} else {{
    o.style.display = 'none'; r.style.display = ''; b.textContent = 'SHOW OUTRIGHT';
  }}
}}
</script>'''
    )


def _tbl_row(metric: str, unit: str, values: pd.Series,
             signed: bool = False, sign_colors: bool = False) -> str:
    s = values.copy()
    s.index = range(1, len(s) + 1)
    html = f'<tr><td class="metric">{metric}</td><td class="unit">{unit}</td>'
    for h in range(1, 25):
        v = s.get(h, pd.NA)
        cls = cell_class(v, sign_colors)
        html += f'<td class="{cls}">{fmt_cell(v, signed)}</td>'
    for hours in [ONPEAK_HOURS, OFFPEAK_HOURS, list(range(1, 25))]:
        vals = pd.to_numeric(s.reindex(hours), errors="coerce").dropna()
        v = float(vals.mean()) if not vals.empty else pd.NA
        cls = cell_class(v, sign_colors)
        html += f'<td class="{cls}">{fmt_cell(v, signed)}</td>'
    html += '</tr>'
    return html


def _tbl_body(component_rows: list[str], net_rows: list[str]) -> str:
    divider = (
        f'<tr><td colspan="{2 + 24 + 3}" style="padding:0;height:3px;'
        f'background:linear-gradient(90deg,#4a6a8a,#4a6a8a60);border:none;"></td></tr>'
    )
    return "".join(component_rows) + divider + "".join(net_rows)


def _render_component_row(day_df: pd.DataFrame, dt_key: str, region_label: str) -> str:
    """Row 1 per date: three side-by-side subplots for Load / Solar / Wind.

    A single top-right SHOW RAMP button flips all three panels between
    outright (line+markers) and ramp (bars).
    """
    components = [
        ("load_mw",  "Load",  COLORS["load"]),
        ("solar_mw", "Solar", COLORS["solar"]),
        ("wind_mw",  "Wind",  COLORS["wind"]),
    ]

    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=tuple(f"{name} — {region_label}" for _, name, _ in components),
        horizontal_spacing=0.06,
    )

    hours = list(range(1, 25))
    outright_indices: list[int] = []
    ramp_indices: list[int] = []

    for col_idx, (col_name, label, color) in enumerate(components, start=1):
        series = day_df.set_index("hour_ending")[col_name].reindex(hours)
        ramp = series.diff()

        outright_indices.append(len(fig.data))
        fig.add_trace(go.Scatter(
            x=hours, y=series.values,
            mode="lines+markers", name=label,
            line=dict(color=color, width=2), marker=dict(size=4),
            showlegend=False,
            hovertemplate=f"HE %{{x}}<br>{label}: %{{y:,.0f}} MW<extra></extra>",
        ), row=1, col=col_idx)

        ramp_indices.append(len(fig.data))
        fig.add_trace(go.Bar(
            x=hours, y=ramp.values, name=f"{label} Ramp",
            marker_color=color, opacity=0.85,
            showlegend=False, visible=False,
            hovertemplate=f"HE %{{x}}<br>{label} Ramp: %{{y:+,.0f}} MW/hr<extra></extra>",
        ), row=1, col=col_idx)

    fig.update_layout(
        template=PLOTLY_TEMPLATE, height=400,
        margin=dict(l=50, r=20, t=80, b=40),
        hovermode="x unified",
    )
    for col in range(1, 4):
        fig.update_xaxes(
            dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True,
            title_text="Hour Ending", row=1, col=col,
        )
        fig.update_yaxes(title_text="MW", row=1, col=col)

    return render_ramp_toggle(
        fig, div_id=f"meteo-comp-{dt_key}",
        outright_indices=outright_indices,
        ramp_indices=ramp_indices,
        n_yaxes=3,
    )


def _render_net_load_row(day_df: pd.DataFrame, dt_key: str, region_label: str) -> str:
    """Row 2 per date: net load stacked area (left) + net load ramp bars (right)."""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(f"Net Load Breakdown — {region_label}", "Net Load Ramp"),
        horizontal_spacing=0.08,
    )
    hours = day_df["hour_ending"]
    net = day_df["net_load_mw"]
    solar = day_df["solar_mw"]
    wind = day_df["wind_mw"]
    load = day_df["load_mw"]
    ramp = net.diff()

    fig.add_trace(go.Scatter(
        x=hours, y=net, mode="lines", name="Net Load", stackgroup="stack",
        line=dict(color=COLORS["net_load"], width=1),
        fillcolor="rgba(96, 165, 250, 0.50)",
        hovertemplate="HE %{x}<br>Net Load: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=hours, y=wind, mode="lines", name="Wind", stackgroup="stack",
        line=dict(color=COLORS["wind"], width=1),
        fillcolor="rgba(52, 211, 153, 0.35)",
        hovertemplate="HE %{x}<br>Wind: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=hours, y=solar, mode="lines", name="Solar", stackgroup="stack",
        line=dict(color=COLORS["solar"], width=1),
        fillcolor="rgba(251, 191, 36, 0.40)",
        hovertemplate="HE %{x}<br>Solar: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=hours, y=load, mode="lines", name="Load",
        line=dict(color=COLORS["gross_load"], width=2),
        hovertemplate="HE %{x}<br>Load: %{y:,.0f} MW<extra></extra>",
    ), row=1, col=1)

    bar_colors = [
        COLORS["ramp_up"] if (pd.notna(v) and v >= 0) else COLORS["ramp_down"]
        for v in ramp
    ]
    fig.add_trace(go.Bar(
        x=hours, y=ramp, name="Net Load Ramp",
        marker_color=bar_colors, opacity=0.85,
        hovertemplate="HE %{x}<br>Ramp: %{y:+,.0f} MW/hr<extra></extra>",
    ), row=1, col=2)
    fig.add_hline(y=0, line_color="#7f8ea3", line_dash="dash", line_width=1, row=1, col=2)

    fig.update_layout(
        template=PLOTLY_TEMPLATE, height=470,
        margin=dict(l=60, r=40, t=80, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.08, x=0),
        hovermode="x unified", barmode="relative",
    )
    fig.update_xaxes(
        title_text="Hour Ending",
        dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True,
    )
    fig.update_yaxes(title_text="MW", col=1)
    fig.update_yaxes(title_text="MW/hr", col=2)

    return fig.to_html(
        include_plotlyjs="cdn", full_html=False,
        div_id=f"rs-netload-{dt_key}", config=PLOTLY_LOCKED_CONFIG,
    )
