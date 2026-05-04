"""Shared chart/CSS helpers for forecast bundles.

Used across every report bundle in `backend/reports/`. Hosts the line+ramp
chart, single-day chart, ramp toggle, snapshot table CSS, and shared
constants (regions, hour groupings, color palette, plotly config).
"""
from __future__ import annotations

import json
import re

import pandas as pd
import plotly.graph_objects as go

PLOTLY_TEMPLATE = "plotly_dark"
HE_COLS = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HOURS = list(range(8, 24))
OFFPEAK_HOURS = [h for h in range(1, 25) if h not in ONPEAK_HOURS]
SUMMARY_COLS = ["OnPeak", "OffPeak", "Flat"]

REGIONS = [
    ("RTO", "RTO"),
    ("WEST", "Western"),
    ("MIDATL", "Mid-Atlantic"),
    ("SOUTH", "Southern"),
]

# Colors are keyed on the quantity (load / solar / wind / net load),
# NOT on the data source. PJM and Meteologica sections use the same
# colour for the same quantity — they're differentiated by section title.
COLORS = {
    "load":        "#60a5fa",
    "net_load":    "#60a5fa",
    "solar":       "#fbbf24",
    "solar_btm":   "#fcd34d",
    "wind":        "#34d399",
    "gross_load":  "#f8fafc",
    "ramp_up":     "#34d399",
    "ramp_down":   "#f87171",
}

FILLS = {
    "load":     "rgba(96, 165, 250, 0.40)",
    "net_load": "rgba(96, 165, 250, 0.50)",
    "solar":    "rgba(251, 191, 36, 0.40)",
    "wind":     "rgba(52, 211, 153, 0.35)",
}

PLOTLY_LOCKED_CONFIG = {
    "displaylogo": False,
    "scrollZoom": False,
    "doubleClick": False,
    "modeBarButtonsToRemove": [
        "zoom2d", "pan2d", "select2d", "lasso2d",
        "zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d",
    ],
}


def prep_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize hour 0 → HE 24 and filter to the 1–24 range."""
    if df.empty:
        return df
    out = df.copy()
    out["hour_ending"] = out["hour_ending"].replace(0, 24)
    out = out[out["hour_ending"].between(1, 24)]
    return out


def day_series(df: pd.DataFrame, target_date, value_col: str) -> pd.Series:
    """Return a Series indexed by hour_ending (1–24) for one date."""
    if df.empty or value_col not in df.columns:
        return pd.Series(dtype=float)
    sub = df[df["date"] == target_date]
    if sub.empty:
        return pd.Series(dtype=float)
    sub = sub.drop_duplicates("hour_ending", keep="last")
    return sub.set_index("hour_ending")[value_col].sort_index()


def latest_line_with_ramp(
    df: pd.DataFrame,
    *,
    value_col: str,
    title: str,
    div_id: str,
    color: str = None,
) -> str:
    """Single-panel line chart with a Show Ramp toggle button.

    Outright trace is a line; the ramp trace is a hidden bar series rendered
    on the same axes. The button flips visibility between the two.
    """
    if df.empty or value_col not in df.columns:
        return empty_html(f"No data for {title}.")

    if color is None:
        color = COLORS["load"]

    df = df.copy().sort_values(["date", "hour_ending"])
    df["datetime"] = pd.to_datetime(df["date"]) + pd.to_timedelta(df["hour_ending"], unit="h")
    df["date_label"] = pd.to_datetime(df["date"]).dt.strftime("%a %b-%d")
    df[f"{value_col}_ramp"] = df[value_col].diff()
    day_boundaries = df["date_label"] != df["date_label"].shift(1)
    df.loc[day_boundaries, f"{value_col}_ramp"] = None

    cd = df[["date_label", "hour_ending"]].values

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["datetime"], y=df[value_col],
        mode="lines", name=title,
        line=dict(color=color, width=2),
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>" + title + ": %{y:,.0f} MW<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=df["datetime"], y=df[f"{value_col}_ramp"],
        name=f"{title} Ramp",
        marker_color=color, opacity=0.85,
        customdata=cd,
        hovertemplate="<b>%{customdata[0]}</b> HE %{customdata[1]}<br>Ramp: %{y:+,.0f} MW/hr<extra></extra>",
        visible=False,
    ))

    fig.update_layout(
        title=title,
        height=440, template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="top", y=-0.12, xanchor="left", x=0),
        margin=dict(l=60, r=40, t=60, b=60),
        hovermode="x unified",
    )
    fig.update_xaxes(tickformat="%a %b-%d %I %p", gridcolor="rgba(99,110,250,0.08)")
    fig.update_yaxes(title_text="MW", tickformat=".1s", gridcolor="rgba(99,110,250,0.1)")

    return render_ramp_toggle(
        fig, div_id=div_id,
        outright_indices=[0], ramp_indices=[1],
    )


def single_day_chart(
    values: pd.Series,
    *,
    title: str,
    div_id: str,
    color: str,
) -> str:
    """Single-panel HE1-24 line chart for one day with a ramp toggle.

    `values` is a numeric Series indexed by hour_ending (1-24).
    """
    if values is None or len(values) == 0 or values.dropna().empty:
        return empty_html(f"No data for {title}.")

    series = values.reindex(range(1, 25))
    ramp = series.diff()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=list(range(1, 25)), y=series.values,
        mode="lines+markers", name=title,
        line=dict(color=color, width=2), marker=dict(size=5),
        hovertemplate="HE %{x}<br>" + title + ": %{y:,.0f} MW<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=list(range(1, 25)), y=ramp.values,
        name=f"{title} Ramp",
        marker_color=color, opacity=0.85,
        visible=False,
        hovertemplate="HE %{x}<br>Ramp: %{y:+,.0f} MW/hr<extra></extra>",
    ))
    fig.update_layout(
        title=title,
        template=PLOTLY_TEMPLATE, height=380,
        margin=dict(l=60, r=40, t=60, b=60),
        legend=dict(orientation="h", yanchor="top", y=-0.12, x=0),
        hovermode="x unified",
    )
    fig.update_xaxes(
        title_text="Hour Ending",
        dtick=1, range=[0.5, 24.5], autorange=False, fixedrange=True,
    )
    fig.update_yaxes(title_text="MW", gridcolor="rgba(99,110,250,0.1)")

    return render_ramp_toggle(
        fig, div_id=div_id,
        outright_indices=[0], ramp_indices=[1],
    )


def render_ramp_toggle(
    fig: go.Figure,
    *,
    div_id: str,
    outright_indices: list[int],
    ramp_indices: list[int],
    y_title_outright: str = "MW",
    y_title_ramp: str = "MW/hr",
    n_yaxes: int = 1,
) -> str:
    """Wrap a figure with a Show Ramp / Show Outright toggle (top-right)."""
    ns = re.sub(r"[^A-Za-z0-9_]", "_", div_id)
    btn_id = f"{div_id}-btn"

    relayout_ramp: dict = {}
    relayout_out: dict = {}
    for i in range(1, n_yaxes + 1):
        suffix = "" if i == 1 else str(i)
        relayout_ramp[f"yaxis{suffix}.title.text"] = y_title_ramp
        relayout_ramp[f"yaxis{suffix}.autorange"] = True
        relayout_out[f"yaxis{suffix}.title.text"] = y_title_outright
        relayout_out[f"yaxis{suffix}.autorange"] = True

    chart_html = fig.to_html(
        include_plotlyjs="cdn", full_html=False,
        div_id=div_id, config=PLOTLY_LOCKED_CONFIG,
    )

    toggle_btn = (
        f'<div class="rs-toggle-bar">'
        f'<button class="rs-toggle" id="{btn_id}" '
        f'onclick="rsChartToggle_{ns}()">SHOW RAMP</button>'
        f'</div>'
    )

    script = f"""
<script>
(function() {{
  var cid = {json.dumps(div_id)};
  var btnId = {json.dumps(btn_id)};
  var out = {json.dumps(outright_indices)};
  var ramp = {json.dumps(ramp_indices)};
  var relayoutRamp = {json.dumps(relayout_ramp)};
  var relayoutOut = {json.dumps(relayout_out)};

  window.rsChartToggle_{ns} = function() {{
    var btn = document.getElementById(btnId);
    var toRamp = (btn.textContent === "SHOW RAMP");
    out.forEach(function(i) {{ Plotly.restyle(cid, {{visible: !toRamp}}, [i]); }});
    ramp.forEach(function(i) {{ Plotly.restyle(cid, {{visible: toRamp}}, [i]); }});
    Plotly.relayout(cid, toRamp ? relayoutRamp : relayoutOut);
    btn.textContent = toRamp ? "SHOW OUTRIGHT" : "SHOW RAMP";
  }};
}})();
</script>
"""
    return f'{_TOGGLE_STYLE}<div class="rs-wrap">{toggle_btn}{chart_html}</div>{script}'


def empty_html(msg: str) -> str:
    return f"<div style='padding:14px;color:#f87171;font-family:monospace;'>{msg}</div>"


def cell_class(val, sign_colors: bool) -> str:
    if not sign_colors or pd.isna(val):
        return ""
    return "pos" if val > 0 else "neg" if val < 0 else "zero"


def fmt_cell(val, signed: bool = False) -> str:
    if pd.isna(val):
        return "—"
    return f"{float(val):+,.0f}" if signed else f"{float(val):,.0f}"


def date_key(dt) -> str:
    return dt.isoformat() if hasattr(dt, "isoformat") and not isinstance(dt, str) else str(dt)


_TOGGLE_STYLE = """
<style>
.rs-wrap { position: relative; padding: 0; }
.rs-toggle-bar {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 6px;
  padding: 10px 12px 0 12px;
}
.rs-toggle {
  padding: 4px 12px; font-size: 11px; font-weight: 600;
  background: #1a2a42; color: #9eb4d3; border: 1px solid #2a3f60;
  border-radius: 4px; cursor: pointer; font-family: inherit;
  text-transform: uppercase; letter-spacing: 0.5px;
  white-space: nowrap; flex-shrink: 0;
}
.rs-toggle:hover { background: #1e3556; color: #dbe7ff; }
</style>
"""


SNAPSHOT_STYLE = """
<style>
.rs-wrap { padding: 8px; }
.rs-tw { overflow-x: auto; border: 1px solid #2a3f60; border-radius: 8px; }
.rs-t {
  width: 100%; border-collapse: collapse;
  font-size: 11px; font-family: monospace;
}
.rs-t th {
  position: sticky; top: 0; background: #16263d; color: #e6efff;
  border-bottom: 1px solid #2a3f60; padding: 6px 8px;
  text-align: right; white-space: nowrap;
}
.rs-t th.metric, .rs-t th.unit { text-align: left; }
.rs-t td {
  padding: 5px 8px; border-bottom: 1px solid #1f334f;
  text-align: right; color: #dbe7ff; white-space: nowrap;
}
.rs-t td.metric { text-align: left; color: #cfe0ff; font-weight: 700; }
.rs-t td.unit { text-align: left; color: #8aa5ca; }
.rs-t tr:nth-child(even) td { background: rgba(18, 32, 50, 0.45); }
.rs-t td.pos { color: #34d399; }
.rs-t td.neg { color: #f87171; }
.rs-t td.zero { color: #9eb4d3; }
.rs-toggle-bar {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 6px;
  padding: 10px 12px 0 12px;
}
.rs-toggle {
  padding: 4px 12px; font-size: 11px; font-weight: 600;
  background: #1a2a42; color: #9eb4d3; border: 1px solid #2a3f60;
  border-radius: 4px; cursor: pointer; font-family: inherit;
  text-transform: uppercase; letter-spacing: 0.5px;
  white-space: nowrap; flex-shrink: 0;
}
.rs-toggle:hover { background: #1e3556; color: #dbe7ff; }
</style>
"""
