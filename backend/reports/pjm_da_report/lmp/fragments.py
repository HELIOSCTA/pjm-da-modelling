"""DA LMP fragments — Summary block at top, then one block per hub.

Summary block: 3-panel chart with shared legend (click toggles all panels);
per-hub line color = the hub's mean-value rank along a green→red diverging
palette (green = high, red = low). Below: a heatmap-tinted summary table.

Per-hub block: one chart with 3 component traces + a pivoted table whose
24 HE cells per row are tinted by the same diverging gradient.

All tables: white background, column-group dividers between Description /
Summary / HE columns, peak-hour overlay (HE8-23) with dashed boundaries.
"""
from __future__ import annotations

import re
from datetime import date

import pandas as pd

from backend.reports._forecast_utils import (
    OFFPEAK_HOURS,
    ONPEAK_HOURS,
    empty_html,
)
from backend.reports.pjm_da_report.lmp import charts
from backend.reports.pjm_da_report.lmp.configs import (
    CONGESTION_COLOR,
    HUB_COLORS,
    REPORT_HUBS,
    SYSTEM_COLOR,
    TOTAL_COLOR,
)
from backend.reports.pjm_da_report.lmp.loader import load_da_lmps

Fragment = tuple[str, str, str | None]

_FLAT_HOURS = list(range(1, 25))

# (label, parquet column) in display order — Total, then Congestion (the
# trader-watched component), then System (uniform across hubs).
_COMPONENTS: tuple[tuple[str, str], ...] = (
    ("Total LMP",      "lmp_total"),
    ("Congestion LMP", "lmp_congestion_price"),
    ("System LMP",     "lmp_system_energy_price"),
)

# Hubs rendered per component. System price is uniform across hubs at any
# hour, so we only show Western — saves 7 redundant rows in the Summary.
_COMPONENT_HUBS: dict[str, list[str]] = {
    "lmp_total":               list(REPORT_HUBS),
    "lmp_congestion_price":    list(REPORT_HUBS),
    "lmp_system_energy_price": [REPORT_HUBS[0]],
}

# Total cols in the Summary table: Date, Hub, OnPeak, OffPeak, Flat, HE1..HE24.
_SUMMARY_COL_COUNT = 5 + 24

# White-background table style — quiet, trader-grade chrome:
# - tabular numerals, small uppercase headers, light grid
# - muted red/green heatmap (no yellow) via inline rgba on body cells
# - peak hours marked only on the header (no body overlay) so the heatmap
#   is the dominant visual signal
_TABLE_STYLE = """
<style>
.rs-wrap { padding: 8px 0; }
.rs-tw {
  overflow-x: auto;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  background: #ffffff;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
}
.rs-t {
  width: 100%; border-collapse: collapse;
  font-size: 11px;
  font-family: "SF Mono", Consolas, "Liberation Mono", Menlo, monospace;
  font-variant-numeric: tabular-nums;
  background: #ffffff;
  color: #0f172a;
}
.rs-t th {
  position: sticky; top: 0; z-index: 1;
  background: #f8fafc; color: #64748b;
  border-bottom: 1px solid #cbd5e1;
  padding: 7px 10px;
  text-align: right; white-space: nowrap;
  font-weight: 600; font-size: 10px;
  text-transform: uppercase; letter-spacing: 0.4px;
}
.rs-t th.metric { text-align: left; }
.rs-t td {
  padding: 5px 10px;
  border-bottom: 1px solid #f1f5f9;
  text-align: right; color: #0f172a;
  white-space: nowrap;
  background: #ffffff;
}
.rs-t td.metric {
  text-align: left;
  color: #334155;
  font-weight: 600;
  font-size: 10px;
  letter-spacing: 0.2px;
}

/* Component-section divider rows. */
.rs-t tr td.section-divider {
  background: #f1f5f9; color: #0f172a; font-weight: 700;
  padding: 8px 10px;
  border-top: 1px solid #cbd5e1; border-bottom: 1px solid #e2e8f0;
  text-align: left; font-size: 10px;
  text-transform: uppercase; letter-spacing: 0.6px;
}

/* Group dividers (between Description / Summary / HE col groups). */
.rs-t th.col-group-start, .rs-t td.col-group-start {
  border-left: 1px solid #cbd5e1;
}

/* Aggregate columns (OnPeak / OffPeak / Flat): not heatmapped, set apart by
   a subtle slate background and bold to read as summary stats. */
.rs-t th.summary-stat { background: #f1f5f9; color: #475569; }
.rs-t td.summary-stat {
  background: #f8fafc;
  font-weight: 600;
  color: #0f172a;
}

/* Peak HE columns (HE8-HE23): header-only accent — keeps body cells clean. */
.rs-t th.peak { background: #eef2ff; color: #3730a3; }
.rs-t th.peak-start { border-left: 1px solid #cbd5e1; }
.rs-t th.peak-end   { border-right: 1px solid #cbd5e1; }
.rs-t td.peak-start { border-left: 1px dotted #e2e8f0; }
.rs-t td.peak-end   { border-right: 1px dotted #e2e8f0; }
</style>
"""


def build_fragments(target_date: date) -> list[str | Fragment]:
    df = load_da_lmps(target_date=target_date, lookback_days=1)

    items: list[str | Fragment] = []
    items.append(f"DA LMPs — {target_date}")
    items.append(("Summary", _build_summary_block(df, target_date=target_date), None))

    for hub in REPORT_HUBS:
        items.append((hub, _build_hub_block(df, target_date=target_date, hub=hub), None))

    return items


# ── Summary block ──────────────────────────────────────────────────

def _build_summary_block(df: pd.DataFrame, *, target_date: date) -> str:
    chart = charts.summary_panels_chart(
        df, target_date=target_date,
        hubs=list(REPORT_HUBS),
        hub_colors=HUB_COLORS,
        components=[(col, label) for label, col in _COMPONENTS],
        panel_hubs=_COMPONENT_HUBS,
        div_id="summary-panels",
    )

    table = _build_summary_table(df, target_date=target_date)

    # Inject _TABLE_STYLE once, here, since Summary renders before any hub block.
    return (
        f"{_TABLE_STYLE}"
        f"<div style='margin-bottom:16px;'>{chart}</div>"
        f"{table}"
    )


def _build_summary_table(df: pd.DataFrame, *, target_date: date) -> str:
    """Component-dividered table; cells diverge red→green by value rank."""
    sub_target = df[df["date"] == target_date]
    rows: list[str] = []

    for label, col in _COMPONENTS:
        section_hubs = _COMPONENT_HUBS[col]
        sub_section = sub_target[sub_target["hub"].isin(section_hubs)]
        vmin, vmax = _component_range(sub_section, col)

        rows.append(
            f'<tr><td class="section-divider" colspan="{_SUMMARY_COL_COUNT}">'
            f'{label}</td></tr>'
        )
        for hub in section_hubs:
            rows.append(_summary_row(
                df, target_date=target_date, hub=hub, col=col,
                vmin=vmin, vmax=vmax,
            ))

    headers = (
        '<th class="metric">Date</th>'
        '<th class="metric">Hub</th>'
        '<th class="col-group-start summary-stat">OnPeak</th>'
        '<th class="summary-stat">OffPeak</th>'
        '<th class="summary-stat">Flat</th>'
        + "".join(f"<th class='{_he_class(h)}'>HE{h}</th>" for h in _FLAT_HOURS)
    )

    return (
        '<div class="rs-wrap"><div class="rs-tw">'
        '<table class="rs-t">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody>'
        '</table></div></div>'
    )


def _summary_row(
    df: pd.DataFrame,
    *,
    target_date: date,
    hub: str,
    col: str,
    vmin: float,
    vmax: float,
) -> str:
    sub = df[(df["date"] == target_date) & (df["hub"] == hub)]
    if sub.empty:
        return (
            f"<tr><td class='metric'>{target_date}</td>"
            f"<td class='metric'>{hub}</td>"
            + "<td>—</td>" * (_SUMMARY_COL_COUNT - 2)
            + "</tr>"
        )

    by_he = sub.drop_duplicates("hour_ending", keep="last").set_index("hour_ending")

    cells = [
        f"<td class='metric'>{target_date}</td>",
        f"<td class='metric'>{hub}</td>",
    ]
    summary_classes = ("col-group-start summary-stat", "summary-stat", "summary-stat")
    for cls, hours in zip(summary_classes, (ONPEAK_HOURS, OFFPEAK_HOURS, _FLAT_HOURS)):
        v = sub[sub["hour_ending"].isin(hours)][col].mean()
        cells.append(_plain_cell(v, extra_class=cls))
    for he in _FLAT_HOURS:
        v = by_he[col].get(he) if col in by_he.columns else None
        cells.append(_diverging_cell(v, vmin, vmax, extra_class=_he_class(he)))

    return f"<tr>{''.join(cells)}</tr>"


# ── Per-hub block ──────────────────────────────────────────────────

def _build_hub_block(df: pd.DataFrame, *, target_date: date, hub: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", hub.lower()).strip("-")

    chart = charts.he_components_chart(
        df, target_date=target_date, hub=hub,
        title=f"{hub} — DA LMP Components",
        div_id=f"{slug}-components",
        color_total=TOTAL_COLOR,
        color_system=SYSTEM_COLOR,
        color_cong=CONGESTION_COLOR,
    )

    chart_block = f'<div style="margin-bottom:16px;">{chart}</div>'
    table = _build_hub_table(df, target_date=target_date, hub=hub)
    return f"{chart_block}{table}"


def _build_hub_table(df: pd.DataFrame, *, target_date: date, hub: str) -> str:
    """Pivoted: one row per component (Total / System / Congestion).

    Each row's HE/summary cells diverge red→green within its own range, so
    you see the shape of the day for that single hub × component.
    """
    sub = df[(df["date"] == target_date) & (df["hub"] == hub)]
    if sub.empty:
        return empty_html(f"No data for {hub} on {target_date}.")

    by_he = sub.drop_duplicates("hour_ending", keep="last").set_index("hour_ending")

    body_rows = []
    for label, col in _COMPONENTS:
        if col in by_he.columns:
            row_vals = by_he[col].dropna()
            if not row_vals.empty:
                vmin, vmax = float(row_vals.min()), float(row_vals.max())
            else:
                vmin = vmax = 0.0
        else:
            vmin = vmax = 0.0

        cells = [
            f"<td class='metric'>{target_date}</td>",
            f"<td class='metric'>{hub}</td>",
            f"<td class='metric'>{label}</td>",
        ]
        summary_classes = ("col-group-start", "", "")
        for cls, hours in zip(summary_classes, (ONPEAK_HOURS, OFFPEAK_HOURS, _FLAT_HOURS)):
            v = sub[sub["hour_ending"].isin(hours)][col].mean()
            cells.append(_diverging_cell(v, vmin, vmax, extra_class=cls))
        for he in _FLAT_HOURS:
            v = by_he[col].get(he) if col in by_he.columns else None
            cells.append(_diverging_cell(v, vmin, vmax, extra_class=_he_class(he)))
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    headers = (
        '<th class="metric">Date</th>'
        '<th class="metric">Hub</th>'
        '<th class="metric">Component</th>'
        '<th class="col-group-start summary-stat">OnPeak</th>'
        '<th class="summary-stat">OffPeak</th>'
        '<th class="summary-stat">Flat</th>'
        + "".join(f"<th class='{_he_class(h)}'>HE{h}</th>" for h in _FLAT_HOURS)
    )

    return (
        '<div class="rs-wrap"><div class="rs-tw">'
        '<table class="rs-t">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table></div></div>'
    )


# ── Helpers ────────────────────────────────────────────────────────

def _he_class(he: int) -> str:
    """Classes for an HE column header / body cell.

    HE1 starts the HE group. HE8-23 are peak; HE8 left-edge, HE23 right-edge.
    """
    classes: list[str] = []
    if he == 1:
        classes.append("col-group-start")
    if 8 <= he <= 23:
        classes.append("peak")
    if he == 8:
        classes.append("peak-start")
    if he == 23:
        classes.append("peak-end")
    return " ".join(classes)


def _component_range(sub_target: pd.DataFrame, col: str) -> tuple[float, float]:
    """Min/max for one component across all hubs and HEs on the target date."""
    if col not in sub_target.columns or sub_target[col].dropna().empty:
        return (0.0, 0.0)
    return (float(sub_target[col].min()), float(sub_target[col].max()))


_HEAT_GREEN = (22, 163, 74)   # Tailwind green-600
_HEAT_RED   = (220, 38, 38)   # Tailwind red-600
_HEAT_MAX_ALPHA = 0.40        # cap so even extreme cells stay readable


def _plain_cell(val, *, extra_class: str = "") -> str:
    """<td> with no heatmap — used for OnPeak/OffPeak/Flat aggregate columns."""
    cls = f" class='{extra_class}'" if extra_class else ""
    return f"<td{cls}>{_fmt(val)}</td>"


def _diverging_cell(
    val,
    vmin: float,
    vmax: float,
    *,
    extra_class: str = "",
) -> str:
    """<td> with a muted red/white/green tint diverging from the median.

    Mid of the range = white; below = red, above = green. Alpha scales with
    distance from the midpoint so the table stays calm but the outliers pop.
    Text stays dark so values are legible at any tint.
    """
    cls = f" class='{extra_class}'" if extra_class else ""
    if val is None or pd.isna(val):
        return f"<td{cls}>—</td>"
    rng = vmax - vmin
    pos = (float(val) - vmin) / rng if rng > 0 else 0.5
    pos = max(0.0, min(1.0, pos))
    dev = (pos - 0.5) * 2.0  # -1 .. +1
    if dev > 0:
        r, g, b = _HEAT_GREEN
        alpha = dev * _HEAT_MAX_ALPHA
    elif dev < 0:
        r, g, b = _HEAT_RED
        alpha = -dev * _HEAT_MAX_ALPHA
    else:
        return f"<td{cls}>{_fmt(val)}</td>"
    return f"<td{cls} style='background:rgba({r},{g},{b},{alpha:.2f});'>{_fmt(val)}</td>"


def _fmt(val) -> str:
    if val is None or pd.isna(val):
        return "—"
    return f"{float(val):.2f}"
