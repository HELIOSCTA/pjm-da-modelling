"""Forecast diff fragments — one section per (forecast_area, forecast_date).

Each section pairs:
    - a snapshot table (rows = run vintage, cols = HE0..HE23 + Peak + OnPeak)
      with the LATEST run plus the closest runs ~12h / 24h / 48h before
      it highlighted, plus DELTA rows (latest minus each anchor)
    - a comparison line plot (one trace per anchor + LATEST).

Run vintages are picked by *evaluated_at_ept*: the latest run is the most
recent vintage for that (area, forecast_date); the 12h / 24h / 48h
anchors are the runs whose evaluated_at_ept is closest to (latest - lag)
(ties broken by older).
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.graph_objects as go

from backend.reports._forecast_utils import (
    PLOTLY_LOCKED_CONFIG,
    PLOTLY_TEMPLATE,
    deferred_plotly_html,
    empty_html,
)
from backend.reports.forecast_differences.loader import load_combined_window

Fragment = tuple[str, str, str | None]

DEFAULT_AREAS = (
    "RTO_COMBINED",
    "MID_ATLANTIC_REGION",
    "WESTERN_REGION",
    "SOUTHERN_REGION",
)

# (source, area) ordering that drives the inner-nav sequence. PJM and
# METEO sit back-to-back for each area so traders can compare the two
# curves directly.
SOURCE_AREA_ORDER: tuple[tuple[str, str], ...] = (
    ("PJM",   "RTO_COMBINED"),
    ("METEO", "RTO_COMBINED"),
    ("PJM",   "MID_ATLANTIC_REGION"),
    ("METEO", "MID_ATLANTIC_REGION"),
    ("PJM",   "WESTERN_REGION"),
    ("METEO", "WESTERN_REGION"),
    ("PJM",   "SOUTHERN_REGION"),
    ("METEO", "SOUTHERN_REGION"),
)

_HE_COLS = list(range(24))  # 0..23 (forecast hour beginning, EPT-local)
_ONPEAK_HE_STARTS = list(range(7, 23))  # HE 8..23 -> hours starting 7..22

_LAGS = [
    ("Latest -48h", timedelta(hours=48), "#f87171"),
    ("Latest -24h", timedelta(hours=24), "#fbbf24"),
    ("Latest -12h", timedelta(hours=12), "#34d399"),
]
_LATEST_COLOR = "#60a5fa"


_TABLE_STYLE = """
<style>
.fd-wrap { padding: 8px 0 16px 0; }
.fd-tw {
  overflow-x: auto;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  background: #ffffff;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
}
.fd-t {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
  font-family: "SF Mono", Consolas, "Liberation Mono", Menlo, monospace;
  font-variant-numeric: tabular-nums;
  color: #0f172a;
  background: #ffffff;
}
.fd-t th {
  position: sticky; top: 0; z-index: 1;
  background: #f8fafc; color: #475569;
  border-bottom: 1px solid #cbd5e1;
  padding: 6px 10px;
  text-align: right; white-space: nowrap;
  font-weight: 600; font-size: 10px;
  text-transform: uppercase; letter-spacing: 0.4px;
}
.fd-t th.metric { text-align: left; }
.fd-t td {
  padding: 5px 10px;
  border-bottom: 1px solid #f1f5f9;
  text-align: right;
  white-space: nowrap;
  background: #ffffff;
  color: #0f172a;
}
.fd-t td.metric {
  text-align: left;
  color: #334155;
  font-weight: 600;
}
.fd-t tr.latest-row td.metric {
  color: #0f172a;
  font-weight: 700;
}
.fd-t tr.delta-row td {
  border-top: 1px solid #cbd5e1;
  font-weight: 600;
}
.fd-t tr.delta-row td.metric {
  color: #334155;
}
.fd-t th.summary-stat, .fd-t td.summary-stat {
  border-left: 1px solid #cbd5e1;
  font-weight: 600;
}
.fd-t th.col-group-start, .fd-t td.col-group-start {
  border-left: 1px solid #cbd5e1;
}
.fd-t th.peak { background: #eef2ff; color: #3730a3; }
</style>
"""

# Tailwind-ish red-500 -> green-600 endpoints. Alpha is capped so the
# heatmap stays legible against white.
_HEAT_RED = (220, 38, 38)
_HEAT_GREEN = (22, 163, 74)
_HEAT_MAX_ALPHA = 0.42


def build_fragments(
    forecast_areas: list[str] | None = None,
    eval_lookback_days: int = 2,
) -> list[str | Fragment]:
    areas = list(forecast_areas or DEFAULT_AREAS)
    df = load_combined_window(
        forecast_areas=areas,
        eval_lookback_days=eval_lookback_days,
    )

    items: list[str | Fragment] = []

    if df.empty:
        items.append("Forecast Diffs")
        items.append(("(no data)", empty_html("No rows returned for the window."), None))
        return items

    requested_pairs = [
        (src, area) for (src, area) in SOURCE_AREA_ORDER if area in areas
    ]

    # Outer nav = forecast_date (DDD MMM-DD), starting from today.
    # Inner nav = (forecast_area, source) within the day, e.g.
    # "RTO Combined - PJM", "RTO Combined - METEO", "Mid Atlantic - PJM".
    today = date.today()
    forecast_dates = sorted(d for d in df["forecast_date"].unique() if d >= today)

    # First sidebar group: a one-page SUMMARY across all forecast dates
    # and (source, area) pairs (latest run per pair × day). Hoist the
    # CSS once onto the first section so it isn't duplicated across
    # ~100 blocks.
    items.append("Summary")
    items.append((
        "Peak & OnPeak",
        f"{_TABLE_STYLE}{_REGION_BORDER_STYLE}"
        + _build_summary_block(df, dates=forecast_dates, pairs=requested_pairs),
        None,
    ))

    # Areas in inner-nav order (each area's PJM and METEO sections are
    # paired, followed by a PJM-vs-METEO compare section).
    seen_areas: list[str] = []
    for (_src, area) in requested_pairs:
        if area not in seen_areas:
            seen_areas.append(area)

    for fdate in forecast_dates:
        sub_day_all = df[df["forecast_date"] == fdate]
        if sub_day_all.empty:
            continue
        items.append(_format_date_label(fdate))
        for area in seen_areas:
            for source in ("PJM", "METEO"):
                if (source, area) not in requested_pairs:
                    continue
                sub_day = sub_day_all[
                    (sub_day_all["forecast_area"] == area)
                    & (sub_day_all["source"] == source)
                ]
                if sub_day.empty:
                    continue
                block = _build_day_block(
                    sub_day, area=area, forecast_date=fdate, source=source,
                )
                if block is None:
                    continue
                items.append((_section_label(area=area, source=source), block, None))

            # PJM vs METEO compare — both sources' latest run for this area.
            sub_area = sub_day_all[sub_day_all["forecast_area"] == area]
            sources_present = set(sub_area["source"].unique())
            if {"PJM", "METEO"}.issubset(sources_present):
                compare_block = _build_compare_block(
                    sub_area, area=area, forecast_date=fdate,
                )
                if compare_block is not None:
                    items.append((
                        f"{_pretty_area(area)} - PJM vs METEO",
                        compare_block,
                        None,
                    ))

    return items


# ── Summary page ──────────────────────────────────────────────────


_METRICS: tuple[tuple[str, str], ...] = (
    ("peak",   "Peak"),
    ("onpeak", "OnPeak"),
)


def _build_summary_block(
    df: pd.DataFrame,
    *,
    dates: list[date],
    pairs: list[tuple[str, str]],
) -> str:
    """One table — column hierarchy is (source-region) > (Peak, OnPeak),
    rows are forecast dates. Each cell is the LATEST run's value for
    that (date, source, area, metric). Per-column gradient so seasonal
    / daily shape jumps out per series.
    """
    if not dates or not pairs:
        return empty_html("Summary unavailable — no overlapping data.")

    grids: dict[str, pd.DataFrame] = {
        key: _summary_grid(df, dates=dates, pairs=pairs, metric=key)
        for (key, _label) in _METRICS
    }
    return _summary_table_html(grids, dates=dates, pairs=pairs)


def _summary_grid(
    df: pd.DataFrame,
    *,
    dates: list[date],
    pairs: list[tuple[str, str]],
    metric: str,
) -> pd.DataFrame:
    """Return a DataFrame indexed by forecast_date with one column per
    (source, area) holding the LATEST run's Peak or OnPeak value.
    """
    col_keys = [_pair_key(source, area) for (source, area) in pairs]
    out = pd.DataFrame(index=dates, columns=col_keys, dtype=float)
    for (source, area), key in zip(pairs, col_keys):
        sub = df[(df["source"] == source) & (df["forecast_area"] == area)]
        if sub.empty:
            continue
        for fdate in dates:
            sub_day = sub[sub["forecast_date"] == fdate]
            if sub_day.empty:
                continue
            latest_ts = sub_day["evaluated_at_ept"].max()
            latest = sub_day[sub_day["evaluated_at_ept"] == latest_ts]
            by_he = (
                latest.drop_duplicates("he_start", keep="last")
                .set_index("he_start")["forecast_load_mw"]
            )
            if metric == "peak":
                val = by_he.max(skipna=True)
            else:
                val = by_he.reindex(_ONPEAK_HE_STARTS).mean(skipna=True)
            if pd.notna(val):
                out.at[fdate, key] = float(val)
    return out


def _pair_key(source: str, area: str) -> str:
    return f"{source}|{area}"


_REGION_BORDER_STYLE = """
<style>
.fd-t tr.region-start td { border-top: 2px solid #475569; }
.fd-t tr.region-start td.metric { background: #f1f5f9; }
.fd-t td.region-label {
  background: #f8fafc;
  border-right: 1px solid #cbd5e1;
  font-weight: 700;
  color: #0f172a;
  letter-spacing: 0.3px;
}
.fd-t td.diff-marker {
  background: #f1f5f9;
  font-style: italic;
}
</style>
"""


def _summary_table_html(
    grids: dict[str, pd.DataFrame],
    *,
    dates: list[date],
    pairs: list[tuple[str, str]],
) -> str:
    """One transposed table grouped by region.

    Columns: Region | Source | Metric | <date1> | <date2> | ...
    Within each region group (rowspan covers the whole group):
        PJM    Peak / OnPeak     — per-row green-max / red-min gradient
        METEO  Peak / OnPeak     — per-row green-max / red-min gradient
        Δ      Peak / OnPeak     — symmetric green/red gradient, 0-centered
                                    (only present when both sources exist)
    A thick top border opens each region group.
    """
    headers = (
        '<th class="metric">Region</th>'
        '<th class="metric col-group-start">Source</th>'
        '<th class="metric col-group-start">Metric</th>'
        + "".join(
            f"<th class='col-group-start' style='text-align:right;'>"
            f"{_format_date_label(fdate)}</th>"
            for fdate in dates
        )
    )

    # Group pairs by area in inner-nav order so each region renders as a
    # contiguous block.
    areas_in_order: list[str] = []
    by_area: dict[str, list[str]] = {}
    for (source, area) in pairs:
        by_area.setdefault(area, []).append(source)
        if area not in areas_in_order:
            areas_in_order.append(area)

    body_rows: list[str] = []
    for area in areas_in_order:
        sources = by_area[area]
        has_diff = "PJM" in sources and "METEO" in sources
        region_rowspan = len(sources) * len(_METRICS) + (len(_METRICS) if has_diff else 0)
        region_label = _pretty_area(area)
        first_row_in_region = True

        for source in sources:
            key = _pair_key(source, area)
            for j, (mkey, mlabel) in enumerate(_METRICS):
                row_vals = grids[mkey].loc[dates, key].astype(float)
                vmin, vmax = _row_range(row_vals)

                row_cls = "region-start" if first_row_in_region else ""
                cells: list[str] = []
                if first_row_in_region:
                    cells.append(
                        f"<td class='region-label' rowspan='{region_rowspan}' "
                        f"style='vertical-align:top;'>{region_label}</td>"
                    )
                if j == 0:
                    cells.append(
                        f"<td class='metric col-group-start' rowspan='{len(_METRICS)}' "
                        f"style='vertical-align:top;'>{source}</td>"
                    )
                cells.append(f"<td class='metric col-group-start'>{mlabel}</td>")
                cells.extend(
                    _heat_cell(row_vals.get(fdate), vmin, vmax, extra="col-group-start")
                    for fdate in dates
                )
                body_rows.append(
                    f"<tr class='{row_cls}'>{''.join(cells)}</tr>"
                )
                first_row_in_region = False

        # Δ rows for regions that have both PJM and METEO.
        if has_diff:
            pjm_key = _pair_key("PJM", area)
            meteo_key = _pair_key("METEO", area)
            for j, (mkey, mlabel) in enumerate(_METRICS):
                pjm_row = grids[mkey].loc[dates, pjm_key].astype(float)
                meteo_row = grids[mkey].loc[dates, meteo_key].astype(float)
                diff_row = meteo_row - pjm_row
                bound = (
                    float(diff_row.dropna().abs().max())
                    if diff_row.notna().any() else 0.0
                )

                cells = []
                if j == 0:
                    cells.append(
                        f"<td class='metric col-group-start diff-marker' "
                        f"rowspan='{len(_METRICS)}' style='vertical-align:top;'>"
                        f"Δ METEO − PJM</td>"
                    )
                cells.append(
                    f"<td class='metric col-group-start diff-marker'>{mlabel}</td>"
                )
                cells.extend(
                    _delta_heat_cell(diff_row.get(fdate), bound, extra="col-group-start")
                    for fdate in dates
                )
                body_rows.append(f"<tr>{''.join(cells)}</tr>")

    return (
        '<div class="fd-wrap"><div class="fd-tw">'
        '<table class="fd-t">'
        f'<thead><tr>{headers}</tr></thead>'
        f"<tbody>{''.join(body_rows)}</tbody>"
        '</table></div></div>'
    )


def _row_range(values: pd.Series) -> tuple[float, float]:
    clean = values.dropna()
    if clean.empty:
        return (0.0, 0.0)
    return (float(clean.min()), float(clean.max()))


# ── Per-day PJM vs METEO compare ─────────────────────────────────


_PJM_COLOR = "#2563eb"   # blue-600
_METEO_COLOR = "#16a34a"  # green-600


def _build_compare_block(
    sub_area: pd.DataFrame,
    *,
    area: str,
    forecast_date: date,
) -> str | None:
    """Latest-run PJM vs METEO for one (forecast_date, area):
    overlay plot + 3-row table (PJM, METEO, Δ METEO-PJM).
    """
    latest_rows: dict[str, tuple[pd.Timestamp, pd.Series]] = {}
    for source in ("PJM", "METEO"):
        sub_src = sub_area[sub_area["source"] == source]
        if sub_src.empty:
            continue
        latest_ts = sub_src["evaluated_at_ept"].max()
        latest = sub_src[sub_src["evaluated_at_ept"] == latest_ts]
        by_he = (
            latest.drop_duplicates("he_start", keep="last")
            .set_index("he_start")["forecast_load_mw"]
            .reindex(_HE_COLS)
        )
        latest_rows[source] = (latest_ts, by_he)

    if "PJM" not in latest_rows or "METEO" not in latest_rows:
        return None

    chart_html = _compare_chart(
        latest_rows, area=area, forecast_date=forecast_date,
    )
    table_html = _compare_table(latest_rows)
    return f"<div style='margin-bottom:14px;'>{chart_html}</div>{table_html}"


def _compare_chart(
    latest_rows: dict[str, tuple[pd.Timestamp, pd.Series]],
    *,
    area: str,
    forecast_date: date,
) -> str:
    pjm_ts, pjm_y = latest_rows["PJM"]
    meteo_ts, meteo_y = latest_rows["METEO"]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=_HE_COLS, y=pjm_y.values,
        mode="lines+markers",
        name=f"PJM ({_fmt_ts(pjm_ts)})",
        line=dict(color=_PJM_COLOR, width=2.4),
        marker=dict(size=5),
        hovertemplate="HE %{x}<br>PJM: %{y:,.0f} MW<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=_HE_COLS, y=meteo_y.values,
        mode="lines+markers",
        name=f"METEO ({_fmt_ts(meteo_ts)})",
        line=dict(color=_METEO_COLOR, width=2.4),
        marker=dict(size=5),
        hovertemplate="HE %{x}<br>METEO: %{y:,.0f} MW<extra></extra>",
    ))

    fig.update_layout(
        title=f"{_pretty_area(area)} — PJM vs METEO (latest, {forecast_date})",
        template=PLOTLY_TEMPLATE,
        height=420,
        margin=dict(l=60, r=40, t=60, b=60),
        legend=dict(orientation="h", yanchor="top", y=-0.14, x=0),
        hovermode="x unified",
    )
    fig.update_xaxes(
        title_text="Hour Beginning (EPT)",
        dtick=1, range=[-0.5, 23.5],
        autorange=False, fixedrange=True,
        gridcolor="rgba(99,110,250,0.08)",
    )
    fig.update_yaxes(
        title_text="MW", tickformat=",",
        gridcolor="rgba(99,110,250,0.1)",
    )

    div_id = _safe_div_id(f"fd-compare-{area}-{forecast_date}")
    return deferred_plotly_html(
        fig, div_id=div_id, height=420, config=PLOTLY_LOCKED_CONFIG,
    )


def _compare_table(
    latest_rows: dict[str, tuple[pd.Timestamp, pd.Series]],
) -> str:
    """3 rows: PJM latest, METEO latest, Δ (METEO - PJM)."""
    pjm_ts, pjm_y = latest_rows["PJM"]
    meteo_ts, meteo_y = latest_rows["METEO"]
    delta = meteo_y - pjm_y

    headers = (
        '<th class="metric">Run (EPT)</th>'
        '<th class="metric">Source</th>'
        '<th class="col-group-start summary-stat">Peak</th>'
        '<th class="summary-stat">OnPeak</th>'
        + "".join(
            f"<th class='{_he_class(h)}'>HE{h}</th>" for h in _HE_COLS
        )
    )

    body_rows = [
        _value_row(pjm_ts, "PJM", pjm_y, css="lag-row"),
        _value_row(meteo_ts, "METEO", meteo_y, css="latest-row"),
        _delta_row(label="Δ METEO − PJM", anchor_ts=meteo_ts, diff=delta),
    ]

    return (
        '<div class="fd-wrap"><div class="fd-tw">'
        '<table class="fd-t">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table></div></div>'
    )


# ── Per-day section ──────────────────────────────────────────────


def _build_day_block(
    sub_day: pd.DataFrame,
    *,
    area: str,
    forecast_date: date,
    source: str = "PJM",
) -> str | None:
    """One section's HTML: snapshot table + comparison chart."""
    # Build a (run -> HE-indexed Series) map. Pivot is keyed on
    # evaluated_at_ept; columns are he_start.
    pivot = (
        sub_day
        .drop_duplicates(["evaluated_at_ept", "he_start"], keep="last")
        .pivot(index="evaluated_at_ept", columns="he_start", values="forecast_load_mw")
        .sort_index()
        .reindex(columns=_HE_COLS)
    )

    if pivot.empty:
        return None

    latest_ts: pd.Timestamp = pivot.index.max()
    anchor_rows = _pick_anchors(pivot.index, latest_ts)

    chart_html = _comparison_chart(
        pivot,
        latest_ts=latest_ts,
        anchors=anchor_rows,
        forecast_date=forecast_date,
        area=area,
        source=source,
    )
    table_html = _snapshot_table(
        pivot,
        latest_ts=latest_ts,
        anchors=anchor_rows,
    )
    window_table_html = _window_table(
        pivot,
        latest_ts=latest_ts,
        window=timedelta(hours=12),
    )
    return (
        f"<div style='margin-bottom:14px;'>{chart_html}</div>"
        f"{table_html}"
        f"{window_table_html}"
    )


# ── anchor selection ──────────────────────────────────────────────


def _pick_anchors(
    index: pd.DatetimeIndex,
    latest_ts: pd.Timestamp,
) -> list[tuple[str, pd.Timestamp | None, str]]:
    """Return [(label, ts_or_None, color), ...] for the 48h / 24h / 12h slots.

    For each lag, picks the run whose evaluated_at_ept is closest to
    (latest_ts - lag). Returns None for the timestamp if no run exists
    within +/- 6 hours of the target — we don't want to label a 36h-ago
    run as "12h ago".
    """
    out: list[tuple[str, pd.Timestamp | None, str]] = []
    candidates = pd.DatetimeIndex(sorted(index))
    for label, lag, color in _LAGS:
        target = latest_ts - lag
        prior = candidates[candidates < latest_ts]
        if len(prior) == 0:
            out.append((label, None, color))
            continue
        abs_deltas = (prior - target).map(abs)
        pos = int(abs_deltas.argmin())
        best = prior[pos]
        # tolerance: a lag-bucket label is only meaningful if the closest
        # run is within +/- 6h of the target.
        if abs_deltas[pos] <= pd.Timedelta(hours=6):
            out.append((label, best, color))
        else:
            out.append((label, None, color))
    return out


# ── snapshot table ────────────────────────────────────────────────


def _snapshot_table(
    pivot: pd.DataFrame,
    *,
    latest_ts: pd.Timestamp,
    anchors: list[tuple[str, pd.Timestamp | None, str]],
) -> str:
    """All run rows in vintage order, with latest + anchors highlighted,
    then delta rows (latest - anchor) at the bottom.
    """
    headers = (
        '<th class="metric">Run (EPT)</th>'
        '<th class="metric">Tag</th>'
        '<th class="col-group-start summary-stat">Peak</th>'
        '<th class="summary-stat">OnPeak</th>'
        + "".join(
            f"<th class='{_he_class(h)}'>HE{h}</th>" for h in _HE_COLS
        )
    )

    # Match the plot: only render the LATEST row plus the 3 lag anchors
    # (oldest first, then LATEST), skipping anchors that fell outside
    # tolerance.
    body_rows: list[str] = []
    for label, ts, _color in anchors:
        if ts is None:
            continue
        body_rows.append(_value_row(ts, label, pivot.loc[ts], css="lag-row"))
    body_rows.append(_value_row(latest_ts, "LATEST", pivot.loc[latest_ts], css="latest-row"))

    # Delta rows at the bottom
    latest_row = pivot.loc[latest_ts]
    for label, ts, _color in anchors:
        if ts is None:
            continue
        diff = latest_row - pivot.loc[ts]
        body_rows.append(_delta_row(
            label=f"Δ vs {label}",
            anchor_ts=ts,
            diff=diff,
        ))

    return (
        '<div class="fd-wrap"><div class="fd-tw">'
        '<table class="fd-t">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table></div></div>'
    )


def _window_table(
    pivot: pd.DataFrame,
    *,
    latest_ts: pd.Timestamp,
    window: timedelta,
) -> str:
    """All runs whose evaluated_at_ept lies in (latest_ts - window, latest_ts].
    Same column shape as `_snapshot_table` — one row per run vintage,
    chronological. Used to show the intraday trajectory inside the window.
    """
    lower = latest_ts - window
    in_window = pivot.index[(pivot.index > lower) & (pivot.index <= latest_ts)]
    if len(in_window) == 0:
        return ""

    headers = (
        '<th class="metric">Run (EPT)</th>'
        '<th class="metric">Tag</th>'
        '<th class="col-group-start summary-stat">Peak</th>'
        '<th class="summary-stat">OnPeak</th>'
        + "".join(
            f"<th class='{_he_class(h)}'>HE{h}</th>" for h in _HE_COLS
        )
    )

    body_rows: list[str] = []
    for ts in sorted(in_window):
        tag = "LATEST" if ts == latest_ts else ""
        css = "latest-row" if ts == latest_ts else "lag-row"
        body_rows.append(_value_row(ts, tag, pivot.loc[ts], css=css))

    hours = int(window.total_seconds() // 3600)
    caption = (
        f"<div style='margin: 10px 0 4px 0; font-size: 11px; color: #475569;'>"
        f"Runs in last {hours}h (n={len(in_window)})</div>"
    )
    return (
        f"{caption}"
        '<div class="fd-wrap"><div class="fd-tw">'
        '<table class="fd-t">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table></div></div>'
    )


def _value_row(
    ts: pd.Timestamp,
    tag: str,
    row: pd.Series,
    *,
    css: str,
) -> str:
    """Render a forecast curve row with a per-row green-max / red-min gradient
    across HE0..HE23. Peak / OnPeak summary columns stay plain.
    """
    he_vals = row.reindex(_HE_COLS)
    vals = he_vals.dropna()
    vmin, vmax = (float(vals.min()), float(vals.max())) if not vals.empty else (0.0, 0.0)

    peak = row.max(skipna=True)
    onpeak = row.reindex(_ONPEAK_HE_STARTS).mean(skipna=True)
    cells = [
        f"<td class='metric'>{_fmt_ts(ts)}</td>",
        f"<td class='metric'>{tag}</td>",
        f"<td class='col-group-start summary-stat'>{_fmt_int(peak)}</td>",
        f"<td class='summary-stat'>{_fmt_int(onpeak)}</td>",
    ]
    cells += [
        _heat_cell(row.get(h), vmin, vmax, extra=_he_class(h))
        for h in _HE_COLS
    ]
    return f"<tr class='{css}'>{''.join(cells)}</tr>"


def _delta_row(
    *,
    label: str,
    anchor_ts: pd.Timestamp,
    diff: pd.Series,
) -> str:
    """Δ row with green-positive / red-negative gradient symmetric around 0."""
    he_diff = diff.reindex(_HE_COLS)
    vals = he_diff.dropna()
    bound = float(vals.abs().max()) if not vals.empty else 0.0

    peak_diff = diff.max(skipna=True)
    onpeak_diff = diff.reindex(_ONPEAK_HE_STARTS).mean(skipna=True)
    cells = [
        f"<td class='metric'>{_fmt_ts(anchor_ts)}</td>",
        f"<td class='metric'>{label}</td>",
        _delta_heat_cell(peak_diff, bound, extra="col-group-start summary-stat"),
        _delta_heat_cell(onpeak_diff, bound, extra="summary-stat"),
    ]
    cells += [
        _delta_heat_cell(diff.get(h), bound, extra=_he_class(h))
        for h in _HE_COLS
    ]
    return f"<tr class='delta-row'>{''.join(cells)}</tr>"


def _heat_cell(val, vmin: float, vmax: float, *, extra: str = "") -> str:
    """Per-row diverging cell: red at row-min, white midpoint, green at row-max."""
    cls = f" class='{extra}'" if extra else ""
    if val is None or pd.isna(val):
        return f"<td{cls}>—</td>"
    rng = vmax - vmin
    pos = (float(val) - vmin) / rng if rng > 0 else 0.5
    pos = max(0.0, min(1.0, pos))
    dev = (pos - 0.5) * 2.0  # -1 .. +1
    return _bg_td(val, dev, signed=False, extra=extra)


def _delta_heat_cell(val, bound: float, *, extra: str = "") -> str:
    """Δ cell: red at -bound, white at 0, green at +bound."""
    cls = f" class='{extra}'" if extra else ""
    if val is None or pd.isna(val):
        return f"<td{cls}>—</td>"
    dev = (float(val) / bound) if bound > 0 else 0.0
    dev = max(-1.0, min(1.0, dev))
    return _bg_td(val, dev, signed=True, extra=extra)


def _bg_td(val, dev: float, *, signed: bool, extra: str = "") -> str:
    cls = f" class='{extra}'" if extra else ""
    text = f"{float(val):+,.0f}" if signed else _fmt_int(val)
    if dev > 0:
        r, g, b = _HEAT_GREEN
        alpha = dev * _HEAT_MAX_ALPHA
    elif dev < 0:
        r, g, b = _HEAT_RED
        alpha = -dev * _HEAT_MAX_ALPHA
    else:
        return f"<td{cls}>{text}</td>"
    return f"<td{cls} style='background:rgba({r},{g},{b},{alpha:.2f});'>{text}</td>"


# ── chart ─────────────────────────────────────────────────────────


def _comparison_chart(
    pivot: pd.DataFrame,
    *,
    latest_ts: pd.Timestamp,
    anchors: list[tuple[str, pd.Timestamp | None, str]],
    forecast_date: date,
    area: str,
    source: str = "PJM",
) -> str:
    """One panel: HE0..HE23 on x-axis, MW on y. Trace per anchor + latest."""
    fig = go.Figure()

    for label, ts, color in reversed(anchors):  # draw oldest first
        if ts is None:
            continue
        y = pivot.loc[ts].reindex(_HE_COLS).values
        fig.add_trace(go.Scatter(
            x=_HE_COLS, y=y,
            mode="lines+markers",
            name=f"{label} ({_fmt_ts(ts)})",
            line=dict(color=color, width=1.6, dash="dot"),
            marker=dict(size=4),
            hovertemplate="HE %{x}<br>" + label + ": %{y:,.0f} MW<extra></extra>",
        ))

    latest_y = pivot.loc[latest_ts].reindex(_HE_COLS).values
    fig.add_trace(go.Scatter(
        x=_HE_COLS, y=latest_y,
        mode="lines+markers",
        name=f"LATEST ({_fmt_ts(latest_ts)})",
        line=dict(color=_LATEST_COLOR, width=2.4),
        marker=dict(size=5),
        hovertemplate="HE %{x}<br>LATEST: %{y:,.0f} MW<extra></extra>",
    ))

    fig.update_layout(
        title=f"{_section_label(area=area, source=source)} — Forecast for {forecast_date}",
        template=PLOTLY_TEMPLATE,
        height=420,
        margin=dict(l=60, r=40, t=60, b=60),
        legend=dict(orientation="h", yanchor="top", y=-0.14, x=0),
        hovermode="x unified",
    )
    fig.update_xaxes(
        title_text="Hour Beginning (EPT)",
        dtick=1, range=[-0.5, 23.5],
        autorange=False, fixedrange=True,
        gridcolor="rgba(99,110,250,0.08)",
    )
    fig.update_yaxes(
        title_text="MW",
        tickformat=",",
        gridcolor="rgba(99,110,250,0.1)",
    )

    div_id = _safe_div_id(f"fd-{source}-{area}-{forecast_date}")
    return deferred_plotly_html(
        fig, div_id=div_id, height=420, config=PLOTLY_LOCKED_CONFIG,
    )


# ── helpers ───────────────────────────────────────────────────────


def _he_class(he: int) -> str:
    classes: list[str] = []
    if he == 0:
        classes.append("col-group-start")
    if 7 <= he <= 22:
        classes.append("peak")
    return " ".join(classes)


def _fmt_int(val) -> str:
    if val is None or pd.isna(val):
        return "—"
    return f"{float(val):,.0f}"


def _fmt_ts(ts: pd.Timestamp) -> str:
    return pd.Timestamp(ts).strftime("%m-%d %H:%M")


def _pretty_area(area: str) -> str:
    return area.replace("_", " ").title().replace("Rto", "RTO")


def _section_label(*, area: str, source: str) -> str:
    """Inner-nav / chart-title label, e.g. 'RTO Combined - PJM'."""
    return f"{_pretty_area(area)} - {source}"


def _format_date_label(d: date) -> str:
    """DDD MMM-DD, e.g. 'Fri May-15'."""
    return datetime(d.year, d.month, d.day).strftime("%a %b-%d")


def _safe_div_id(raw: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "-", raw)


__all__ = ["build_fragments", "DEFAULT_AREAS"]
