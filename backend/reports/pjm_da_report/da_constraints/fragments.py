"""DA / RT / DART Constraints fragments — flat tables, LMP-style chrome.

Inner sidebar layout (in order):
  - DA Tomorrow    : DA constraints for `target_date` (current report focus)
  - DA Yesterday   : DA constraints for `target_date - 1`
  - RT Yesterday   : RT constraints for `target_date - 1`
  - DART Yesterday : DART (RT - DA) constraints for `target_date - 1`

Each section shares the same flat-table shape:
  Constraint | Contingency | Total | HE1..HE24 | Reported Name
Rows sort by |total_price| desc; cells use a single-direction red gradient
self-scaled to that section's data so RT magnitudes don't crush DA detail.
Values display as absolute magnitudes.
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from plotly import colors as pc

from backend.mcp_server.data.constraints import (
    pull_constraints_da,
    pull_constraints_rt_dart,
)
from backend.reports._forecast_utils import empty_html

Fragment = tuple[str, str, str | None]

_FLAT_HOURS = list(range(1, 25))
_HE_COLS = [f"he{h:02d}" for h in _FLAT_HOURS]

# Monotone red scale — sample from light pink to deep red.
_RED_SCALE = "Reds"

# Match LMP table chrome: white background, sticky header, peak-hour
# header accent, monospace tabular numerals, soft borders.
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

.rs-t th.col-group-start, .rs-t td.col-group-start {
  border-left: 1px solid #cbd5e1;
}

/* Bold the Total column body cells so the row's headline reads first. */
.rs-t td.total { font-weight: 700; }

/* Peak HE columns (HE8-HE23): header-only accent. */
.rs-t th.peak { background: #eef2ff; color: #3730a3; }
.rs-t th.peak-start { border-left: 1px solid #cbd5e1; }
.rs-t th.peak-end   { border-right: 1px solid #cbd5e1; }
.rs-t td.peak-start { border-left: 1px dotted #e2e8f0; }
.rs-t td.peak-end   { border-right: 1px dotted #e2e8f0; }
</style>
"""


# ── Entry point ───────────────────────────────────────────────────

def build_fragments(target_date: date) -> list[str | Fragment]:
    """Four sections: DA tomorrow + DA/RT/DART for the previous day."""
    prev = target_date - timedelta(days=1)

    rt_dart_html = _rt_dart_split(prev)

    return [
        f"DA Constraints — {target_date}",
        ("DA Tomorrow",    _da_section(target_date), None),
        ("DA Yesterday",   _da_section(prev),        None),
        ("RT Yesterday",   rt_dart_html["RT"],       None),
        ("DART Yesterday", rt_dart_html["DART"],     None),
    ]


def _da_section(d: date) -> str:
    """DA constraints table for one date, with empty / error handling."""
    try:
        df = pull_constraints_da(d)
    except Exception as exc:  # pylint: disable=broad-except
        return f"{_TABLE_STYLE}{empty_html(f'DA constraints for {d} unavailable: {exc}')}"
    if df is None or df.empty:
        return f"{_TABLE_STYLE}{empty_html(f'No DA constraints rows for {d}.')}"
    return _build_table(df)


def _rt_dart_split(d: date) -> dict[str, str]:
    """Single pull for RT + DART on date `d`; split by `market` column.

    The pivot mart returns long-form rows with `market in ('RT','DART')`.
    Splitting client-side avoids a second SQL roundtrip.
    """
    try:
        df = pull_constraints_rt_dart(d, d)
    except Exception as exc:  # pylint: disable=broad-except
        msg = f"{_TABLE_STYLE}{empty_html(f'RT/DART constraints for {d} unavailable: {exc}')}"
        return {"RT": msg, "DART": msg}
    if df is None or df.empty:
        msg = f"{_TABLE_STYLE}{empty_html(f'No RT/DART constraints rows for {d}.')}"
        return {"RT": msg, "DART": msg}

    out: dict[str, str] = {}
    for market in ("RT", "DART"):
        sub = df[df["market"] == market]
        if sub.empty:
            out[market] = (
                f"{_TABLE_STYLE}"
                f"{empty_html(f'No {market} constraints for {d}.')}"
            )
        else:
            out[market] = _build_table(sub)
    return out


# ── Table ─────────────────────────────────────────────────────────

def _build_table(df: pd.DataFrame) -> str:
    df = df.copy()
    df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce")
    df["abs_total"] = df["total_price"].abs()
    df = df.sort_values("abs_total", ascending=False, na_position="last")

    # Single global vmax across Total + every HE cell so color intensity is
    # comparable across rows and across columns.
    he_vals = df[_HE_COLS].apply(pd.to_numeric, errors="coerce").abs()
    vmax = max(
        _safe_float(he_vals.max(skipna=True).max()),
        _safe_float(df["abs_total"].max(skipna=True)),
        1.0,
    )

    headers = (
        '<th class="metric">Constraint</th>'
        '<th class="metric">Contingency</th>'
        '<th class="col-group-start">Total</th>'
        + "".join(
            f"<th class='{_he_class(h)}'>{h}</th>" for h in _FLAT_HOURS
        )
        + '<th class="metric col-group-start">Reported Name</th>'
    )

    body_rows = [_row(r, vmax=vmax) for _, r in df.iterrows()]

    return (
        f"{_TABLE_STYLE}"
        '<div class="rs-wrap"><div class="rs-tw">'
        '<table class="rs-t">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table></div></div>'
    )


def _row(row: pd.Series, *, vmax: float) -> str:
    name = row.get("constraint_name") or "—"
    ctgcy = row.get("contingency") or "—"
    reported = row.get("reported_name") or ""

    cells = [
        f"<td class='metric'>{_trunc(name, 44)}</td>",
        f"<td class='metric'>{_trunc(ctgcy, 38)}</td>",
        _red_cell(_safe_float(row.get("total_price")), vmax,
                  extra_class="col-group-start total"),
    ]
    for h in _FLAT_HOURS:
        cells.append(_red_cell(
            _safe_float(row.get(f"he{h:02d}")), vmax,
            extra_class=_he_class(h),
        ))
    cells.append(
        f"<td class='metric col-group-start'>{_trunc(reported, 44)}</td>"
    )
    return f"<tr>{''.join(cells)}</tr>"


# ── Helpers ───────────────────────────────────────────────────────

def _red_cell(val, vmax: float, *, extra_class: str = "") -> str:
    cls = f" class='{extra_class}'" if extra_class else ""
    if val is None or pd.isna(val) or val == 0:
        return f"<td{cls}>—</td>"
    mag = abs(float(val))
    pos = min(1.0, mag / vmax) if vmax > 0 else 0.0
    bg = pc.sample_colorscale(_RED_SCALE, [pos])[0]
    text = "#ffffff" if pos > 0.55 else "#0f172a"
    return (
        f"<td{cls} style='background:{bg};color:{text};'>"
        f"{mag:,.0f}</td>"
    )


def _he_class(he: int) -> str:
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


def _safe_float(val) -> float:
    if val is None or pd.isna(val):
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _trunc(s, n: int) -> str:
    if not s:
        return ""
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"
