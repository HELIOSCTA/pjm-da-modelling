"""Transmission Outages fragments — 4 inner sections, all flat tables.

Inner sidebar layout (in order):
  - Active           : every active >=230 kV outage, with regional summary strip
  - Starting Today   : outages whose start_datetime falls on `target_date`
  - Returning Soon   : outages with days_to_return <= 3 (sorted soonest first)
  - Network Match    : PSS/E-enriched table with bus IDs / rating_mva / neighbor count

Pure tables, white-bg LMP-style chrome — no charts in v1. Cell shading kept
intentionally minimal: red tint for high-risk rows, voltage-tier badge in
the kV column, days-to-return cells gradient (bright red for <=1 day).
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from backend.mcp_server.views.transmission_outages import (
    build_active_view_model,
    build_network_view_model,
)
from backend.reports._forecast_utils import empty_html
from backend.reports.pjm_da_report.transmission_outages.loader import (
    load_active_outages,
)

Fragment = tuple[str, str, str | None]


# Same chrome as the LMP and DA Constraints tables — white bg, sticky
# header, monospace tabular numerals, soft borders. Plus a few outage-
# specific accents (risk row, voltage tier badges, return-soon cells).
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
.rs-t td.metric { text-align: left; color: #0f172a; }
.rs-t tr:nth-child(even) td { background: #fafbfc; }

/* High-risk outage rows: light red wash. */
.rs-t tr.risk td { background: #fef2f2; }
.rs-t tr.risk:nth-child(even) td { background: #fee2e2; }
.rs-t tr.risk td.metric { color: #991b1b; font-weight: 700; }

/* Voltage tier accent on the kV column. */
.rs-t td.kv-765 { background: #fef3c7 !important; font-weight: 700; }
.rs-t td.kv-500 { background: #fed7aa !important; font-weight: 700; }
.rs-t td.kv-345 { background: #fde68a !important; }
.rs-t td.kv-230 { background: #f1f5f9 !important; }

/* Days-to-return gradient: closer return = brighter red. */
.rs-t td.dtr-0 { background: #dc2626 !important; color: #ffffff; font-weight: 700; }
.rs-t td.dtr-1 { background: #ef4444 !important; color: #ffffff; font-weight: 700; }
.rs-t td.dtr-2 { background: #f97316 !important; color: #ffffff; }
.rs-t td.dtr-3 { background: #fbbf24 !important; }

/* Coverage strip — same style as DA Constraints. */
.rs-cov {
  display: flex; gap: 18px; flex-wrap: wrap;
  padding: 10px 14px; margin: 8px 0;
  background: #f8fafc; border: 1px solid #cbd5e1; border-radius: 6px;
  font-family: "SF Mono", Consolas, monospace; font-size: 12px; color: #1e293b;
}
.rs-cov b { color: #0f172a; }

/* Region summary strip — one chip per region. */
.rs-region {
  display: flex; gap: 10px; flex-wrap: wrap;
  padding: 10px 14px; margin: 8px 0;
  background: #ffffff; border: 1px solid #e2e8f0; border-radius: 6px;
  font-family: "SF Mono", Consolas, monospace; font-size: 11px; color: #0f172a;
}
.rs-region .chip {
  padding: 4px 8px; border: 1px solid #cbd5e1; border-radius: 4px;
  background: #f8fafc;
}
.rs-region .chip.high { background: #fef2f2; border-color: #fecaca; color: #991b1b; }
.rs-region .chip b { color: #0f172a; }
</style>
"""


# ── Entry point ────────────────────────────────────────────────────

def build_fragments(target_date: date) -> list[str | Fragment]:
    """Compose the Transmission Outages section."""
    try:
        enriched, branches = load_active_outages()
    except Exception as exc:  # pylint: disable=broad-except
        return [
            f"Transmission Outages — {target_date}",
            ("Status", empty_html(f"Outages unavailable: {exc}"), None),
        ]

    if enriched is None or enriched.empty:
        return [
            f"Transmission Outages — {target_date}",
            ("Status", empty_html(
                f"No active >=230 kV outages for {target_date}."
            ), None),
        ]

    # Build the view-models once and slice them across sections.
    active_vm = build_active_view_model(enriched, target_date)
    network_vm = build_network_view_model(enriched, branches, target_date)

    items: list[str | Fragment] = [f"Transmission Outages — {target_date}"]
    items.append(("Active", _active_section(active_vm), None))
    items.append((
        "Starting Today",
        _starting_today_section(active_vm, target_date=target_date),
        None,
    ))
    items.append((
        "Returning Soon",
        _returning_soon_section(active_vm),
        None,
    ))
    items.append(("Network Match", _network_section(network_vm), None))
    return items


# ── Active section ─────────────────────────────────────────────────

def _active_section(vm: dict) -> str:
    total = vm.get("total_active", 0)
    regional = vm.get("regional_summary", []) or []
    notable = vm.get("notable_outages", []) or []

    region_strip = _region_strip(regional)
    notable_table = _outage_table(
        notable,
        empty_msg="No notable outages.",
        show_tags=True,
    )

    header = (
        f'<div class="rs-cov">'
        f'<span><b>Total active</b>: {total:,}</span>'
        f'<span><b>Notable</b>: {len(notable):,}</span>'
        f'<span><b>Regions</b>: {len(regional):,}</span>'
        f'</div>'
    )

    return f"{_TABLE_STYLE}{header}{region_strip}{notable_table}"


def _region_strip(regional: list[dict]) -> str:
    if not regional:
        return ""
    chips = []
    for r in regional:
        cls = "chip high" if r.get("risk_flagged", 0) > 0 else "chip"
        chips.append(
            f'<span class="{cls}">'
            f'<b>{r["region"]}</b> &nbsp;'
            f'{r["total"]} &middot; '
            f'500+={r["count_500kv"] + r["count_765kv"]} &middot; '
            f'345={r["count_345kv"]} &middot; '
            f'230={r["count_230kv"]}'
            f'{" &middot; " + str(r["risk_flagged"]) + " risk" if r.get("risk_flagged") else ""}'
            f'</span>'
        )
    return f'<div class="rs-region">{"".join(chips)}</div>'


# ── Starting Today section ─────────────────────────────────────────

def _starting_today_section(vm: dict, *, target_date: date) -> str:
    target_str = target_date.isoformat()
    starting = [
        r for r in vm.get("notable_outages", []) or []
        if r.get("started") == target_str
    ]
    # `notable_outages` is filtered (high-risk / 500+ / new / returning).
    # New outages may not yet be tagged "new" if the scrape lag straddles
    # today, so also fall back to the raw active records the active VM
    # didn't render — we don't have those here. For v1, "Starting Today"
    # surfaces only notable outages that started today; if empty, the
    # status message tells the reader.
    table = _outage_table(
        starting,
        empty_msg=f"No outages started on {target_date}.",
    )
    return f"{_TABLE_STYLE}{table}"


# ── Returning Soon section ─────────────────────────────────────────

def _returning_soon_section(vm: dict) -> str:
    soon = [
        r for r in vm.get("notable_outages", []) or []
        if r.get("days_to_return") is not None and r["days_to_return"] <= 3
    ]
    soon.sort(key=lambda r: (r.get("days_to_return", 999), -(r.get("kv") or 0)))
    table = _outage_table(
        soon,
        empty_msg="No outages returning within 3 days.",
        highlight_dtr=True,
    )
    return f"{_TABLE_STYLE}{table}"


# ── Network Match section ──────────────────────────────────────────

def _network_section(vm: dict) -> str:
    cov = vm.get("match_coverage", {}) or {}
    matched = vm.get("matched_outages", []) or []
    ambiguous = vm.get("ambiguous_outages", []) or []
    unmatched = vm.get("unmatched_outages", []) or []

    rate = cov.get("match_rate_pct", 0.0)
    coverage_html = (
        f'<div class="rs-cov">'
        f'<span><b>Total</b>: {cov.get("total", 0)}</span>'
        f'<span><b>Matched</b>: {cov.get("matched", 0)}</span>'
        f'<span><b>Ambiguous</b>: {cov.get("ambiguous", 0)}</span>'
        f'<span><b>Unmatched</b>: {cov.get("unmatched", 0)}</span>'
        f'<span><b>Match rate</b>: {rate:.1f}%</span>'
        f'</div>'
    )

    matched_html = _network_table(
        matched, empty_msg="No matched outages."
    )

    extras = []
    if ambiguous:
        extras.append(
            "<div style='padding:6px 8px;font-size:11px;color:#64748b;'>"
            f"Ambiguous matches: {len(ambiguous)} (multi-XFMR substations or "
            "look-alike facility names) — first PSS/E candidate shown.</div>"
        )
        extras.append(_network_table(ambiguous, empty_msg=""))
    if unmatched:
        extras.append(
            "<div style='padding:6px 8px;font-size:11px;color:#64748b;'>"
            f"Unmatched: {len(unmatched)} — facility not found in 2021 PSS/E "
            "model (new substation or non-standard description).</div>"
        )
        extras.append(_outage_table(unmatched, empty_msg=""))

    return f"{_TABLE_STYLE}{coverage_html}{matched_html}{''.join(extras)}"


# ── Generic table renderers ────────────────────────────────────────

def _outage_table(
    records: list[dict],
    *,
    empty_msg: str,
    show_tags: bool = False,
    highlight_dtr: bool = False,
) -> str:
    if not records:
        return empty_html(empty_msg) if empty_msg else ""

    headers_cols = [
        ("metric", "Ticket"),
        ("metric", "Region"),
        ("metric", "Facility"),
        ("",       "kV"),
        ("metric", "Equip"),
        ("metric", "Started"),
        ("metric", "Est Return"),
        ("",       "Days Out"),
        ("",       "Days to Ret"),
        ("metric", "State"),
        ("metric", "Risk"),
        ("metric", "Cause"),
    ]
    if show_tags:
        headers_cols.append(("metric", "Tags"))

    headers = "".join(
        f"<th class='{cls}'>{label}</th>" if cls else f"<th>{label}</th>"
        for cls, label in headers_cols
    )

    body_rows: list[str] = []
    for r in records:
        risk = r.get("risk_flag")
        kv = r.get("kv")
        dtr = r.get("days_to_return")
        cells = [
            f"<td class='metric'>{r.get('ticket_id') or '—'}</td>",
            f"<td class='metric'>{_e(r.get('region'))}</td>",
            f"<td class='metric'>{_trunc(r.get('facility'), 44)}</td>",
            f"<td class='{_kv_class(kv)}'>{kv if kv is not None else '—'}</td>",
            f"<td class='metric'>{_e(r.get('equip'))}</td>",
            f"<td class='metric'>{_e(r.get('started'))}</td>",
            f"<td class='metric'>{_e(r.get('est_return'))}</td>",
            f"<td>{_e(r.get('days_out'))}</td>",
            (
                f"<td class='{_dtr_class(dtr) if highlight_dtr else ''}'>"
                f"{_e(dtr)}</td>"
            ),
            f"<td class='metric'>{_e(r.get('outage_state'))}</td>",
            f"<td class='metric'>{'YES' if risk else ''}</td>",
            f"<td class='metric'>{_trunc(r.get('cause'), 28)}</td>",
        ]
        if show_tags:
            tags = r.get("tags") or []
            cells.append(
                f"<td class='metric'>{', '.join(tags) if tags else ''}</td>"
            )
        row_cls = " class='risk'" if risk else ""
        body_rows.append(f"<tr{row_cls}>{''.join(cells)}</tr>")

    return (
        '<div class="rs-wrap"><div class="rs-tw">'
        '<table class="rs-t">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table></div></div>'
    )


def _network_table(records: list[dict], *, empty_msg: str) -> str:
    if not records:
        return empty_html(empty_msg) if empty_msg else ""

    headers = (
        '<th class="metric">Ticket</th>'
        '<th class="metric">Region</th>'
        '<th class="metric">Facility</th>'
        '<th>kV</th>'
        '<th class="metric">Equip</th>'
        '<th>From Bus</th>'
        '<th>To Bus</th>'
        '<th>MVA</th>'
        '<th>Neighbors</th>'
        '<th class="metric">Match</th>'
        '<th class="metric">Est Return</th>'
        '<th class="metric">Risk</th>'
    )

    body_rows: list[str] = []
    for r in records:
        kv = r.get("kv")
        rating = r.get("rating_mva")
        risk = r.get("risk_flag")
        cells = [
            f"<td class='metric'>{r.get('ticket_id') or '—'}</td>",
            f"<td class='metric'>{_e(r.get('region'))}</td>",
            f"<td class='metric'>{_trunc(r.get('facility'), 44)}</td>",
            f"<td class='{_kv_class(kv)}'>{kv if kv is not None else '—'}</td>",
            f"<td class='metric'>{_e(r.get('equip'))}</td>",
            f"<td>{_e(r.get('from_bus_psse'))}</td>",
            f"<td>{_e(r.get('to_bus_psse'))}</td>",
            f"<td>{f'{rating:,.0f}' if rating else '—'}</td>",
            f"<td>{_e(r.get('neighbor_count'))}</td>",
            f"<td class='metric'>{_e(r.get('match_status'))}</td>",
            f"<td class='metric'>{_e(r.get('est_return'))}</td>",
            f"<td class='metric'>{'YES' if risk else ''}</td>",
        ]
        row_cls = " class='risk'" if risk else ""
        body_rows.append(f"<tr{row_cls}>{''.join(cells)}</tr>")

    return (
        '<div class="rs-wrap"><div class="rs-tw">'
        '<table class="rs-t">'
        f'<thead><tr>{headers}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table></div></div>'
    )


# ── Helpers ────────────────────────────────────────────────────────

def _kv_class(kv) -> str:
    if kv is None:
        return ""
    if kv >= 765:
        return "kv-765"
    if kv >= 500:
        return "kv-500"
    if kv >= 345:
        return "kv-345"
    return "kv-230"


def _dtr_class(dtr) -> str:
    if dtr is None or pd.isna(dtr):
        return ""
    try:
        d = int(dtr)
    except (TypeError, ValueError):
        return ""
    if d <= 0:
        return "dtr-0"
    if d == 1:
        return "dtr-1"
    if d == 2:
        return "dtr-2"
    if d == 3:
        return "dtr-3"
    return ""


def _e(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return str(val)


def _trunc(s, n: int) -> str:
    if not s:
        return ""
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"
