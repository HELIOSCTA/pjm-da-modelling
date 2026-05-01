"""Markdown formatters for the transmission-outage MCP endpoints.

One formatter per view model. Each takes the dict produced by the matching
builder in `views/transmission_outages.py` and returns a markdown string.
"""
from __future__ import annotations

from tabulate import tabulate


def _table(headers: list[str], rows: list[list], floatfmt: str = ".2f") -> str:
    """Render a markdown pipe table via tabulate."""
    return tabulate(rows, headers=headers, tablefmt="pipe", numalign="right")


def _route(rec: dict) -> str:
    """Compose 'FROM→TO' for lines, station for transformers/PS, '-' otherwise."""
    if rec.get("from_station") and rec.get("to_station"):
        return f"{rec['from_station']}→{rec['to_station']}"
    if rec.get("station"):
        return rec["station"]
    return "-"


# ─── Active mart ─────────────────────────────────────────────────────────────


def format_transmission_outages_active(vm: dict) -> str:
    """Markdown for ``GET /views/transmission_outages_active``."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    parts.append(f"# Transmission Outages — Active — {vm.get('reference_date', '?')}")
    parts.append(
        f"\nActive/Approved ≥230 kV LINE/XFMR/PS: "
        f"**{vm.get('total_active', '?')}** outages"
    )

    regional = vm.get("regional_summary", [])
    if regional:
        parts.append("\n## Regional Summary")
        headers = [
            "Region", "Total", "Lines", "Equip",
            "765kV", "500kV", "345kV", "230kV",
            "Risk", "Longest Out", "Soonest Return",
        ]
        rows = []
        for r in regional:
            rows.append([
                r["region"],
                r["total"],
                r.get("path_count") or "-",
                r.get("capacity_count") or "-",
                r["count_765kv"] or "-",
                r["count_500kv"] or "-",
                r["count_345kv"] or "-",
                r["count_230kv"] or "-",
                r["risk_flagged"] or "-",
                f"{r['longest_out_days']}d" if r.get("longest_out_days") else "-",
                f"{r['soonest_return_days']}d" if r.get("soonest_return_days") is not None else "-",
            ])
        parts.append(_table(headers, rows))

    notable = vm.get("notable_outages", [])
    if notable:
        parts.append(f"\n## Notable Outages ({len(notable)})")
        headers = [
            "Tags", "Region", "Facility", "Type", "kV", "Route",
            "Started", "Est Return", "Days Out", "Days Left", "Cause",
        ]
        rows = []
        for n in notable:
            rows.append([
                ", ".join(n["tags"]),
                n["region"],
                n.get("facility", "")[:40],
                n.get("equip_category", n.get("equip", "")),
                n["kv"],
                _route(n),
                n.get("started", "-"),
                n.get("est_return", "-"),
                n.get("days_out", "-"),
                n.get("days_to_return") if n.get("days_to_return") is not None else "overdue",
                n.get("cause", "")[:35],
            ])
        parts.append(_table(headers, rows))

    return "\n".join(parts)


# ─── Window 7d mart ──────────────────────────────────────────────────────────


def format_transmission_outages_window_7d(vm: dict) -> str:
    """Markdown for ``GET /views/transmission_outages_window_7d``."""
    parts: list[str] = []
    parts.append(f"# Transmission Outages — Next 7 Days — {vm.get('reference_date', '?')}")
    parts.append(
        f"\n**{vm.get('total', 0)}** outages overlap the window — "
        f"**{vm.get('locked_count', 0)} locked** (Active/Approved), "
        f"**{vm.get('planned_count', 0)} planned** (Received)"
    )

    regional = vm.get("regional_summary", [])
    if regional:
        parts.append("\n## Regional Summary")
        headers = ["Region", "Total", "Locked", "Planned", "500kV+", "Risk"]
        rows = []
        for r in regional:
            rows.append([
                r["region"],
                r["total"],
                r["locked"] or "-",
                r["planned"] or "-",
                r["count_500kv_plus"] or "-",
                r["risk_flagged"] or "-",
            ])
        parts.append(_table(headers, rows))

    locked = vm.get("locked_outages", [])
    if locked:
        parts.append(f"\n## Locked Outages ({len(locked)}) — Active or Approved, sorted by days-to-return")
        parts.append(_window_outage_table(locked))

    planned = vm.get("planned_outages", [])
    if planned:
        parts.append(f"\n## Planned Outages ({len(planned)}) — Received (unapproved), sorted by start date")
        parts.append(_window_outage_table(planned))

    return "\n".join(parts)


def _window_outage_table(outages: list[dict]) -> str:
    headers = [
        "Region", "Facility", "Type", "kV", "Route",
        "State", "Risk", "Start", "End", "Days Left", "Cause",
    ]
    rows = []
    for n in outages:
        rows.append([
            n["region"],
            n.get("facility", "")[:40],
            n.get("equip_category", n.get("equip", "")),
            n["kv"],
            _route(n),
            n.get("outage_state", "-"),
            "Yes" if n.get("risk_flag") else "-",
            n.get("started", "-"),
            n.get("est_return", "-"),
            n.get("days_to_return") if n.get("days_to_return") is not None else "overdue",
            n.get("cause", "")[:35],
        ])
    return _table(headers, rows)


# ─── Changes 24h — simple ────────────────────────────────────────────────────


def format_transmission_outages_changes_24h_simple(vm: dict) -> str:
    """Markdown for ``GET /views/transmission_outages_changes_24h_simple``."""
    parts: list[str] = []
    parts.append(f"# Transmission Outages — Last 24h Delta (simple) — {vm.get('reference_date', '?')}")
    parts.append(
        f"\n**{vm.get('total_changes', 0)}** changes — "
        f"**{vm.get('new_count', 0)} new**, **{vm.get('revised_count', 0)} revised**. "
        f"_Source: created_at / last_revised on the source table._"
    )

    new_t = vm.get("new_tickets", [])
    if new_t:
        parts.append(f"\n## New Tickets ({len(new_t)}) — first appeared in last 24h")
        parts.append(_change_outage_table(new_t, include_diff=False))

    rev = vm.get("revised_tickets", [])
    if rev:
        parts.append(f"\n## Revised Tickets ({len(rev)}) — PJM revised existing rows")
        parts.append(_change_outage_table(rev, include_diff=False))

    if not new_t and not rev:
        parts.append("\n_No changes in the last 24h._")

    return "\n".join(parts)


# ─── Changes 24h — snapshot ──────────────────────────────────────────────────


def format_transmission_outages_changes_24h_snapshot(vm: dict) -> str:
    """Markdown for ``GET /views/transmission_outages_changes_24h_snapshot``."""
    parts: list[str] = []
    parts.append(f"# Transmission Outages — Last 24h Delta (snapshot) — {vm.get('reference_date', '?')}")

    note = vm.get("note")
    if note:
        parts.append(f"\n> {note}")

    parts.append(
        f"\n**{vm.get('total_changes', 0)}** changes — "
        f"**{vm.get('new_count', 0)} new**, "
        f"**{vm.get('revised_count', 0)} revised**, "
        f"**{vm.get('cleared_count', 0)} cleared**. "
        f"_Source: SCD2 snapshot diff._"
    )

    new_t = vm.get("new_tickets", [])
    if new_t:
        parts.append(f"\n## New Tickets ({len(new_t)}) — first appeared in last 24h")
        parts.append(_change_outage_table(new_t, include_diff=False))

    rev = vm.get("revised_tickets", [])
    if rev:
        parts.append(f"\n## Revised Tickets ({len(rev)}) — diff vs prior snapshot")
        parts.append(_change_outage_table(rev, include_diff=True))

    cleared = vm.get("cleared_tickets", [])
    if cleared:
        parts.append(f"\n## Cleared Tickets ({len(cleared)}) — vanished from PJM source in last 24h")
        parts.append(_change_outage_table(cleared, include_diff=False))

    if not new_t and not rev and not cleared and not note:
        parts.append("\n_No changes in the last 24h._")

    return "\n".join(parts)


def _change_outage_table(outages: list[dict], *, include_diff: bool) -> str:
    """Shared row layout for change tables (NEW/REVISED/CLEARED).

    When ``include_diff`` is True, an extra "Diff" column shows the synthesized
    diff_text from the snapshot variant (e.g. "end: 5/12 → 5/19, state: ...").
    """
    headers = [
        "Region", "Facility", "Type", "kV", "Route",
        "State", "Start", "End", "Risk", "Cause",
    ]
    if include_diff:
        headers.append("Diff")

    rows = []
    for n in outages:
        row = [
            n["region"],
            n.get("facility", "")[:40],
            n.get("equip_category", n.get("equip", "")),
            n["kv"],
            _route(n),
            n.get("outage_state", "-"),
            n.get("started", "-"),
            n.get("est_return", "-"),
            "Yes" if n.get("risk_flag") else "-",
            n.get("cause", "")[:30],
        ]
        if include_diff:
            row.append(n.get("diff_text", "-"))
        rows.append(row)
    return _table(headers, rows)


# ─── Network-enriched view ───────────────────────────────────────────────────


def format_transmission_outages_network(vm: dict) -> str:
    """Markdown for ``GET /views/transmission_outages_network``."""
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    parts.append(f"# Transmission Outages — Network Enrichment — {vm.get('reference_date', '?')}")

    cov = vm.get("match_coverage", {})
    parts.append(
        f"\n**Match coverage**: {cov.get('matched', 0) + cov.get('ambiguous', 0)} / "
        f"{cov.get('total', 0)} ({cov.get('match_rate_pct', 0)}%) — "
        f"{cov.get('matched', 0)} unique, "
        f"{cov.get('ambiguous', 0)} multi-match, "
        f"{cov.get('unmatched', 0)} unmatched"
    )

    matched = vm.get("matched_outages", [])
    if matched:
        parts.append(f"\n## Matched ({len(matched)})")
        parts.append(_network_outage_table(matched, with_neighbors=True))

    ambiguous = vm.get("ambiguous_outages", [])
    if ambiguous:
        parts.append(
            f"\n## Ambiguous ({len(ambiguous)}) — first PSS/E candidate shown; "
            f"facility name maps to multiple branches at same substation+kV"
        )
        parts.append(_network_outage_table(ambiguous, with_neighbors=True))

    unmatched = vm.get("unmatched_outages", [])
    if unmatched:
        parts.append(
            f"\n## Unmatched ({len(unmatched)}) — substation missing from PSS/E "
            f"model or non-standard facility description"
        )
        parts.append(_network_unmatched_table(unmatched))

    return "\n".join(parts)


def _network_outage_table(outages: list[dict], *, with_neighbors: bool) -> str:
    headers = [
        "Region", "Facility", "Type", "kV", "Route",
        "From Bus", "To Bus", "Rating MVA", "Neighbors",
    ]
    if with_neighbors:
        headers.append("Top Neighbors")

    rows = []
    for n in outages:
        row = [
            n["region"],
            n.get("facility", "")[:40],
            n.get("equip_category", n.get("equip", "")),
            n["kv"],
            _route(n),
            n.get("from_bus_psse", "-"),
            n.get("to_bus_psse", "-"),
            f"{n['rating_mva']:,.0f}" if n.get("rating_mva") else "-",
            n.get("neighbor_count", "-"),
        ]
        if with_neighbors:
            row.append(_format_neighbors(n.get("neighbors", [])))
        rows.append(row)
    return _table(headers, rows)


def _network_unmatched_table(outages: list[dict]) -> str:
    headers = ["Region", "Zone", "Facility", "Type", "kV"]
    rows = [
        [n["region"], n.get("zone", "-"), n.get("facility", "")[:50],
         n.get("equip_category", n.get("equip", "")), n["kv"]]
        for n in outages
    ]
    return _table(headers, rows)


def _format_neighbors(neighbors: list[dict]) -> str:
    """One-line summary of top 1-hop neighbors."""
    if not neighbors:
        return "-"
    parts = []
    for nb in neighbors[:3]:
        if nb.get("equipment_type") == "LINE":
            label = f"{nb['from_name']}→{nb['to_name']}"
        else:
            label = f"XFMR@{nb['from_name']}"
        parts.append(f"{label} ({int(nb['voltage_kv'])}kV)")
    return "; ".join(parts)
