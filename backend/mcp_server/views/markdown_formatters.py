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


# ─── Binding constraints — DA forward + RT/DART backward ─────────────────────


def _constraint_route(rec: dict) -> str:
    """Compose 'FROM→TO' or single-station from a constraint record."""
    f, t = rec.get("parsed_from_station"), rec.get("parsed_to_station")
    if f and t:
        return f"{f}→{t}"
    s = rec.get("parsed_single_station")
    if s:
        return s
    return "-"


def _bus_pair(rec: dict) -> str:
    fb, tb = rec.get("from_bus_psse"), rec.get("to_bus_psse")
    if fb is None or tb is None:
        return "-"
    return f"{fb}↔{tb}"


def _money(val) -> str:
    if val is None:
        return "-"
    return f"${val:,.0f}"


def format_constraints_da_network(vm: dict) -> str:
    """Markdown for ``GET /views/constraints_da_network``."""
    parts: list[str] = []
    parts.append(f"# DA Constraints — Network-Enriched — {vm.get('target_date', '?')}")

    bh = vm.get("binding_hours")
    if bh:
        parts.append(
            f"\n_Funnel mode: filtered to binding HEs {bh}, "
            f"sorted by sum-over-binding-hours shadow price._"
        )

    cov = vm.get("match_coverage", {})
    parts.append(
        f"\n**Match coverage**: {cov.get('matched', 0) + cov.get('ambiguous', 0)} / "
        f"{cov.get('total', 0) - cov.get('interface', 0)} branch-class "
        f"({cov.get('match_rate_pct', 0)}%) — "
        f"{cov.get('matched', 0)} unique, "
        f"{cov.get('ambiguous', 0)} multi-match, "
        f"{cov.get('unmatched', 0)} unmatched, "
        f"{cov.get('interface', 0)} interface (no branch)"
    )

    matched = vm.get("matched_constraints", [])
    if matched:
        sort_label = "binding HE price" if bh else "total price"
        parts.append(f"\n## Matched ({len(matched)}) — sorted by {sort_label}")
        parts.append(_constraint_da_table(matched, with_neighbors=True, binding_hours=bh))

    ambiguous = vm.get("ambiguous_constraints", [])
    if ambiguous:
        parts.append(
            f"\n## Ambiguous ({len(ambiguous)}) — first PSS/E candidate shown"
        )
        parts.append(_constraint_da_table(ambiguous, with_neighbors=True, binding_hours=bh))

    unmatched = vm.get("unmatched_constraints", [])
    if unmatched:
        parts.append(
            f"\n## Unmatched ({len(unmatched)}) — facility not found in PSS/E"
        )
        parts.append(_constraint_unmatched_table(unmatched, market="da"))

    interface = vm.get("interface_constraints", [])
    if interface:
        parts.append(
            f"\n## Interface / Zone ({len(interface)}) — no branch match attempted"
        )
        parts.append(_constraint_unmatched_table(interface, market="da"))

    return "\n".join(parts)


def format_constraints_rt_dart_network(vm: dict) -> str:
    """Markdown for ``GET /views/constraints_rt_dart_network``."""
    parts: list[str] = []
    parts.append(
        f"# RT + DART Constraints — Network-Enriched — "
        f"{vm.get('start_date', '?')} → {vm.get('end_date', '?')}"
    )

    cov = vm.get("match_coverage", {})
    parts.append(
        f"\n**Match coverage**: {cov.get('matched', 0) + cov.get('ambiguous', 0)} / "
        f"{cov.get('total', 0) - cov.get('interface', 0)} branch-class "
        f"({cov.get('match_rate_pct', 0)}%) — "
        f"{cov.get('matched', 0)} unique, "
        f"{cov.get('ambiguous', 0)} multi-match, "
        f"{cov.get('unmatched', 0)} unmatched, "
        f"{cov.get('interface', 0)} interface — "
        f"sorted by `|DART total|` desc"
    )

    matched = vm.get("matched_constraints", [])
    if matched:
        parts.append(f"\n## Matched ({len(matched)})")
        parts.append(_constraint_rt_dart_table(matched, with_neighbors=True))

    ambiguous = vm.get("ambiguous_constraints", [])
    if ambiguous:
        parts.append(
            f"\n## Ambiguous ({len(ambiguous)}) — first PSS/E candidate shown"
        )
        parts.append(_constraint_rt_dart_table(ambiguous, with_neighbors=True))

    unmatched = vm.get("unmatched_constraints", [])
    if unmatched:
        parts.append(f"\n## Unmatched ({len(unmatched)})")
        parts.append(_constraint_unmatched_table(unmatched, market="rt_dart"))

    interface = vm.get("interface_constraints", [])
    if interface:
        parts.append(f"\n## Interface / Zone ({len(interface)})")
        parts.append(_constraint_unmatched_table(interface, market="rt_dart"))

    return "\n".join(parts)


def _constraint_da_table(
    rows: list[dict], *, with_neighbors: bool,
    binding_hours: list[int] | None = None,
) -> str:
    if binding_hours:
        # Funnel mode — replace OnPk/OffPk $ cols with Binding HE $ summary
        headers = [
            "Constraint", "Contingency", "kV", "Route", "Buses",
            "Total $", "Binding HE $", "Hrs Bound", "MVA",
        ]
        if with_neighbors:
            headers.append("Top Neighbors")
        out = []
        for r in rows:
            bh = r.get("hourly_binding") or {}
            sum_str = (
                f"{_money(r.get('binding_price'))} "
                f"(HE{','.join(str(h) for h in sorted(bh.keys()))})"
                if bh else "-"
            )
            row = [
                (r.get("constraint_name") or "-")[:30],
                (r.get("contingency") or "-")[:30],
                r.get("parsed_voltage_kv") or "-",
                _constraint_route(r),
                _bus_pair(r),
                _money(r.get("da_total_price")),
                sum_str,
                r.get("binding_hours_bound") or "-",
                f"{r['rating_mva']:,.0f}" if r.get("rating_mva") else "-",
            ]
            if with_neighbors:
                # In funnel mode neighbors are dropped, so show bus IDs instead
                ids = r.get("neighbor_bus_ids") or []
                row.append(", ".join(str(b) for b in ids[:5]) if ids else "-")
            out.append(row)
        return _table(headers, out)

    # Default mode — unchanged
    headers = [
        "Constraint", "Contingency", "kV", "Route", "Buses",
        "Total $", "Hrs", "OnPk $", "OffPk $", "MVA",
    ]
    if with_neighbors:
        headers.append("Top Neighbors")
    out = []
    for r in rows:
        row = [
            (r.get("constraint_name") or "-")[:30],
            (r.get("contingency") or "-")[:30],
            r.get("parsed_voltage_kv") or "-",
            _constraint_route(r),
            _bus_pair(r),
            _money(r.get("da_total_price")),
            r.get("da_total_hours") or "-",
            _money(r.get("da_onpeak_price")),
            _money(r.get("da_offpeak_price")),
            f"{r['rating_mva']:,.0f}" if r.get("rating_mva") else "-",
        ]
        if with_neighbors:
            row.append(_format_neighbors(r.get("neighbors", [])))
        out.append(row)
    return _table(headers, out)


def _constraint_rt_dart_table(rows: list[dict], *, with_neighbors: bool) -> str:
    headers = [
        "Date", "Constraint", "Contingency", "kV", "Route", "Buses",
        "RT $", "RT Hrs", "DART $", "DART Hrs", "MVA",
    ]
    if with_neighbors:
        headers.append("Top Neighbors")
    out = []
    for r in rows:
        row = [
            r.get("date") or "-",
            (r.get("constraint_name") or "-")[:28],
            (r.get("contingency") or "-")[:28],
            r.get("parsed_voltage_kv") or "-",
            _constraint_route(r),
            _bus_pair(r),
            _money(r.get("rt_total_price")),
            r.get("rt_total_hours") or "-",
            _money(r.get("dart_total_price")),
            r.get("dart_total_hours") or "-",
            f"{r['rating_mva']:,.0f}" if r.get("rating_mva") else "-",
        ]
        if with_neighbors:
            row.append(_format_neighbors(r.get("neighbors", [])))
        out.append(row)
    return _table(headers, out)


# ─── DA LMPs — hub summary + outage overlap ──────────────────────────────────


def _money_d(val) -> str:
    """$X.XX with cents — LMP-scale prices (vs constraint-scale ``_money``)."""
    if val is None:
        return "-"
    return f"${val:,.2f}"


def _pct(val) -> str:
    if val is None:
        return "-"
    return f"{val * 100:.0f}%"


def format_lmp_da_hub_summary(vm: dict) -> str:
    """Markdown for ``GET /views/lmp_da_hub_summary``."""
    parts: list[str] = []
    parts.append(f"# DA LMP — Hub Summary — {vm.get('target_date', '?')}")

    if "error" in vm and not vm.get("hubs"):
        parts.append(f"\n_{vm['error']}_")
        return "\n".join(parts)

    parts.append(
        f"\n**{vm.get('hub_count', 0)} hubs** × {vm.get('hour_count', 0)} hours · "
        f"**{vm.get('high_congestion_count', 0)}** with onpeak |congestion| > "
        f"{vm.get('high_congestion_threshold', 0.10) * 100:.0f}% of total"
    )

    mo = vm.get("market_avg_onpeak") or {}
    mf = vm.get("market_avg_offpeak") or {}
    parts.append(
        f"\n**Market avg (onpeak)**: total {_money_d(mo.get('total'))} = "
        f"energy {_money_d(mo.get('energy'))} + congestion {_money_d(mo.get('congestion'))} + "
        f"loss {_money_d(mo.get('loss'))}  ·  "
        f"**(offpeak)**: total {_money_d(mf.get('total'))}, "
        f"congestion {_money_d(mf.get('congestion'))}"
    )

    hubs = vm.get("hubs") or []
    if hubs:
        parts.append("\n## Hubs — sorted by |onpeak congestion|")
        headers = [
            "Hub", "OnPk Total", "OnPk Energy", "OnPk Cong", "Cong %",
            "OffPk Total", "OffPk Cong",
            "Peak HE", "Peak Total", "Peak Cong",
        ]
        rows = []
        for h in hubs:
            rows.append([
                h.get("hub", "-"),
                _money_d(h.get("onpeak_total")),
                _money_d(h.get("onpeak_energy")),
                _money_d(h.get("onpeak_congestion")),
                _pct(h.get("congestion_pct_of_total")),
                _money_d(h.get("offpeak_total")),
                _money_d(h.get("offpeak_congestion")),
                h.get("peak_hour") or "-",
                _money_d(h.get("peak_total")),
                _money_d(h.get("peak_congestion")),
            ])
        parts.append(_table(headers, rows))

    return "\n".join(parts)


def format_lmp_da_outage_overlap(vm: dict) -> str:
    """Markdown for ``GET /views/lmp_da_outage_overlap``."""
    parts: list[str] = []
    parts.append(f"# DA Constraints × Outage Overlap — {vm.get('target_date', '?')}")

    if "error" in vm and not vm.get("constraints"):
        parts.append(f"\n_{vm['error']}_")
        return "\n".join(parts)

    parts.append(
        f"\nTop **{vm.get('constraint_count', 0)}** binding DA constraints · "
        f"**{vm.get('with_overlap_count', 0)}** have an outage overlap on the "
        f"seed branch or a 2-hop ≥230 kV neighbor "
        f"(window: target ± {vm.get('window_days', 7)}d)"
    )

    constraints = vm.get("constraints") or []
    if not constraints:
        parts.append("\n_No matched constraints._")
        return "\n".join(parts)

    # Summary table — one row per top constraint
    parts.append("\n## Constraints")
    sum_headers = [
        "Constraint", "Contingency", "kV", "Route", "Buses",
        "Total $", "Hrs", "Nbrs", "Active", "Starting", "Ending",
    ]
    sum_rows = []
    for c in constraints:
        sum_rows.append([
            (c.get("constraint_name") or "-")[:30],
            (c.get("contingency") or "-")[:25],
            c.get("parsed_voltage_kv") or "-",
            _constraint_route(c),
            _bus_pair(c),
            _money(c.get("total_price")),
            c.get("total_hours") or "-",
            c.get("neighbor_count_k2_hv") or 0,
            c.get("active_count") or "-",
            c.get("starting_soon_count") or "-",
            c.get("ending_soon_count") or "-",
        ])
    parts.append(_table(sum_headers, sum_rows))

    # Per-constraint detail — only those with overlap
    overlap_constraints = [c for c in constraints if c.get("outage_overlap")]
    if overlap_constraints:
        parts.append(f"\n## Outage Detail ({len(overlap_constraints)})")
        for c in overlap_constraints:
            cname = c.get("constraint_name") or "?"
            parts.append(
                f"\n### {cname} — "
                f"{_constraint_route(c)} · {_bus_pair(c)} · "
                f"{_money(c.get('total_price'))} · "
                f"{c.get('total_hours') or 0}h"
            )
            o_headers = [
                "Bucket", "On", "Branch", "Facility", "kV",
                "State", "Risk", "Start", "End", "Days→Ret",
            ]
            o_rows = []
            for o in c["outage_overlap"]:
                o_rows.append([
                    o.get("bucket"),
                    o.get("on_branch"),
                    (o.get("branch_label") or "-")[:30],
                    (o.get("facility") or "")[:38],
                    o.get("kv") or "-",
                    o.get("outage_state") or "-",
                    "Yes" if o.get("risk_flag") else "-",
                    o.get("started") or "-",
                    o.get("est_return") or "-",
                    o.get("days_to_return") if o.get("days_to_return") is not None else "-",
                ])
            parts.append(_table(o_headers, o_rows))
    else:
        parts.append("\n_No outage overlaps in window._")

    return "\n".join(parts)


def _constraint_unmatched_table(rows: list[dict], *, market: str) -> str:
    """Compact table for unmatched / interface rows. ``market`` controls
    whether DA or RT+DART metrics are shown."""
    if market == "da":
        headers = ["Constraint", "Contingency", "kV", "Total $", "Hrs", "Dialect"]
    else:
        headers = ["Date", "Constraint", "Contingency", "kV", "RT $", "DART $", "Dialect"]
    out = []
    for r in rows:
        if market == "da":
            out.append([
                (r.get("constraint_name") or "-")[:35],
                (r.get("contingency") or "-")[:30],
                r.get("parsed_voltage_kv") or "-",
                _money(r.get("da_total_price")),
                r.get("da_total_hours") or "-",
                r.get("parser_dialect") or "?",
            ])
        else:
            out.append([
                r.get("date") or "-",
                (r.get("constraint_name") or "-")[:32],
                (r.get("contingency") or "-")[:28],
                r.get("parsed_voltage_kv") or "-",
                _money(r.get("rt_total_price")),
                _money(r.get("dart_total_price")),
                r.get("parser_dialect") or "?",
            ])
    return _table(headers, out)


# ─── Tier 1 — DA LMP daily summary ───────────────────────────────────────────


def format_lmps_daily_summary(vm: dict) -> str:
    """Markdown for ``GET /views/lmps_daily_summary``."""
    parts: list[str] = [format_lmp_da_hub_summary(vm)]
    drilldown = vm.get("top_zones_for_drilldown") or []
    if drilldown:
        parts.append(
            f"\n_Tier 2 deep-dive: **{', '.join(drilldown)}** "
            f"(top {len(drilldown)} hubs by |onpeak congestion|)._"
        )
    return "\n".join(parts)


# ─── Tier 2 — DA LMP hourly drilldown (heatmap) ──────────────────────────────


def _heatmap_glyph(cong: float | None) -> str:
    """Single-char intensity glyph for a congestion price."""
    if cong is None:
        return " "
    if cong < 0:
        return "."
    a = abs(cong)
    if a < 10:
        return "."
    if a < 25:
        return "-"
    if a < 50:
        return "+"
    return "#"


def format_lmps_hourly_summary(vm: dict) -> str:
    """Markdown for ``GET /views/lmps_hourly_summary``."""
    parts: list[str] = []
    parts.append(f"# DA LMP — Hourly Drilldown — {vm.get('target_date', '?')}")

    if vm.get("error") and not vm.get("hub_hour_grid"):
        parts.append(f"\n_{vm['error']}_")
        return "\n".join(parts)

    threshold = vm.get("binding_threshold_usd", 25.0)
    binding = vm.get("binding_hours_for_drilldown") or []
    parts.append(
        f"\n**{vm.get('hub_count', 0)} hubs** × {vm.get('hour_count', 0)} hours · "
        f"binding threshold ${threshold:.0f}/MWh · "
        f"**{len(binding)} binding hours**"
    )

    callout = vm.get("peak_hour_callout") or []
    if callout:
        parts.append("\n## Peak-hour callout")
        for c in callout:
            parts.append(
                f"- HE {c.get('hour_ending')} — max |cong| "
                f"{_money_d(c.get('max_abs_congestion'))} at "
                f"{c.get('hub')} · mean across hubs "
                f"{_money_d(c.get('mean_abs_congestion_across_hubs'))} · "
                f"{c.get('hubs_with_congestion_gt_threshold')} hubs > ${threshold:.0f}"
            )

    grid = vm.get("hub_hour_grid") or []
    if grid:
        # Build pivot: rows = hubs, cols = HE 1..24, cells = glyph based on congestion
        hubs_order = vm.get("hubs") or sorted({r["hub"] for r in grid})
        by_hub_he: dict[tuple[str, int], dict] = {}
        for r in grid:
            he = r.get("hour_ending")
            if he is None:
                continue
            by_hub_he[(r["hub"], int(he))] = r

        parts.append(
            "\n## Congestion heatmap "
            "(`. <$10`  `- <$25`  `+ <$50`  `# ≥$50`, `.` for negative)"
        )
        # Header line
        hour_hdr = " ".join(f"{h:02d}" for h in range(1, 25))
        parts.append(f"\n```\n{'Hub':<22} {hour_hdr}")
        for hub in hubs_order:
            cells = []
            for h in range(1, 25):
                cell = by_hub_he.get((hub, h))
                cong = cell.get("lmp_congestion_price") if cell else None
                cells.append(_heatmap_glyph(cong))
            parts.append(f"{hub[:22]:<22} {' '.join(c.rjust(2) for c in cells)}")
        parts.append("```")

    per_hub = vm.get("per_hub_summary") or []
    if per_hub:
        parts.append("\n## Per-hub summary")
        headers = ["Hub", "Max |Cong|", "@ HE", "Mean Cong", "Hrs > Thresh"]
        rows = [
            [
                p.get("hub"),
                _money_d(p.get("max_abs_congestion")),
                p.get("max_abs_hour") or "-",
                _money_d(p.get("mean_congestion")),
                p.get("binding_hours_count") or 0,
            ]
            for p in per_hub
        ]
        parts.append(_table(headers, rows))

    if binding:
        parts.append(
            f"\n_Tier 3 deep-dive: binding HEs **{binding}**._"
        )

    return "\n".join(parts)


# ─── Tier 4 — outages for constraints ────────────────────────────────────────


def format_transmission_outages_for_constraints(vm: dict) -> str:
    """Markdown for ``GET /views/transmission_outages_for_constraints``."""
    parts: list[str] = []
    parts.append(
        f"# Transmission Outages — On/Near Constraint Buses — "
        f"{vm.get('reference_date', '?')}"
    )

    parts.append(
        f"\n**{vm.get('matched_count', 0)}** outages on or adjacent to the "
        f"**{vm.get('constraint_bus_count', 0)}** constraint bus IDs · "
        f"({vm.get('total_active', 0)} active outages scanned)"
    )

    outages = vm.get("outages") or []
    if not outages:
        parts.append("\n_No active outages on the supplied constraint bus set._")
        return "\n".join(parts)

    headers = [
        "Region", "Facility", "Type", "kV", "Route",
        "From Bus", "To Bus", "State",
        "Started", "Days Out", "Days Left",
        "Near Constraint",
    ]
    rows = []
    for o in outages:
        labels = o.get("near_constraint_labels") or []
        if labels:
            near = "; ".join(labels[:2])  # cap to 2 for table width
        else:
            near = ", ".join(str(b) for b in (o.get("near_constraint_buses") or []))
        rows.append([
            o.get("region", "-"),
            (o.get("facility") or "")[:36],
            o.get("equip_category", o.get("equip", "")),
            o.get("kv") or "-",
            _route(o),
            o.get("from_bus_psse", "-"),
            o.get("to_bus_psse", "-"),
            o.get("outage_state", "-"),
            o.get("started", "-"),
            o.get("days_out") if o.get("days_out") is not None else "-",
            o.get("days_to_return") if o.get("days_to_return") is not None else "overdue",
            (near or "-")[:40],
        ])
    parts.append(_table(headers, rows))

    return "\n".join(parts)
