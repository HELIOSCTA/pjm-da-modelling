"""Markdown formatter for the transmission outages view model."""
from __future__ import annotations

from tabulate import tabulate


def _table(headers: list[str], rows: list[list], floatfmt: str = ".2f") -> str:
    """Render a markdown pipe table via tabulate."""
    return tabulate(rows, headers=headers, tablefmt="pipe", numalign="right")


def format_transmission_outages(vm: dict) -> str:
    """Format the transmission outages view model as markdown.

    Two sections: regional summary table and notable individual outages.
    """
    if "error" in vm:
        return f"# Error\n\n{vm['error']}"

    parts: list[str] = []
    parts.append(f"# Transmission Outages — {vm.get('reference_date', '?')}")
    parts.append(f"\nActive/Approved >=230 kV: **{vm.get('total_active', '?')}** outages")

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
            if n.get("from_station") and n.get("to_station"):
                route = f"{n['from_station']}→{n['to_station']}"
            elif n.get("station"):
                route = n["station"]
            else:
                route = "-"

            rows.append([
                ", ".join(n["tags"]),
                n["region"],
                n.get("facility", "")[:40],
                n.get("equip_category", n.get("equip", "")),
                n["kv"],
                route,
                n.get("started", "-"),
                n.get("est_return", "-"),
                n.get("days_out", "-"),
                n.get("days_to_return") if n.get("days_to_return") is not None else "overdue",
                n.get("cause", "")[:35],
            ])
        parts.append(_table(headers, rows))

    cancelled = vm.get("recently_cancelled", [])
    if cancelled:
        parts.append(f"\n## Recently Cancelled ({len(cancelled)}, last 7 days)")
        headers = [
            "Region", "Facility", "Type", "kV", "Route",
            "Was Sched Start", "Was Sched End", "Cancelled", "Cause",
        ]
        rows = []
        for c in cancelled:
            if c.get("from_station") and c.get("to_station"):
                route = f"{c['from_station']}→{c['to_station']}"
            elif c.get("station"):
                route = c["station"]
            else:
                route = "-"

            rows.append([
                c["region"],
                c.get("facility", "")[:40],
                c.get("equip_category", c.get("equip", "")),
                c["kv"],
                route,
                c.get("was_scheduled_start", "-"),
                c.get("was_scheduled_end", "-"),
                c.get("cancelled_date", "-"),
                c.get("cause", "")[:35],
            ])
        parts.append(_table(headers, rows))

    return "\n".join(parts)
