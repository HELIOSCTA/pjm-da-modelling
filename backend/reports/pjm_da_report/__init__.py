"""PJM DA-release report — tomorrow's cleared day-ahead market.

Fixed to tomorrow (today + 1) by default. Run after PJM clears DA
(~13:00 EPT); the loader raises ValueError if tomorrow isn't in the
parquet yet.

Usage::

    python -m backend.reports.pjm_da_report

Bundles included today:
- lmp                    Per-hub block: Total / System / Congestion charts + pivoted HE table
- da_constraints         Single flat constraints table, sorted by |total $| desc
- transmission_outages   Active >=230 kV outages, returning-soon, network-matched
"""
from __future__ import annotations

import sys
from datetime import date, timedelta

from backend.reports._runner import Fragment, render
from backend.reports.pjm_da_report.da_constraints import fragments as da_constraints_fragments
from backend.reports.pjm_da_report.lmp import fragments as lmp_fragments
from backend.reports.pjm_da_report.transmission_outages import (
    fragments as transmission_outages_fragments,
)


def run(target_date: date | None = None) -> str:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    target = target_date or date.today() + timedelta(days=1)

    sections: list[str | Fragment] = []
    sections += lmp_fragments.build_fragments(target_date=target)
    sections += da_constraints_fragments.build_fragments(target_date=target)
    sections += transmission_outages_fragments.build_fragments(target_date=target)

    out_path = render(
        title=f"PJM DA Release - {target}",
        output_name=f"pjm_da_report_{target}.html",
        sections=sections,
    )
    print(f"Wrote {out_path}")
    return out_path


__all__ = ["run"]
