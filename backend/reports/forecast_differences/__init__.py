"""7-day load forecast diff report — latest run vs ~12h / 24h / 48h prior.

Pulls hourly forecast points from
`pjm.seven_day_load_forecast_v1_2025_08_13` for the last 2 days of run
vintages, groups by (forecast_area, forecast_date), and renders one
section per forecast date: a snapshot table (all runs + delta rows) and
a comparison plot of the latest curve against the 12h / 24h / 48h
anchors.

Usage::

    python -m backend.reports.forecast_differences
    python -m backend.reports.forecast_differences --areas RTO_COMBINED
    python -m backend.reports.forecast_differences --lookback 3
"""
from __future__ import annotations

import sys

from backend.reports._runner import Fragment, render
from backend.reports.forecast_differences.fragments import (
    DEFAULT_AREAS,
    build_fragments,
)


def run(
    forecast_areas: list[str] | None = None,
    eval_lookback_days: int = 2,
) -> str:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    areas = list(forecast_areas or DEFAULT_AREAS)

    sections: list[str | Fragment] = build_fragments(
        forecast_areas=areas,
        eval_lookback_days=eval_lookback_days,
    )

    from datetime import date as _date
    output_name = f"forecast_differences_{_date.today()}.html"
    out_path = render(
        title=f"PJM 7-Day Load Forecast Diffs - {_date.today()}",
        output_name=output_name,
        sections=sections,
    )
    print(f"Wrote {out_path}")
    return out_path


__all__ = ["run", "DEFAULT_AREAS"]
