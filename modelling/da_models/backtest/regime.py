"""Regime classifiers for the leaderboard slices.

Attach as columns on the tall replay frame, then pass the column names
to ``metrics.point.point_metrics_by_model(group_by=...)`` /
``metrics.quantile.quantile_metrics_by_model(group_by=...)``. v1 keeps
this small: day-type (weekday / weekend / NERC holiday), block
(OnPeak HE8-23 / OffPeak), and a coarse net-load tier IF the row has a
``utilization`` column (supply_stack writes it).

Future enhancements (kept out of v1 to stay focused):
  - ``scarcity_flag`` from the reserve-market mart per (date, HE)
  - season (summer / shoulder / winter) from ``pjm_dates_daily``
  - load-tier bands from ``pjm_supply_demand_coalesced``
"""

from __future__ import annotations

import pandas as pd

from da_models.common.calendar import compute_calendar_row

_ONPEAK_HOURS: set[int] = set(range(8, 24))  # HE8..HE23 (PJM convention)


def _row_day_type(d) -> str:
    cal = compute_calendar_row(d)
    if cal["is_nerc_holiday"]:
        return "holiday"
    if cal["is_weekend"]:
        return "weekend"
    return "weekday"


def attach_day_type(df: pd.DataFrame) -> pd.DataFrame:
    """Add a ``day_type`` column: weekday / weekend / holiday."""
    out = df.copy()
    out["day_type"] = out["target_date"].map(_row_day_type)
    return out


def attach_block(df: pd.DataFrame) -> pd.DataFrame:
    """Add a ``block`` column: ``OnPeak`` (HE8-23) or ``OffPeak`` (HE1-7, HE24)."""
    out = df.copy()
    out["block"] = out["hour_ending"].map(
        lambda h: "OnPeak" if int(h) in _ONPEAK_HOURS else "OffPeak"
    )
    return out


def attach_all_default(df: pd.DataFrame) -> pd.DataFrame:
    """Convenience: apply both default classifiers."""
    return attach_block(attach_day_type(df))
