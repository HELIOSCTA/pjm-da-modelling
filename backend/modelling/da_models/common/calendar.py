"""Calendar-derived features shared by all model families."""

from __future__ import annotations

import math
from datetime import date
from datetime import timedelta

from backend.modelling.da_models.common.configs import DOW_GROUPS

def _observe_fixed_holiday(value: date) -> date:
    if value.weekday() == 5:  # Saturday
        return value - timedelta(days=1)
    if value.weekday() == 6:  # Sunday
        return value + timedelta(days=1)
    return value


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    current += timedelta(days=7 * (n - 1))
    return current


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        current = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        current = date(year, month + 1, 1) - timedelta(days=1)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _nerc_holidays(year: int) -> set[date]:
    new_year = _observe_fixed_holiday(date(year, 1, 1))
    memorial_day = _last_weekday_of_month(year, 5, 0)  # Monday
    independence_day = _observe_fixed_holiday(date(year, 7, 4))
    labor_day = _nth_weekday_of_month(year, 9, 0, 1)  # 1st Monday
    thanksgiving = _nth_weekday_of_month(year, 11, 3, 4)  # 4th Thursday
    christmas = _observe_fixed_holiday(date(year, 12, 25))
    return {
        new_year,
        memorial_day,
        independence_day,
        labor_day,
        thanksgiving,
        christmas,
    }


def _is_nerc_holiday(value: date) -> bool:
    return value in _nerc_holidays(value.year)


def compute_calendar_row(value: date) -> dict[str, object]:
    """Build day-level calendar features for a single date."""
    day_of_week_number = value.weekday()
    angle = (2.0 * math.pi * day_of_week_number) / 7.0

    return {
        "day_of_week_number": day_of_week_number,
        "dow_group": DOW_GROUPS[day_of_week_number],
        "is_weekend": day_of_week_number >= 5,
        "is_nerc_holiday": _is_nerc_holiday(value),
        "dow_sin": math.sin(angle),
        "dow_cos": math.cos(angle),
    }
