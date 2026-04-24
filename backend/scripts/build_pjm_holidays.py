"""Build the PJM DA-modelling holidays seed.

One-shot. Emits every holiday date relevant to PJM load / DA price behavior,
tagged by type so the date marts can expose separate flags:

    nerc    -- NERC bulk electric system holidays (define on/off-peak)
    federal -- US federal holidays NOT in the NERC list
    soft    -- load-depressed days that are neither NERC nor federal
               (Good Friday, Black Friday, Christmas Eve, New Year's Eve)

NERC holiday set:
    New Year's Day, Memorial Day, Independence Day, Labor Day,
    Thanksgiving, Christmas Day (all observed — shifted off weekends).

Usage:
    python backend/scripts/build_pjm_holidays.py
    python backend/scripts/build_pjm_holidays.py --start-year 2010 --end-year 2035

Output:
    backend/dbt/dbt_azure_postgresql/seeds/pjm_holidays.csv
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, timedelta
from pathlib import Path

import holidays

NERC_HOLIDAYS = {
    "New Year's Day",
    "Memorial Day",
    "Independence Day",
    "Labor Day",
    "Thanksgiving Day",
    "Christmas Day",
}

FEDERAL_NON_NERC = {
    "Martin Luther King Jr. Day",
    "Washington's Birthday",          # Presidents' Day
    "Juneteenth National Independence Day",
    "Columbus Day",
    "Veterans Day",
}


def _easter_sunday(year: int) -> date:
    """Meeus/Jones/Butcher Gregorian Easter algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def good_friday(year: int) -> date:
    return _easter_sunday(year) - timedelta(days=2)


def thanksgiving(year: int) -> date:
    nov1 = date(year, 11, 1)
    first_thu = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    return first_thu + timedelta(days=21)  # fourth Thursday


def _federal_observed(d: date) -> date:
    """Apply US federal observance rule: Saturday → Friday, Sunday → Monday."""
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def holidays_for_year(year: int) -> list[tuple[date, str, str]]:
    # Use observed=False to get exactly one entry per holiday (the actual date),
    # then apply the weekend-shift rule ourselves. Avoids the library's dual-row
    # behavior where it emits both actual and observed when they differ.
    us = holidays.US(years=year, observed=False)
    rows: list[tuple[date, str, str]] = []

    for d, name in sorted(us.items()):
        if name in NERC_HOLIDAYS:
            rows.append((_federal_observed(d), "nerc", name))
        elif name in FEDERAL_NON_NERC:
            label = "Presidents Day" if name == "Washington's Birthday" else name
            label = "Juneteenth" if name.startswith("Juneteenth") else label
            rows.append((_federal_observed(d), "federal", label))

    # Soft holidays
    rows.append((good_friday(year), "soft", "Good Friday"))
    rows.append((thanksgiving(year) + timedelta(days=1), "soft", "Black Friday"))
    rows.append((date(year, 12, 24), "soft", "Christmas Eve"))
    rows.append((date(year, 12, 31), "soft", "New Year Eve"))

    return sorted(rows)


def dedupe_by_priority(rows: list[tuple[date, str, str]]) -> list[tuple[date, str, str]]:
    """Collapse rows sharing a date; keep the highest-priority type (nerc > federal > soft).

    Matters for dates like Dec 31 that can be both soft (New Year Eve) and nerc
    (observed New Year's Day when Jan 1 falls on Saturday).
    """
    priority = {"nerc": 0, "federal": 1, "soft": 2}
    best: dict[date, tuple[str, str]] = {}
    for d, t, n in rows:
        cur = best.get(d)
        if cur is None or priority[t] < priority[cur[0]]:
            best[d] = (t, n)
    return sorted((d, t, n) for d, (t, n) in best.items())


def write_seed(rows: list[tuple[date, str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["holiday_date", "holiday_type", "holiday_name"])
        for d, t, n in rows:
            writer.writerow([d.isoformat(), t, n])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year", type=int, default=2035)
    parser.add_argument(
        "--seed-path",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "dbt"
        / "dbt_azure_postgresql"
        / "seeds"
        / "pjm_holidays.csv",
    )
    args = parser.parse_args()

    rows: list[tuple[date, str, str]] = []
    for year in range(args.start_year, args.end_year + 1):
        rows.extend(holidays_for_year(year))

    rows = dedupe_by_priority(rows)
    write_seed(rows, args.seed_path)
    print(f"Wrote {len(rows)} holidays ({args.start_year}-{args.end_year}) -> {args.seed_path}")


if __name__ == "__main__":
    main()
