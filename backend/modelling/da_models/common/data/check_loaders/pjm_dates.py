"""Print the PJM calendar loader as a wide table.

Mirrors the calendar metadata frame consumed by the like_day_model_knn
filters (``apply_calendar_filter``) and surfaced in the Streamlit
"Modelling Inputs > Run / Candidates" pages.

One row per delivery date with day-of-week (PJM Sun=0..Sat=6), the
weekend / holiday flags, summer/winter season, and the NERC holiday
name (when present). Future-dated rows are expected — the calendar
parquet is forward-projected through the end of the contract year so
the model can ask about target dates the load forecast hasn't covered
yet.

Sections:
  - recent N days table (default 60)
  - NERC holiday roster within the window
  - forward-calendar tail (rows beyond today)
  - sanity checks: date-sequence gaps, weekend/dow mismatch, holidays
    flagged but missing a name

Usage::

    python -m backend.modelling.da_models.common.data.check_loaders.pjm_dates
    python modelling/da_models/common/data/check_loaders/pjm_dates.py
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[6]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.utils.logging_utils import init_logging, print_header, print_section  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ────────────────────────
CACHE_DIR: Path | None = None
LOOKBACK_DAYS: int | None = 60         # set to None to print all dates
FORWARD_TAIL_ROWS: int = 10            # rows to show beyond today
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"

# PJM convention: Sun=0..Sat=6 (matches pjm_dates_daily.day_of_week_number).
_DOW_LABELS: dict[int, str] = {
    0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat",
}
_PJM_WEEKEND_DOW: set[int] = {0, 6}

ORDERED_COLS: list[str] = [
    "Date", "DOW", "DOW#",
    "Weekend", "NERC Hol", "Fed Hol",
    "Season", "Holiday Name",
]

_FORMATTERS: dict = {
    "Date": str,
    "DOW#": lambda v: "" if pd.isna(v) else f"{int(v):>4d}",
    "Weekend": lambda v: "Y" if int(v) == 1 else "",
    "NERC Hol": lambda v: "Y" if int(v) == 1 else "",
    "Fed Hol": lambda v: "Y" if int(v) == 1 else "",
    "Holiday Name": lambda v: "" if pd.isna(v) or v == "" else str(v),
}


def _to_wide(dates_df: pd.DataFrame) -> pd.DataFrame:
    """Project the normalized calendar frame to the display column set."""
    if dates_df.empty:
        return pd.DataFrame(columns=ORDERED_COLS)

    out = pd.DataFrame({
        "Date": dates_df["date"],
        "DOW#": dates_df["day_of_week_number"],
    })
    out["DOW"] = out["DOW#"].map(
        lambda v: _DOW_LABELS.get(int(v), "?") if pd.notna(v) else "?"
    )
    out["Weekend"] = dates_df.get("is_weekend", 0)
    out["NERC Hol"] = dates_df.get("is_nerc_holiday", 0)
    out["Fed Hol"] = dates_df.get("is_federal_holiday", 0)
    out["Season"] = dates_df.get("summer_winter", "")
    out["Holiday Name"] = dates_df.get("holiday_name", pd.Series(dtype="object"))
    return out[ORDERED_COLS].reset_index(drop=True)


def build_pjm_dates_table(
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
    today: date | None = None,
) -> pd.DataFrame:
    """Return the wide PJM calendar table, sorted Date desc.

    ``lookback_days`` trims the frame to the N days ending at ``today``
    (default: actual today), while still keeping any forward-dated rows
    so callers can verify forward-calendar coverage. ``None`` returns
    every row.

    Columns: Date | DOW | DOW# | Weekend | NERC Hol | Fed Hol | Season | Holiday Name.
    """
    df = loader.load_pjm_dates_daily(cache_dir=cache_dir)
    if df.empty:
        return _to_wide(df)

    if lookback_days is not None:
        anchor = today or date.today()
        cutoff = anchor - timedelta(days=lookback_days - 1)
        df = df[df["date"] >= cutoff]

    return _to_wide(df).sort_values("Date", ascending=False).reset_index(drop=True)


def _print_recent_window(pl, table: pd.DataFrame) -> None:
    print_section("recent window")
    if table.empty:
        pl.warning("No calendar rows in window.")
        return
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", None,
    ):
        print(table.to_string(index=False, formatters=_FORMATTERS))


def _print_holiday_roster(pl, table: pd.DataFrame) -> None:
    print_section("NERC holidays in window")
    holidays = table[table["NERC Hol"].astype(int) == 1]
    if holidays.empty:
        pl.info("No NERC holidays in window.")
        return
    pl.info(f"{len(holidays)} NERC holiday row(s) in window.")
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", None,
    ):
        print(holidays.to_string(index=False, formatters=_FORMATTERS))


def _print_forward_tail(pl, full: pd.DataFrame, today_value: date, n_rows: int) -> None:
    print_section(f"forward calendar tail (next {n_rows})")
    forward = full[full["Date"] > today_value].sort_values("Date").head(n_rows)
    if forward.empty:
        pl.warning(
            "No forward-dated calendar rows. pjm_dates_daily.parquet may be "
            "stale; the like_day model needs forward coverage for target dates."
        )
        return
    pl.info(
        f"forward-calendar coverage: {len(full[full['Date'] > today_value]):,} "
        f"rows beyond {today_value} (last: {full['Date'].max()})."
    )
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", None,
    ):
        print(forward.to_string(index=False, formatters=_FORMATTERS))


def _print_sanity_checks(pl, full: pd.DataFrame) -> None:
    print_section("sanity checks")
    if full.empty:
        pl.warning("Empty calendar frame; skipping sanity checks.")
        return

    # 1. Date-sequence gaps (calendar should be daily and continuous).
    sorted_dates = pd.to_datetime(full["Date"]).sort_values().reset_index(drop=True)
    expected = pd.date_range(sorted_dates.iloc[0], sorted_dates.iloc[-1], freq="D")
    missing = expected.difference(sorted_dates)
    if len(missing) == 0:
        pl.info(
            f"date sequence: continuous, {len(sorted_dates):,} rows from "
            f"{sorted_dates.iloc[0].date()} to {sorted_dates.iloc[-1].date()}."
        )
    else:
        sample = ", ".join(str(d.date()) for d in missing[:10])
        pl.warning(
            f"date sequence: {len(missing)} missing day(s) in expected range. "
            f"Sample: {sample}{' ...' if len(missing) > 10 else ''}"
        )

    # 2. Weekend flag vs day_of_week_number (PJM Sun=0, Sat=6).
    dow_num = full["DOW#"].dropna().astype(int)
    weekend = full.loc[dow_num.index, "Weekend"].astype(int)
    expected_weekend = dow_num.isin(_PJM_WEEKEND_DOW).astype(int)
    mismatch = (weekend != expected_weekend).sum()
    if mismatch == 0:
        pl.info(f"weekend flag: consistent with PJM Sun=0/Sat=6 across {len(dow_num):,} rows.")
    else:
        pl.warning(
            f"weekend flag: {mismatch} row(s) where is_weekend disagrees with "
            "day_of_week_number under PJM Sun=0/Sat=6 convention."
        )

    # 3. NERC holiday flagged but holiday_name empty.
    flagged = full[full["NERC Hol"].astype(int) == 1]
    nameless = flagged[flagged["Holiday Name"].fillna("").astype(str).str.len() == 0]
    if len(nameless) == 0:
        pl.info(f"holiday names: all {len(flagged):,} flagged NERC holidays carry a name.")
    else:
        sample = ", ".join(str(d) for d in nameless["Date"].tolist()[:10])
        pl.warning(
            f"holiday names: {len(nameless)} NERC-flagged date(s) missing a holiday_name. "
            f"Sample: {sample}{' ...' if len(nameless) > 10 else ''}"
        )


def run(
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
    forward_tail_rows: int = FORWARD_TAIL_ROWS,
) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="check_loaders_pjm_dates", log_dir=LOG_DIR)
    try:
        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(f"load_pjm_dates_daily ({lookback_label})")

        with pl.timer("load pjm_dates_daily"):
            full_raw = loader.load_pjm_dates_daily(cache_dir=cache_dir)

        if full_raw.empty:
            pl.warning("Calendar frame is empty; nothing to print.")
            return

        full_wide = (
            _to_wide(full_raw).sort_values("Date", ascending=False).reset_index(drop=True)
        )
        today_value = date.today()

        pl.info(
            f"calendar coverage: {len(full_wide):,} rows | "
            f"{full_wide['Date'].min()} -> {full_wide['Date'].max()} | "
            f"weekend={int(full_wide['Weekend'].sum()):,} "
            f"NERC_holiday={int(full_wide['NERC Hol'].sum()):,} "
            f"federal_holiday={int(full_wide['Fed Hol'].sum()):,}"
        )

        window = build_pjm_dates_table(
            cache_dir=cache_dir,
            lookback_days=lookback_days,
            today=today_value,
        )
        # Trim window to non-future rows for the recent-window section so the
        # forward tail section owns those rows exclusively.
        recent = window[window["Date"] <= today_value]
        _print_recent_window(pl, recent)
        _print_holiday_roster(pl, recent)
        _print_forward_tail(pl, full_wide, today_value, forward_tail_rows)
        _print_sanity_checks(pl, full_wide)

        pl.success(
            f"Printed pjm_dates_daily: {len(recent):,} recent row(s), "
            f"{forward_tail_rows} forward row(s) shown."
        )
    finally:
        pl.close()


if __name__ == "__main__":
    run()
