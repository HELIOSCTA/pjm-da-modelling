"""Composable input-validation checks for the DA forecast preflight.

Each ``check_*`` function inspects a loaded frame and returns a
:class:`CheckResult` — it never raises for "data is bad" (that's the result's
job); it only raises if *called wrong* (e.g. a column it was told to inspect
doesn't exist), and the runner turns even that into an ERROR result.

Two severities:

  - ``ERROR`` — the forecast cannot be trusted; the preflight aborts.
  - ``WARN``  — suspicious but survivable (e.g. a stale-but-present vintage);
    printed, does not abort.

Sanity-bound constants are intentionally wide — they catch "the mart went to
zero / NaN / negative-a-million", not "this number looks a bit high". Tighten
later if a real bug slips through.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import Sequence

import pandas as pd

# ── Wide sanity bounds ─────────────────────────────────────────────────────
DA_LMP_MIN_USD: float = -150.0
DA_LMP_MAX_USD: float = 3_000.0
LOAD_MW_MIN: float = 0.0
LOAD_MW_MAX: float = 250_000.0
# Net load = load minus reported solar/wind, so it can dip below zero on a
# sunny low-load hour but not by a wild margin.
NET_LOAD_MW_MIN: float = -50_000.0
NET_LOAD_MW_MAX: float = 250_000.0

# A DA-cutoff vintage's as_of_date this many days behind the reference date is
# a WARN, not an ERROR — the data is there, it's just older than expected.
FRESHNESS_WARN_DAYS: int = 3

HOURS_PER_DAY: int = 24


class CheckStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    ERROR = "ERROR"


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one check: a stable name, a severity, a human detail line."""

    name: str
    status: CheckStatus
    detail: str

    @property
    def failed(self) -> bool:
        """True only for ERROR — WARN does not count as a failure."""
        return self.status is CheckStatus.ERROR


def _ok(name: str, detail: str) -> CheckResult:
    return CheckResult(name=name, status=CheckStatus.PASS, detail=detail)


def _warn(name: str, detail: str) -> CheckResult:
    return CheckResult(name=name, status=CheckStatus.WARN, detail=detail)


def _error(name: str, detail: str) -> CheckResult:
    return CheckResult(name=name, status=CheckStatus.ERROR, detail=detail)


def _require_columns(df: pd.DataFrame, columns: Sequence[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(
            f"frame is missing expected column(s) {missing}; has {list(df.columns)}"
        )


def _rows_for_date(df: pd.DataFrame, target_date: date, date_col: str) -> pd.DataFrame:
    series = pd.to_datetime(df[date_col], errors="coerce").dt.date
    return df[series == target_date]


# ── Checks ─────────────────────────────────────────────────────────────────
def check_frame_non_empty(name: str, df: pd.DataFrame) -> CheckResult:
    """ERROR if the loaded frame has no rows at all (mart missing / empty)."""
    if df is None or df.empty:
        return _error(name, "loaded frame is empty (mart missing or not built)")
    return _ok(name, f"{len(df):,} row(s)")


def check_target_date_present(
    name: str,
    df: pd.DataFrame,
    target_date: date,
    *,
    date_col: str = "date",
) -> CheckResult:
    """ERROR if there are no rows for ``target_date``."""
    _require_columns(df, [date_col])
    rows = _rows_for_date(df, target_date, date_col)
    if rows.empty:
        present = pd.to_datetime(df[date_col], errors="coerce").dt.date
        latest = present.max() if not present.dropna().empty else None
        return _error(
            name,
            f"no rows for target_date {target_date} (latest date in frame: {latest})",
        )
    return _ok(name, f"{len(rows):,} row(s) for {target_date}")


def check_row_count_per_day(
    name: str,
    df: pd.DataFrame,
    target_date: date,
    *,
    expected: int = HOURS_PER_DAY,
    date_col: str = "date",
    hour_col: str = "hour_ending",
    group_cols: Sequence[str] | None = None,
) -> CheckResult:
    """ERROR unless ``target_date`` has exactly ``expected`` distinct hours.

    ``group_cols`` (e.g. ``("region",)``) checks the count *per group* so a
    multi-region frame must be complete for every region.
    """
    _require_columns(df, [date_col, hour_col])
    rows = _rows_for_date(df, target_date, date_col)
    if rows.empty:
        return _error(name, f"no rows for target_date {target_date}")
    if group_cols:
        _require_columns(df, list(group_cols))
        bad: list[str] = []
        for keys, grp in rows.groupby(list(group_cols)):
            n = grp[hour_col].nunique()
            if n != expected:
                bad.append(f"{keys}={n}")
        if bad:
            return _error(
                name,
                f"expected {expected} hours per {list(group_cols)} on {target_date}; "
                f"off groups: {', '.join(bad)}",
            )
        return _ok(
            name,
            f"{expected} hours x {rows.groupby(list(group_cols)).ngroups} group(s)",
        )
    n = rows[hour_col].nunique()
    if n != expected:
        return _error(
            name, f"expected {expected} distinct hours on {target_date}, found {n}"
        )
    return _ok(name, f"{expected} distinct hours on {target_date}")


def check_no_all_nan(
    name: str,
    df: pd.DataFrame,
    value_cols: Sequence[str],
    *,
    target_date: date | None = None,
    date_col: str = "date",
) -> CheckResult:
    """ERROR if every value in any of ``value_cols`` is NaN.

    Scoped to ``target_date`` when given, else the whole frame.
    """
    _require_columns(df, list(value_cols))
    scope = _rows_for_date(df, target_date, date_col) if target_date else df
    if scope.empty:
        return _error(name, f"no rows to inspect (target_date {target_date})")
    dead = [c for c in value_cols if scope[c].isna().all()]
    if dead:
        return _error(name, f"column(s) entirely NaN: {dead}")
    return _ok(name, f"all of {list(value_cols)} have at least one non-NaN value")


def check_value_range(
    name: str,
    df: pd.DataFrame,
    value_col: str,
    *,
    low: float,
    high: float,
    target_date: date | None = None,
    date_col: str = "date",
) -> CheckResult:
    """ERROR if any non-NaN value in ``value_col`` falls outside [low, high]."""
    _require_columns(df, [value_col])
    scope = _rows_for_date(df, target_date, date_col) if target_date else df
    vals = pd.to_numeric(scope[value_col], errors="coerce").dropna()
    if vals.empty:
        return _warn(name, f"{value_col}: no numeric values to range-check")
    out = vals[(vals < low) | (vals > high)]
    if not out.empty:
        return _error(
            name,
            f"{value_col}: {len(out)} value(s) outside [{low:,.1f}, {high:,.1f}] "
            f"(min={vals.min():,.2f}, max={vals.max():,.2f})",
        )
    return _ok(name, f"{value_col} in [{vals.min():,.2f}, {vals.max():,.2f}]")


def check_no_duplicate_keys(
    name: str,
    df: pd.DataFrame,
    key_cols: Sequence[str],
    *,
    severity: CheckStatus = CheckStatus.ERROR,
) -> CheckResult:
    """Flag rows that share a ``key_cols`` tuple.

    Defaults to ERROR. Pass ``severity=CheckStatus.WARN`` when downstream
    aggregation tolerates duplicates (e.g. a ``pivot_table`` mean) but you
    still want the double-publish surfaced.
    """
    _require_columns(df, list(key_cols))
    dupes = df.duplicated(subset=list(key_cols), keep=False)
    n = int(dupes.sum())
    if n:
        sample = (
            df.loc[dupes, list(key_cols)].drop_duplicates().head(3).to_dict("records")
        )
        return CheckResult(
            name,
            severity,
            f"{n} duplicate row(s) on key {list(key_cols)}; e.g. {sample}",
        )
    return _ok(name, f"no duplicates on key {list(key_cols)}")


def check_lead_days(
    name: str,
    df: pd.DataFrame,
    target_date: date,
    *,
    lead_days: int,
    date_col: str = "date",
    as_of_col: str = "as_of_date",
) -> CheckResult:
    """ERROR if rows for ``target_date`` aren't the expected DA-cutoff vintage.

    The loader is supposed to have filtered to ``as_of_date == date - lead_days``.
    If the frame carries no ``as_of_date`` column the check is a no-op PASS
    (some parquets are single-vintage).
    """
    _require_columns(df, [date_col])
    if as_of_col not in df.columns:
        return _ok(
            name, f"no {as_of_col} column; single-vintage frame, nothing to check"
        )
    rows = _rows_for_date(df, target_date, date_col)
    if rows.empty:
        return _error(name, f"no rows for target_date {target_date}")
    delta = (
        pd.to_datetime(rows[date_col], errors="coerce")
        - pd.to_datetime(rows[as_of_col], errors="coerce")
    ).dt.days
    observed = sorted(set(delta.dropna().astype(int).tolist()))
    if observed != [lead_days]:
        return _error(
            name,
            f"expected lead_days=={lead_days} for {target_date}, "
            f"found vintage delta(s) {observed}",
        )
    return _ok(name, f"vintage delta == {lead_days} day(s) for {target_date}")


def check_freshness(
    name: str,
    df: pd.DataFrame,
    *,
    max_age_days: int = FRESHNESS_WARN_DAYS,
    as_of_col: str = "as_of_date",
    reference_date: date | None = None,
) -> CheckResult:
    """WARN if the newest ``as_of_date`` in the frame is older than expected.

    Never an ERROR: the data is present, it's just aged. No-op PASS when the
    frame has no ``as_of_date`` column.
    """
    if as_of_col not in df.columns:
        return _ok(name, f"no {as_of_col} column; freshness not applicable")
    ref = reference_date or date.today()
    as_of = pd.to_datetime(df[as_of_col], errors="coerce").dt.date.dropna()
    if as_of.empty:
        return _warn(name, f"{as_of_col} present but all values unparseable / NaT")
    newest = max(as_of)
    cutoff = ref - timedelta(days=max_age_days)
    if newest < cutoff:
        return _warn(
            name,
            f"newest {as_of_col} is {newest}, older than {ref} - {max_age_days}d "
            f"({cutoff})",
        )
    return _ok(name, f"newest {as_of_col} is {newest}")
