"""Per-HE operating-reserve MW for the supply-stack dispatch denominator.

Reads the dbt mart ``pjm_reserve_market_results_hourly`` via
``loader.load_reserve_market_results_hourly()``. The mart is
backward-only -- today and forward delivery dates don't have rows -- so
this helper layers three resolution rules in priority order:

  1. **Direct hit**: ``target_date`` has all 24 HE rows in the mart -> use
     the actual ``operating_reserve_requirement_mw`` per hour. This is
     the historical/backtest path. We use the *requirement* (as_req_mw
     sum), not the cleared MW: PJM overclears 30-MIN reserve with units
     that are *also* bidding into energy, so cleared MW double-counts
     capacity that remains energy-available. The requirement is the
     stable ~7.5 GW figure that should drive the haircut.
  2. **Rolling profile**: average the most recent ``ROLLING_DAYS`` of
     historical rows, grouped by (weekend?, HE). This is the forward
     path (D+1 .. D+N). The requirement is extremely stable
     (~7,500 MW +/- 30 MW in May 2026), so even a flat scalar would
     work; per-HE profile catches the small overnight/peak shape.
  3. **Fallback constant**: ``configs.OPERATING_RESERVE_MW_FALLBACK``.
     Triggered when the parquet is missing or empty (loader exception,
     no historical rows within the window).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from da_models.common.data import loader
from da_models.supply_stack import configs as C

logger = logging.getLogger(__name__)

ROLLING_DAYS: int = 30


def _fallback(reason: str) -> dict[int, float]:
    logger.warning(
        "operating reserve fallback (%s) -- using %.0f MW",
        reason,
        C.OPERATING_RESERVE_MW_FALLBACK,
    )
    return {h: float(C.OPERATING_RESERVE_MW_FALLBACK) for h in C.HOURS}


def get_operating_reserve_mw_by_he(
    target_date: date, *, cache_dir: Path | None = None
) -> dict[int, float]:
    """Return per-HE operating-reserve requirement MW for ``target_date``.

    Direct historical hit first, then a (weekend?, HE) rolling-mean
    profile from the last ``ROLLING_DAYS`` days, then the config fallback.
    The result is a dense dict (every HE in ``configs.HOURS`` is keyed) so
    the dispatch loop never has to handle a missing HE.
    """
    try:
        df = loader.load_reserve_market_results_hourly(cache_dir=cache_dir)
    except Exception as exc:  # noqa: BLE001
        return _fallback(f"loader error: {exc}")
    if df.empty or "operating_reserve_requirement_mw" not in df.columns:
        return _fallback("empty or schema-mismatched mart")

    # 1. Direct hit -- 24 rows for the target date.
    day = df[df["date"] == target_date]
    if len(day) >= len(C.HOURS):
        by_he = (
            day.groupby("hour_ending")["operating_reserve_requirement_mw"]
            .mean()
            .to_dict()
        )
        if all(h in by_he and pd.notna(by_he[h]) for h in C.HOURS):
            return {h: float(by_he[h]) for h in C.HOURS}

    # 2. Rolling (weekend?, HE) profile from history.
    cutoff = target_date - timedelta(days=ROLLING_DAYS)
    recent = df[(df["date"] >= cutoff) & (df["date"] < target_date)].copy()
    if recent.empty:
        return _fallback(
            f"no historical rows in last {ROLLING_DAYS} days before {target_date}"
        )
    is_weekend_target = target_date.weekday() >= 5
    recent["is_weekend"] = recent["date"].apply(lambda d: d.weekday() >= 5)
    matched = recent[recent["is_weekend"] == is_weekend_target]
    if matched.empty:  # fall back to all DOWs if the matched subset is too thin
        matched = recent
    profile = (
        matched.groupby("hour_ending")["operating_reserve_requirement_mw"]
        .mean()
        .to_dict()
    )
    if not profile:
        return _fallback("rolling profile empty")
    # Day-level mean as the inner-fallback for any missing HE.
    day_mean = float(matched["operating_reserve_requirement_mw"].mean())
    return {h: float(profile.get(h, day_mean)) for h in C.HOURS}
