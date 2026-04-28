"""Calendar filtering and fallback ladder for forward-only KNN."""
from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _holiday_mask(pool: pd.DataFrame, target_is_holiday: bool) -> pd.Series:
    """Mask for holiday matching."""
    if "is_nerc_holiday" not in pool.columns:
        return pd.Series([True] * len(pool), index=pool.index)
    if target_is_holiday:
        return pool["is_nerc_holiday"] == 1
    return pool["is_nerc_holiday"] != 1


def _exact_dow_mask(pool: pd.DataFrame, target_dow: int) -> pd.Series:
    """Mask for exact day-of-week matching (Sun=0..Sat=6)."""
    if "day_of_week_number" not in pool.columns:
        return pd.Series([True] * len(pool), index=pool.index)
    return pool["day_of_week_number"] == int(target_dow)


def _dow_group_mask(pool: pd.DataFrame, target_dow_group: int) -> pd.Series:
    """Mask for day-of-week group matching (weekday/sat/sun)."""
    if "dow_group" not in pool.columns:
        return pd.Series([True] * len(pool), index=pool.index)
    return pool["dow_group"] == int(target_dow_group)


def apply_filter_ladder(
    pool: pd.DataFrame,
    target_dow: int,
    target_dow_group: int,
    target_is_holiday: bool,
    min_pool_size: int,
    same_dow_group: bool = True,
    exclude_holidays: bool = True,
) -> pd.DataFrame:
    """Apply strict-to-relaxed calendar filtering until minimum pool size is met."""
    base = pool.copy()

    holiday_mask = _holiday_mask(base, target_is_holiday) if exclude_holidays else pd.Series(
        [True] * len(base), index=base.index,
    )

    exact_dow = _exact_dow_mask(base, target_dow)
    group_dow = _dow_group_mask(base, target_dow_group) if same_dow_group else pd.Series(
        [True] * len(base), index=base.index,
    )

    candidates: list[tuple[str, pd.DataFrame]] = [
        ("exact_dow+holiday", base[exact_dow & holiday_mask]),
        ("exact_dow_only", base[exact_dow]),
        ("dow_group+holiday", base[group_dow & holiday_mask]),
        ("dow_group_only", base[group_dow]),
        ("no_calendar_filter", base),
    ]

    for stage, frame in candidates:
        if len(frame) >= min_pool_size:
            logger.info(
                "Calendar filter stage '%s' accepted (%s rows, min=%s)",
                stage,
                len(frame),
                min_pool_size,
            )
            return frame

    logger.warning(
        "Pool remains below minimum after fallback ladder (%s rows, min=%s)",
        len(base),
        min_pool_size,
    )
    return base


def outage_regime_filter(
    pool: pd.DataFrame,
    target_outage: float | None,
    outage_col: str = "outage_total_mw",
    tolerance_std: float = 1.5,
) -> tuple[pd.DataFrame, dict]:
    """Drop candidates whose outage z-score is too far from the target's.

    A 35 GW outage day is not a good analog for a 65 GW outage day even if
    load and gas profiles match. This filter mirrors the helioscta-pjm-da
    `outage_regime_filter`: candidates whose z-score differs from the
    target's by more than ``tolerance_std`` are excluded.

    Returns the filtered pool and a stats dict with before/after counts and
    the target's z-score (useful for the diagnostic log line).
    """
    stats: dict = {
        "applied": False,
        "before": len(pool),
        "after": len(pool),
        "target_outage": target_outage,
        "z_target": None,
        "tolerance_std": tolerance_std,
        "skipped_reason": None,
    }

    if outage_col not in pool.columns:
        stats["skipped_reason"] = f"column '{outage_col}' missing from pool"
        return pool, stats

    if target_outage is None or (isinstance(target_outage, float) and np.isnan(target_outage)):
        stats["skipped_reason"] = "target outage is NaN/None"
        return pool, stats

    series = pd.to_numeric(pool[outage_col], errors="coerce")
    valid = series.notna()
    if valid.sum() < 2:
        stats["skipped_reason"] = "fewer than 2 valid outage values in pool"
        return pool, stats

    pool_mean = float(series[valid].mean())
    pool_std = float(series[valid].std())
    if pool_std == 0:
        stats["skipped_reason"] = "pool outage std is zero"
        return pool, stats

    z_target = (float(target_outage) - pool_mean) / pool_std
    z_candidates = (series - pool_mean) / pool_std
    keep = (z_candidates - z_target).abs() <= float(tolerance_std)
    keep = keep.fillna(False)

    filtered = pool[keep].copy()
    stats["applied"] = True
    stats["after"] = len(filtered)
    stats["z_target"] = z_target
    return filtered, stats


def ensure_minimum_pool(
    filtered: pd.DataFrame,
    full: pd.DataFrame,
    target_date: date,
    min_size: int,
) -> pd.DataFrame:
    """Backfill from the full pool by date proximity if filters cut too aggressively."""
    if len(filtered) >= min_size:
        return filtered

    logger.warning(
        "Pool too small after filters (%s < %s) — relaxing on date proximity",
        len(filtered), min_size,
    )

    fallback = full[
        (full["date"] != target_date) & (full["date"] < target_date)
    ].copy()
    if len(fallback) == 0:
        return filtered

    fallback["_d"] = np.abs(
        (pd.to_datetime(fallback["date"]) - pd.Timestamp(target_date)).dt.days
    )
    fallback = fallback.sort_values("_d").drop(columns=["_d"])
    return fallback.head(max(min_size, len(filtered)))
