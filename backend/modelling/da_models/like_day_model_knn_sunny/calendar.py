"""Calendar/date-metadata helpers for like_day_model_knn_sunny.

Faithful port of Sunny's per-hour calendar logic:

  - Sun=0..Sat=6 day-of-week numbering (vs. Python's Mon=0..Sun=6).
  - Filter ladder picks the FIRST stage whose candidate count meets
    ``min_pool_size``: ``exact_dow+holiday`` → ``exact_dow_only`` →
    ``weekend_group+holiday`` → ``weekend_group_only`` → ``no_filter``.
  - Linear pre-selection age penalty:
    ``distance *= 1 + age_days / max(half_life_days, 1)``.

Loads ``pjm_dates_daily.parquet`` for holiday/weekend metadata; falls
back to weekday-derived values when a row is missing (e.g. far-future
delivery date).
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from backend.modelling.da_models.common.data.loader import _resolve_cache_dir, load_pjm_dates_daily
from backend.modelling.da_models.like_day_model_knn_sunny import configs

__all__ = [
    "FunnelCounts",
    "FunnelStage",
    "compute_sunny_calendar_row",
    "resolve_day_type",
    "resolved_dates_daily_path",
    "load_pjm_dates_daily",
    "resolve_target_day_metadata",
    "filter_candidates",
    "linear_age_penalty",
]

logger = logging.getLogger(__name__)


# ── Calendar row (Sun=0..Sat=6 numbering) ──────────────────────────────


def compute_sunny_calendar_row(d: date, is_nerc_holiday: bool = False) -> dict:
    """Day-level calendar features using Sunny's Sun=0..Sat=6 convention.

    Mirrors ``forecast.py:_calendar_features_for``. ``is_nerc_holiday``
    must be provided by the caller (looked up from ``pjm_dates_daily``);
    this function does not own the holiday calendar.
    """
    weekday_mon0 = d.weekday()  # Mon=0..Sun=6 (Python)
    dow_num = (weekday_mon0 + 1) % 7  # Sun=0..Sat=6 (Sunny)
    return {
        "day_of_week_number": int(dow_num),
        "is_nerc_holiday": int(bool(is_nerc_holiday)),
        "is_weekend": 1 if dow_num in (0, 6) else 0,
        "dow_sin": float(math.sin(2 * math.pi * dow_num / 7.0)),
        "dow_cos": float(math.cos(2 * math.pi * dow_num / 7.0)),
    }


def resolve_day_type(d: date) -> str:
    """Return ``"weekday"`` / ``"saturday"`` / ``"sunday"`` for a delivery date."""
    return configs._day_type_for(d)


def resolved_dates_daily_path(cache_dir: Path | str | None) -> Path:
    return _resolve_cache_dir(cache_dir) / configs.PJM_DATES_DAILY_PARQUET


# ── Funnel accumulator (parallel to like_day_model_knn) ────────────────


@dataclass
class FunnelStage:
    name: str
    detail: str
    survives: int
    dropped: int
    relaxed: bool = False
    would_survive: int | None = None


@dataclass
class FunnelCounts:
    stages: list[FunnelStage] = field(default_factory=list)

    def record(
        self,
        name: str,
        detail: str,
        *,
        before: int,
        after: int,
        relaxed: bool = False,
        would_survive: int | None = None,
    ) -> None:
        self.stages.append(
            FunnelStage(
                name=name,
                detail=detail,
                survives=after,
                dropped=max(0, before - after),
                relaxed=relaxed,
                would_survive=would_survive,
            )
        )

    @property
    def initial(self) -> int:
        return self.stages[0].survives if self.stages else 0

    @property
    def final(self) -> int:
        return self.stages[-1].survives if self.stages else 0


# ── Target-date metadata ───────────────────────────────────────────────


def resolve_target_day_metadata(
    target_date: date,
    dates_meta: pd.DataFrame | None,
) -> dict:
    """Look up ``target_date`` in ``dates_meta``; fall back to weekday-derived
    values (Sun=0..Sat=6 numbering) if missing."""
    target_wd_py = target_date.weekday()  # Mon=0..Sun=6
    dow_sunny = (target_wd_py + 1) % 7  # Sun=0..Sat=6
    fallback = {
        "date": target_date,
        "day_of_week_number": dow_sunny,
        "is_weekend": 1 if dow_sunny in (0, 6) else 0,
        "is_nerc_holiday": 0,
        "is_federal_holiday": 0,
        "summer_winter": "SUMMER" if 4 <= target_date.month <= 10 else "WINTER",
        "day_type": resolve_day_type(target_date),
    }
    if dates_meta is None or len(dates_meta) == 0:
        return fallback

    row = dates_meta[dates_meta["date"] == target_date]
    if len(row) == 0:
        logger.warning(
            "target_date %s not in pjm_dates_daily; using weekday-derived metadata",
            target_date,
        )
        return fallback

    rec = row.iloc[0].to_dict()
    rec["day_type"] = resolve_day_type(target_date)
    # ``pjm_dates_daily`` numbers DOW Sun=0..Sat=6 already, so no remap needed.
    if "day_of_week_number" in rec and pd.notna(rec["day_of_week_number"]):
        rec["day_of_week_number"] = int(rec["day_of_week_number"])
    return rec


# ── Sunny's filter ladder ──────────────────────────────────────────────


def filter_candidates(
    work: pd.DataFrame,
    *,
    target_dow: int,
    target_holiday: int,
    target_weekend: int,
    same_dow_group: bool,
    same_weekend_group: bool,
    same_weekend_group_for_weekends: bool,
    exclude_holidays: bool,
    min_pool_size: int,
    funnel: FunnelCounts | None = None,
) -> tuple[pd.DataFrame, str]:
    """Pick the first ladder stage whose candidate count meets ``min_pool_size``.

    Mirrors ``forecast.py:_select_analogs_for_hour`` lines 522–554.
    Stages, in order:
        1. ``exact_dow+holiday`` (when same_dow_group AND exclude_holidays)
        2. ``exact_dow_only``    (when same_dow_group)
        3. ``weekend_group+holiday`` (when apply_weekend AND exclude_holidays)
        4. ``weekend_group_only``    (when apply_weekend)
        5. ``no_filter``         (always tried last)

    ``apply_weekend`` is True when ``same_weekend_group`` is set, OR when
    ``same_weekend_group_for_weekends`` is set and the target is itself
    a weekend day. Returns (chosen_frame, stage_name).

    The ``holiday_mask`` matches the original: when the target is a
    holiday, restrict to holiday candidates; otherwise restrict to
    non-holiday candidates.
    """
    if work is None or len(work) == 0:
        return work, "empty"

    if "is_nerc_holiday" not in work.columns:
        # Without holiday metadata, holiday_mask is a no-op → True everywhere.
        holiday_mask = pd.Series(True, index=work.index)
    else:
        holiday_mask = (
            (work["is_nerc_holiday"] == 1)
            if target_holiday
            else (work["is_nerc_holiday"] != 1)
        )

    candidates: list[tuple[str, pd.DataFrame]] = []
    if same_dow_group and "day_of_week_number" in work.columns:
        exact_dow = work["day_of_week_number"] == target_dow
        if exclude_holidays:
            candidates.append(("exact_dow+holiday", work[exact_dow & holiday_mask]))
        candidates.append(("exact_dow_only", work[exact_dow]))

    apply_weekend = same_weekend_group or (
        same_weekend_group_for_weekends and target_weekend == 1
    )
    if apply_weekend and "is_weekend" in work.columns:
        same_weekend = work["is_weekend"] == target_weekend
        if exclude_holidays:
            candidates.append(
                ("weekend_group+holiday", work[same_weekend & holiday_mask])
            )
        candidates.append(("weekend_group_only", work[same_weekend]))

    candidates.append(("no_filter", work))

    chosen_name = "no_filter"
    chosen = work
    for stage, frame in candidates:
        if funnel is not None:
            funnel.record(
                f"ladder:{stage}",
                f"size={len(frame)}",
                before=len(work),
                after=len(frame),
                relaxed=len(frame) < min_pool_size,
                would_survive=len(frame) if len(frame) < min_pool_size else None,
            )
        if len(frame) >= min_pool_size:
            chosen = frame
            chosen_name = stage
            break

    return chosen, chosen_name


# ── Linear age penalty (pre-selection) ─────────────────────────────────


def linear_age_penalty(
    distances: np.ndarray,
    candidate_dates: pd.Series | np.ndarray | list,
    target_date: date,
    half_life_days: float,
) -> np.ndarray:
    """Multiply distances by ``1 + age_days / max(half_life_days, 1)``.

    Applied BEFORE top-N selection, faithful to ``forecast.py:587-589``.
    Older candidates receive larger distances and so are less likely to
    be picked at all (in contrast to the post-selection exponential decay
    used by the sibling ``like_day_model_knn`` package).
    """
    target_ts = pd.Timestamp(target_date)
    dates = pd.to_datetime(pd.Series(list(candidate_dates)))
    age = ((target_ts - dates).dt.days).to_numpy(dtype=float)
    age = np.maximum(age, 0.0)
    half = float(max(half_life_days, 1.0))
    return distances * (1.0 + age / half)


# ── Light-weight smoke test ────────────────────────────────────────────


def _self_check() -> None:  # pragma: no cover - run via __main__
    df = load_pjm_dates_daily(cache_dir=None)
    target = date(2024, 8, 6)  # Tuesday
    meta = resolve_target_day_metadata(target, df)
    assert meta["day_type"] == "weekday"
    row = compute_sunny_calendar_row(target, bool(meta.get("is_nerc_holiday", 0)))
    assert row["day_of_week_number"] == 2  # Sun=0..Sat=6: Sun=0, Mon=1, Tue=2
    print(f"smoke: target={target} day_type={meta['day_type']} sunny_row={row}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    _self_check()
