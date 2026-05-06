"""Calendar/date-metadata helpers for like_day_model_knn.

Loads ``pjm_dates_daily.parquet`` (cached locally under
``modelling/data/cache``) and exposes a single ``apply_calendar_filter``
helper that the per-model engines call on the candidate pool before the
distance computation runs.

Schema produced by ``load_pjm_dates_daily``:
    date                  datetime.date
    day_of_week_number    int   (Sun=0 .. Sat=6)
    is_weekend            int   (0/1)
    is_nerc_holiday       int   (0/1)
    is_federal_holiday    int   (0/1)
    summer_winter         str   ('SUMMER'/'WINTER')
    holiday_name          str | None
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.data.loader import _resolve_cache_dir, load_pjm_dates_daily
from da_models.like_day_model_knn import configs

__all__ = [
    "FunnelCounts",
    "FunnelStage",
    "resolve_day_type",
    "resolved_dates_daily_path",
    "load_pjm_dates_daily",
    "resolve_target_day_metadata",
    "apply_calendar_filter",
    "filtered_pool_for_target",
    "age_years",
    "linear_age_penalty",
]


@dataclass
class FunnelStage:
    """One row of the candidate-pool funnel.

    ``relaxed=True`` means the filter was *not* applied because doing so
    would have left fewer than ``min_pool_size`` candidates; in that case
    ``survives`` equals the pre-filter count, ``dropped=0``, and
    ``would_survive`` records the count the filter would have yielded
    had it been applied (so the printer can flag the relax fall-back).
    """

    name: str
    detail: str
    survives: int
    dropped: int
    relaxed: bool = False
    would_survive: int | None = None


@dataclass
class FunnelCounts:
    """Accumulator threaded through the pool-filtering chain.

    Stages are appended in pipeline order: raw pool → chronological cut →
    explicit excludes → recency cap → holiday exclusion → DOW group →
    season window. Any disabled or no-op filter is simply not recorded.
    """

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


logger = logging.getLogger(__name__)


# ── Day-type ───────────────────────────────────────────────────────────


def resolve_day_type(d: date) -> str:
    """Return ``"weekday"`` / ``"saturday"`` / ``"sunday"`` for a delivery date."""
    return configs._day_type_for(d)


# ── Loader ─────────────────────────────────────────────────────────────
# load_pjm_dates_daily lives in common/data/loader.py and is re-exported above.


def resolved_dates_daily_path(cache_dir: Path | str | None) -> Path:
    return _resolve_cache_dir(cache_dir) / configs.PJM_DATES_DAILY_PARQUET


# ── Helpers ────────────────────────────────────────────────────────────


def _dow_group_index(dow_num: int) -> int:
    """Map Sun=0..Sat=6 day-of-week to its DOW_GROUPS bucket index."""
    for idx, days in enumerate(configs.DOW_GROUPS.values()):
        if dow_num in days:
            return idx
    return -1


def resolve_target_day_metadata(
    target_date: date,
    dates_meta: pd.DataFrame | None,
) -> dict:
    """Look up ``target_date`` in ``dates_meta``; fall back to weekday-derived
    values if the date is missing (e.g. far-future delivery date)."""
    target_wd_py = target_date.weekday()  # Mon=0..Sun=6
    fallback = {
        "date": target_date,
        "day_of_week_number": (target_wd_py + 1) % 7,
        "is_weekend": 1 if target_wd_py >= 5 else 0,
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
    if "day_of_week_number" in rec and pd.notna(rec["day_of_week_number"]):
        rec["day_of_week_number"] = int(rec["day_of_week_number"])
    return rec


# ── Filter ─────────────────────────────────────────────────────────────


def apply_calendar_filter(
    pool: pd.DataFrame,
    target_date: date,
    dates_meta: pd.DataFrame | None,
    *,
    same_dow_group: bool = configs.FILTER_SAME_DOW_GROUP,
    same_weekend_group: bool = configs.FILTER_SAME_WEEKEND_GROUP,
    same_weekend_group_for_weekends: bool = configs.FILTER_SAME_WEEKEND_GROUP_FOR_WEEKENDS,
    exclude_holidays: bool = configs.FILTER_EXCLUDE_HOLIDAYS,
    exclude_dates: list[str] | None = None,
    max_age_years: int | None = None,
    min_pool_size: int = configs.MIN_POOL_SIZE,
    funnel: FunnelCounts | None = None,
) -> pd.DataFrame:
    """Restrict the candidate pool to dates with compatible calendar metadata.

    Filters applied (in order):
      1. drop any date listed in ``exclude_dates``
      2. drop candidates older than ``target_date - max_age_years`` when set
      3. drop NERC holidays when the target date is non-holiday and
         ``exclude_holidays`` is True
      4. keep only dates in the same DOW group as the target when
         ``same_dow_group`` is True
      5. keep only dates with matching ``is_weekend`` flag when
         ``same_weekend_group`` is True, OR when target is itself a
         weekend day and ``same_weekend_group_for_weekends`` is True
         (sunny parity — weekday targets pass through, weekend targets
         lock to the weekend candidate set).

    Each filter is reverted (its candidates re-included) when applying it
    would push the pool below ``min_pool_size`` — this mirrors the relaxed
    fallback behavior in the old ``like_day_forecast/similarity/engine.py``.
    """
    if pool is None or len(pool) == 0:
        return pool

    if "date" not in pool.columns:
        logger.warning("apply_calendar_filter: pool has no 'date' column; skipping")
        return pool

    work = pool.copy()
    work["date"] = pd.to_datetime(work["date"]).dt.date

    # 1. explicit exclude-dates list
    excl = list(exclude_dates or [])
    if excl:
        excl_dates = {pd.to_datetime(s).date() for s in excl}
        before = len(work)
        candidates = work[~work["date"].isin(excl_dates)]
        if len(candidates) >= min_pool_size or len(candidates) >= max(0, before - 50):
            work = candidates
            logger.info(
                "calendar filter: excluded %d explicit date(s), %d candidates remain",
                before - len(work),
                len(work),
            )
            if funnel is not None:
                funnel.record(
                    "explicit excludes",
                    f"exclude_dates=[{len(excl_dates)} date(s)]",
                    before=before,
                    after=len(work),
                )

    # 2. max-age cap. Drop anything older than target_date - max_age_years.
    if max_age_years is not None and max_age_years > 0:
        cutoff = pd.Timestamp(target_date) - pd.DateOffset(years=int(max_age_years))
        cutoff_date = cutoff.date()
        before = len(work)
        candidates = work[work["date"] >= cutoff_date]
        if len(candidates) >= min_pool_size:
            work = candidates
            logger.info(
                "calendar filter: max_age_years=%d cutoff=%s dropped %d, %d remain",
                int(max_age_years),
                cutoff_date,
                before - len(work),
                len(work),
            )
            if funnel is not None:
                funnel.record(
                    "recency cap",
                    f"max_age={int(max_age_years)}y (cutoff {cutoff_date})",
                    before=before,
                    after=len(work),
                )
        else:
            logger.warning(
                "calendar filter: max_age_years=%d would leave only %d (< min %d) - relaxing",
                int(max_age_years),
                len(candidates),
                min_pool_size,
            )
            if funnel is not None:
                funnel.record(
                    "recency cap",
                    f"max_age={int(max_age_years)}y (cutoff {cutoff_date})",
                    before=before,
                    after=before,
                    relaxed=True,
                    would_survive=len(candidates),
                )

    if dates_meta is None or len(dates_meta) == 0:
        return work.reset_index(drop=True)

    meta = dates_meta[dates_meta["date"].notna()].copy()
    meta["date"] = pd.to_datetime(meta["date"]).dt.date
    target_meta = resolve_target_day_metadata(target_date, meta)

    work = work.merge(
        meta[
            ["date"]
            + [
                c
                for c in (
                    "day_of_week_number",
                    "is_weekend",
                    "is_nerc_holiday",
                    "is_federal_holiday",
                    "summer_winter",
                )
                if c in meta.columns
            ]
        ],
        on="date",
        how="left",
    )

    # 2. holiday filter — mirror target (sunny parity). When target is a
    # non-holiday, restrict candidates to non-holidays. When target IS a
    # holiday, restrict to holiday candidates (so the rare holiday-target
    # case picks analogs of OTHER holidays, not random weekday). Same
    # ``min_pool_size`` relax-on-undershoot fallback as before.
    target_is_holiday = int(target_meta.get("is_nerc_holiday", 0) or 0) == 1
    if exclude_holidays and "is_nerc_holiday" in work.columns:
        before = len(work)
        holiday_int = work["is_nerc_holiday"].fillna(0).astype(int)
        if target_is_holiday:
            candidates = work[holiday_int == 1]
            detail = "exclude_holidays=True (target IS holiday — keep holidays only)"
        else:
            candidates = work[holiday_int != 1]
            detail = "exclude_holidays=True (target is non-holiday)"
        if len(candidates) >= min_pool_size:
            work = candidates
            logger.info(
                "calendar filter: holiday filter (target_holiday=%s) kept %d, dropped %d",
                target_is_holiday,
                len(work),
                before - len(work),
            )
            if funnel is not None:
                funnel.record(
                    "NERC holiday filter",
                    detail,
                    before=before,
                    after=len(work),
                )
        else:
            logger.warning(
                "calendar filter: holiday filter would leave only %d (< min %d) - relaxing",
                len(candidates),
                min_pool_size,
            )
            if funnel is not None:
                funnel.record(
                    "NERC holiday filter",
                    detail,
                    before=before,
                    after=before,
                    relaxed=True,
                    would_survive=len(candidates),
                )

    # 3. same DOW group
    if same_dow_group and "day_of_week_number" in work.columns:
        target_dow = int(target_meta.get("day_of_week_number", -1))
        target_group = _dow_group_index(target_dow) if target_dow >= 0 else -1
        if target_group >= 0:
            cand_groups = work["day_of_week_number"].apply(
                lambda v: _dow_group_index(int(v)) if pd.notna(v) else -1,
            )
            before = len(work)
            candidates = work[cand_groups == target_group]
            group_name = list(configs.DOW_GROUPS.keys())[target_group]
            group_days = list(configs.DOW_GROUPS.values())[target_group]
            if len(candidates) >= min_pool_size:
                work = candidates
                logger.info(
                    "calendar filter: same DOW group kept %d candidates (dropped %d)",
                    len(work),
                    before - len(work),
                )
                if funnel is not None:
                    funnel.record(
                        "same DOW group",
                        f"group={group_name} (dow_nums={group_days})",
                        before=before,
                        after=len(work),
                    )
            else:
                logger.warning(
                    "calendar filter: same DOW group would leave only %d (< min %d) - relaxing",
                    len(candidates),
                    min_pool_size,
                )
                if funnel is not None:
                    funnel.record(
                        "same DOW group",
                        f"group={group_name} (dow_nums={group_days})",
                        before=before,
                        after=before,
                        relaxed=True,
                        would_survive=len(candidates),
                    )

    # 4. same weekend group (sunny parity).
    target_weekend = int(target_meta.get("is_weekend", 0) or 0)
    apply_weekend = same_weekend_group or (
        same_weekend_group_for_weekends and target_weekend == 1
    )
    if apply_weekend and "is_weekend" in work.columns:
        before = len(work)
        candidates = work[work["is_weekend"].fillna(0).astype(int) == target_weekend]
        weekend_label = "weekend" if target_weekend == 1 else "weekday"
        if len(candidates) >= min_pool_size:
            work = candidates
            logger.info(
                "calendar filter: same weekend group (%s) kept %d candidates (dropped %d)",
                weekend_label,
                len(work),
                before - len(work),
            )
            if funnel is not None:
                funnel.record(
                    "same weekend group",
                    f"is_weekend={target_weekend} ({weekend_label})",
                    before=before,
                    after=len(work),
                )
        else:
            logger.warning(
                "calendar filter: same weekend group would leave only %d (< min %d) - relaxing",
                len(candidates),
                min_pool_size,
            )
            if funnel is not None:
                funnel.record(
                    "same weekend group",
                    f"is_weekend={target_weekend} ({weekend_label})",
                    before=before,
                    after=before,
                    relaxed=True,
                    would_survive=len(candidates),
                )

    return work.reset_index(drop=True)


def filtered_pool_for_target(
    pool: pd.DataFrame,
    target_date: date,
    cfg,
    dates_meta: pd.DataFrame | None,
) -> pd.DataFrame:
    """Convenience wrapper that pulls the filter knobs off a ``KnnModelConfig``."""
    return apply_calendar_filter(
        pool=pool,
        target_date=target_date,
        dates_meta=dates_meta,
        same_dow_group=bool(
            getattr(cfg, "same_dow_group", configs.FILTER_SAME_DOW_GROUP)
        ),
        same_weekend_group=bool(
            getattr(cfg, "same_weekend_group", configs.FILTER_SAME_WEEKEND_GROUP)
        ),
        same_weekend_group_for_weekends=bool(
            getattr(
                cfg,
                "same_weekend_group_for_weekends",
                configs.FILTER_SAME_WEEKEND_GROUP_FOR_WEEKENDS,
            )
        ),
        exclude_holidays=bool(
            getattr(cfg, "exclude_holidays", configs.FILTER_EXCLUDE_HOLIDAYS)
        ),
        exclude_dates=list(getattr(cfg, "exclude_dates", []) or []),
        max_age_years=getattr(cfg, "max_age_years", None),
        min_pool_size=int(getattr(cfg, "min_pool_size", configs.MIN_POOL_SIZE)),
    )


# ── Recency weighting ──────────────────────────────────────────────────


def age_years(
    candidate_dates: pd.Series | np.ndarray | list,
    target_date: date,
) -> np.ndarray:
    """Years between each candidate date and the target date (365.25-day years)."""
    target_ts = pd.Timestamp(target_date)
    dates = pd.to_datetime(pd.Series(list(candidate_dates)))
    return ((target_ts - dates).dt.days / 365.25).to_numpy(dtype=float)


def linear_age_penalty(
    distances: np.ndarray,
    candidate_dates: pd.Series | np.ndarray | list,
    target_date: date,
    half_life_days: float,
) -> np.ndarray:
    """Multiply distances by ``1 + age_days / max(half_life_days, 1)``.

    Applied BEFORE top-N selection — older candidates receive larger
    distances and so are less likely to be picked at all (in contrast
    to the prior post-selection exponential decay this replaces).
    Faithful to ``like_day_model_knn_sunny.calendar.linear_age_penalty``
    and to Sunny's original ``forecast.py:587-589``.
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
    assert {"date", "day_of_week_number", "is_nerc_holiday"}.issubset(df.columns)
    target = date(2024, 8, 6)  # Tuesday
    meta = resolve_target_day_metadata(target, df)
    assert meta["day_type"] == "weekday"

    pool = pd.DataFrame(
        {
            "date": pd.date_range("2024-07-01", "2024-09-15", freq="D").date,
            "load_h1": np.arange(77, dtype=float),
        }
    )
    out = apply_calendar_filter(
        pool=pool,
        target_date=target,
        dates_meta=df,
        same_dow_group=True,
        exclude_holidays=True,
        exclude_dates=[],
        min_pool_size=5,
    )
    print(
        f"smoke: target={target} kept={len(out)}/{len(pool)} day_type={meta['day_type']}"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    _self_check()
