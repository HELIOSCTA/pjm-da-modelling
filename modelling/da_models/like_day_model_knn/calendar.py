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
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.data.loader import _resolve_cache_dir
from da_models.like_day_model_knn import configs

logger = logging.getLogger(__name__)


# ── Day-type ───────────────────────────────────────────────────────────

def resolve_day_type(d: date) -> str:
    """Return ``"weekday"`` / ``"saturday"`` / ``"sunday"`` for a delivery date."""
    return configs._day_type_for(d)


# ── Loader ─────────────────────────────────────────────────────────────

_DATES_DAILY_COLS = (
    "date",
    "day_of_week_number",
    "is_weekend",
    "is_nerc_holiday",
    "is_federal_holiday",
    "summer_winter",
    "holiday_name",
)


def resolved_dates_daily_path(cache_dir: Path | str | None) -> Path:
    return _resolve_cache_dir(cache_dir) / configs.PJM_DATES_DAILY_PARQUET


def load_pjm_dates_daily(cache_dir: Path | str | None = None) -> pd.DataFrame:
    """Load ``pjm_dates_daily.parquet`` and normalize types for filter use.

    Returns a sorted, de-duplicated frame with one row per delivery date.
    """
    path = resolved_dates_daily_path(cache_dir)
    if not path.exists():
        raise FileNotFoundError(
            f"pjm_dates_daily parquet not found at {path}. "
            f"Set DA_MODELS_CACHE_DIR or place the cache file there."
        )

    df = pd.read_parquet(path)
    keep = [c for c in _DATES_DAILY_COLS if c in df.columns]
    df = df[keep].copy()

    df["date"] = pd.to_datetime(df["date"]).dt.date
    if "day_of_week_number" in df.columns:
        df["day_of_week_number"] = pd.to_numeric(
            df["day_of_week_number"], errors="coerce",
        ).astype("Int64")
    for flag in ("is_weekend", "is_nerc_holiday", "is_federal_holiday"):
        if flag in df.columns:
            df[flag] = pd.to_numeric(df[flag], errors="coerce").fillna(0).astype(int)
    if "summer_winter" in df.columns:
        df["summer_winter"] = (
            df["summer_winter"].astype("string").str.upper().fillna("")
        )

    df = df.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
    df = df.sort_values("date").reset_index(drop=True)
    logger.info("Loaded pjm_dates_daily: %d rows from %s", len(df), path.name)
    return df


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
    exclude_holidays: bool = configs.FILTER_EXCLUDE_HOLIDAYS,
    exclude_dates: list[str] | None = None,
    min_pool_size: int = configs.MIN_POOL_SIZE,
) -> pd.DataFrame:
    """Restrict the candidate pool to dates with compatible calendar metadata.

    Filters applied (in order):
      1. drop any date listed in ``exclude_dates``
      2. drop NERC holidays when the target date is non-holiday and
         ``exclude_holidays`` is True
      3. keep only dates in the same DOW group as the target when
         ``same_dow_group`` is True

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
                before - len(work), len(work),
            )

    if dates_meta is None or len(dates_meta) == 0:
        return work.reset_index(drop=True)

    meta = dates_meta[dates_meta["date"].notna()].copy()
    meta["date"] = pd.to_datetime(meta["date"]).dt.date
    target_meta = resolve_target_day_metadata(target_date, meta)

    work = work.merge(
        meta[["date"] + [c for c in (
            "day_of_week_number", "is_weekend", "is_nerc_holiday",
            "is_federal_holiday", "summer_winter",
        ) if c in meta.columns]],
        on="date", how="left",
    )

    # 2. holiday exclusion (only when target is itself non-holiday)
    target_is_holiday = int(target_meta.get("is_nerc_holiday", 0) or 0) == 1
    if exclude_holidays and not target_is_holiday and "is_nerc_holiday" in work.columns:
        before = len(work)
        candidates = work[work["is_nerc_holiday"].fillna(0).astype(int) != 1]
        if len(candidates) >= min_pool_size:
            work = candidates
            logger.info(
                "calendar filter: dropped %d NERC holiday candidate(s), %d remain",
                before - len(work), len(work),
            )
        else:
            logger.warning(
                "calendar filter: holiday exclusion would leave only %d (< min %d) - keeping holidays",
                len(candidates), min_pool_size,
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
            if len(candidates) >= min_pool_size:
                work = candidates
                logger.info(
                    "calendar filter: same DOW group kept %d candidates (dropped %d)",
                    len(work), before - len(work),
                )
            else:
                logger.warning(
                    "calendar filter: same DOW group would leave only %d (< min %d) - relaxing",
                    len(candidates), min_pool_size,
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
        same_dow_group=bool(getattr(cfg, "same_dow_group", configs.FILTER_SAME_DOW_GROUP)),
        exclude_holidays=bool(getattr(cfg, "exclude_holidays", configs.FILTER_EXCLUDE_HOLIDAYS)),
        exclude_dates=list(getattr(cfg, "exclude_dates", []) or []),
        min_pool_size=int(getattr(cfg, "min_pool_size", configs.MIN_POOL_SIZE)),
    )


# ── Light-weight smoke test ────────────────────────────────────────────

def _self_check() -> None:  # pragma: no cover - run via __main__
    df = load_pjm_dates_daily(None)
    assert {"date", "day_of_week_number", "is_nerc_holiday"}.issubset(df.columns)
    target = date(2024, 8, 6)  # Tuesday
    meta = resolve_target_day_metadata(target, df)
    assert meta["day_type"] == "weekday"

    pool = pd.DataFrame({
        "date": pd.date_range("2024-07-01", "2024-09-15", freq="D").date,
        "fcst_load_h1": np.arange(77, dtype=float),
    })
    out = apply_calendar_filter(
        pool=pool, target_date=target, dates_meta=df,
        same_dow_group=True, exclude_holidays=True,
        exclude_dates=[], min_pool_size=5,
    )
    print(f"smoke: target={target} kept={len(out)}/{len(pool)} day_type={meta['day_type']}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    _self_check()
