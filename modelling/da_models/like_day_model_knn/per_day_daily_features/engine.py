"""Engine for per_day_daily_features - daily summary features x day-level matching.

Distance: per-group pool-fit z-score, NaN-aware Euclidean within group,
weighted average across groups using
``PER_DAY_DAILY_FEATURES_SPEC.feature_group_weights``.
Inverse-distance analog weighting.
"""
from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from da_models.like_day_model_knn import calendar as _calendar
from da_models.like_day_model_knn import configs
from da_models.like_day_model_knn.configs import ModelSpec

logger = logging.getLogger(__name__)


def _circular_day_distance(day_of_year: np.ndarray, target_doy: int) -> np.ndarray:
    direct = np.abs(day_of_year - float(target_doy))
    return np.minimum(direct, 366.0 - direct)


def _candidate_pool(
    pool: pd.DataFrame,
    target_date: date,
    season_window_days: int,
    min_pool_size: int,
    dates_meta: pd.DataFrame | None = None,
    same_dow_group: bool = False,
    exclude_holidays: bool = False,
    exclude_dates: list[str] | None = None,
    max_age_years: int | None = None,
) -> pd.DataFrame:
    work = pool.copy()
    work = work[pd.to_datetime(work["date"]).dt.date < target_date].copy()
    if len(work) == 0:
        return work
    if (
        same_dow_group or exclude_holidays or exclude_dates or max_age_years
    ) and (dates_meta is not None or max_age_years):
        work = _calendar.apply_calendar_filter(
            pool=work,
            target_date=target_date,
            dates_meta=dates_meta,
            same_dow_group=same_dow_group,
            exclude_holidays=exclude_holidays,
            exclude_dates=exclude_dates,
            max_age_years=max_age_years,
            min_pool_size=min_pool_size,
        )
        if len(work) == 0:
            return work
    if season_window_days > 0:
        target_doy = pd.Timestamp(target_date).dayofyear
        doys = pd.to_datetime(work["date"]).dt.dayofyear.to_numpy(dtype=float)
        keep = _circular_day_distance(doys, target_doy) <= float(season_window_days)
        candidates = work[keep]
        if len(candidates) >= min_pool_size:
            work = candidates.copy()
            logger.info(
                "per_day_daily_features season window +/-%dd kept %d candidates",
                season_window_days, len(work),
            )
        else:
            logger.warning(
                "per_day_daily_features season window kept only %d candidates "
                "(< min %d) - falling back to full history (%d)",
                len(candidates), min_pool_size, len(work),
            )
    return work


def _zscore_fit(arr: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    means = np.nanmean(arr, axis=0)
    stds = np.nanstd(arr, axis=0)
    stds = np.where(stds == 0, 1.0, stds)
    return means, stds


def _nan_aware_euclidean(query: np.ndarray, pool_row: np.ndarray) -> tuple[float, int]:
    diff = query - pool_row
    mask = ~np.isnan(diff)
    n_valid = int(mask.sum())
    if n_valid == 0:
        return float("nan"), 0
    return float(np.sqrt(np.sum(diff[mask] ** 2) / n_valid)), n_valid


def _compute_distances(
    query: pd.Series,
    pool: pd.DataFrame,
    spec: ModelSpec,
) -> np.ndarray:
    n = len(pool)
    weighted_sum = np.zeros(n, dtype=float)
    weight_sum = np.zeros(n, dtype=float)

    for group, cols in spec.feature_groups.items():
        weight = float(spec.feature_group_weights.get(group, 0.0))
        if weight <= 0:
            continue
        cols_present = [c for c in cols if c in pool.columns and c in query.index]
        if not cols_present:
            continue

        pool_vals = pool[cols_present].to_numpy(dtype=float)
        query_vals = query[cols_present].to_numpy(dtype=float)

        means, stds = _zscore_fit(pool_vals)
        pool_z = (pool_vals - means) / stds
        query_z = ((query_vals - means) / stds).reshape(-1)

        for i in range(n):
            d, n_valid = _nan_aware_euclidean(query_z, pool_z[i])
            if n_valid == 0 or np.isnan(d):
                continue
            weighted_sum[i] += weight * d
            weight_sum[i] += weight

    distances = np.full(n, np.inf, dtype=float)
    valid = weight_sum > 0
    distances[valid] = weighted_sum[valid] / weight_sum[valid]
    return distances


def find_twins_day(
    query: pd.Series,
    pool: pd.DataFrame,
    target_date: date,
    spec: ModelSpec = configs.PER_DAY_DAILY_FEATURES_SPEC,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    season_window_days: int = configs.SEASON_WINDOW_DAYS,
    min_pool_size: int = configs.MIN_POOL_SIZE,
    dates_meta: pd.DataFrame | None = None,
    same_dow_group: bool = False,
    exclude_holidays: bool = False,
    exclude_dates: list[str] | None = None,
    max_age_years: int | None = None,
    recency_half_life_years: float | None = None,
) -> pd.DataFrame:
    """Top-N analog days. Columns: rank, date, distance, weight, lmp_h1..lmp_h24."""
    out_cols = ["rank", "date", "distance", "weight"] + configs.LMP_LABEL_COLUMNS

    work = _candidate_pool(
        pool, target_date, season_window_days, min_pool_size,
        dates_meta=dates_meta,
        same_dow_group=same_dow_group,
        exclude_holidays=exclude_holidays,
        exclude_dates=exclude_dates,
        max_age_years=max_age_years,
    )
    if len(work) == 0:
        logger.warning(
            "per_day_daily_features: pool has no rows before target_date=%s",
            target_date,
        )
        return pd.DataFrame(columns=out_cols)

    distances = _compute_distances(query, work, spec)
    work = work.assign(distance=distances)
    work = work[np.isfinite(work["distance"])].copy()
    if len(work) == 0:
        logger.warning(
            "per_day_daily_features: all pool rows produced infinite/NaN distance",
        )
        return pd.DataFrame(columns=out_cols)

    work = work.sort_values(["distance", "date"], ascending=[True, False])
    top = work.head(n_analogs).reset_index(drop=True)

    eps = 1e-6
    inv_dist = 1.0 / (top["distance"].to_numpy(dtype=float) + eps)
    decay = _calendar.age_decay_weights(top["date"], target_date, recency_half_life_years)
    raw = inv_dist * decay
    if raw.sum() <= 0:
        weights = np.full(len(top), 1.0 / max(1, len(top)))
    else:
        weights = raw / raw.sum()
    top = top.assign(weight=weights, rank=range(1, len(top) + 1))
    return top[out_cols]
