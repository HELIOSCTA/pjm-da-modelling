"""Engine for per_hour - 3-hour window features x per-hour matching (AnEn-NWP-style).

Key structural difference vs the per_day_* engines: one match per
(target_date, target_hour), not per (target_date). For each target HE h,
the feature vector is the load forecast at hours [h - flt_radius, h + flt_radius]
(clipped to [1, 24]).

Same-hour-of-day constraint: target HE h is matched only against candidate
days' HE h. Output is hour-keyed - 24 separate top-N selections produce
24 * n_analogs rows total.

Pool-fit z-score, NaN-aware Euclidean over the window, inverse-distance
analog weighting normalized within each hour.
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
                "per_hour season window +/-%dd kept %d candidates",
                season_window_days, len(work),
            )
        else:
            logger.warning(
                "per_hour season window kept only %d candidates "
                "(< min %d) - falling back to full history (%d)",
                len(candidates), min_pool_size, len(work),
            )
    return work


def _window_columns(target_hour: int, flt_radius: int) -> list[str]:
    """Hourly load feature column names for a target hour and +/- flt_radius window."""
    lo = max(1, target_hour - flt_radius)
    hi = min(24, target_hour + flt_radius)
    return [f"load_h{h}" for h in range(lo, hi + 1)]


def _combined_non_load_distance(
    spec: ModelSpec, pool: pd.DataFrame, query: pd.Series,
) -> tuple[np.ndarray | None, float]:
    """Weighted-average per-group RMS-z distance over non-load groups.

    Non-load group features are constant across target hours (broadcast),
    so the combined non-load distance is computed once per pool row and
    reused for all 24 target hours. Returns ``(distance_array, total_weight)``;
    ``distance_array`` is ``None`` when the spec has no non-load groups.
    """
    non_load_groups = [
        (g, float(spec.feature_group_weights.get(g, 0.0)))
        for g in spec.feature_groups
        if not g.startswith("load_") and float(spec.feature_group_weights.get(g, 0.0)) > 0
    ]
    if not non_load_groups:
        return None, 0.0

    n = len(pool)
    weighted_sum = np.zeros(n, dtype=float)
    weight_sum = np.zeros(n, dtype=float)
    for group, weight in non_load_groups:
        cols = spec.feature_groups[group]
        cols_present = [c for c in cols if c in pool.columns and c in query.index]
        if not cols_present:
            continue
        pool_vals = pool[cols_present].to_numpy(dtype=float)
        query_vals = query[cols_present].to_numpy(dtype=float)
        means = np.nanmean(pool_vals, axis=0)
        stds = np.nanstd(pool_vals, axis=0)
        stds = np.where(stds == 0, 1.0, stds)
        pool_z = (pool_vals - means) / stds
        query_z = (query_vals - means) / stds
        diff = query_z - pool_z
        mask = ~np.isnan(diff)
        sq = np.where(mask, diff ** 2, 0.0)
        n_valid = mask.sum(axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            d = np.where(n_valid > 0, np.sqrt(sq.sum(axis=1) / n_valid), np.nan)
        valid = ~np.isnan(d)
        weighted_sum[valid] += weight * d[valid]
        weight_sum[valid] += weight

    distance = np.full(n, np.nan, dtype=float)
    valid = weight_sum > 0
    distance[valid] = weighted_sum[valid] / weight_sum[valid]
    return distance, sum(w for _, w in non_load_groups)


def find_twins_per_hour(
    query: pd.Series,
    pool: pd.DataFrame,
    target_date: date,
    spec: ModelSpec = configs.PER_HOUR_SPEC,
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
    """Per-hour analog table. Shape: 24 * n_analogs rows.

    Columns: hour_ending, rank, date, distance, weight, lmp.
    """
    out_cols = ["hour_ending", "rank", "date", "distance", "weight", "lmp"]

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
            "per_hour: pool has no rows before target_date=%s", target_date,
        )
        return pd.DataFrame(columns=out_cols)

    flt_radius = int(spec.flt_radius)
    rows: list[dict] = []

    # Pre-compute non-load groups' combined distance (constant across hours).
    non_load_dist, non_load_weight = _combined_non_load_distance(spec, work, query)
    load_weight = sum(
        float(w) for g, w in spec.feature_group_weights.items() if g.startswith("load_")
    )
    if load_weight <= 0:
        # No load groups in the spec; treat the dynamic window as the full
        # remaining weight so distances stay finite.
        load_weight = max(0.0, 1.0 - non_load_weight)

    for h in configs.HOURS:
        cols = _window_columns(h, flt_radius)
        cols_present = [c for c in cols if c in work.columns and c in query.index]
        if not cols_present:
            continue

        pool_vals = work[cols_present].to_numpy(dtype=float)
        query_vals = query[cols_present].to_numpy(dtype=float)

        means = np.nanmean(pool_vals, axis=0)
        stds = np.nanstd(pool_vals, axis=0)
        stds = np.where(stds == 0, 1.0, stds)
        pool_z = (pool_vals - means) / stds
        query_z = ((query_vals - means) / stds).reshape(-1)

        diff = query_z - pool_z
        mask = ~np.isnan(diff)
        sq = np.where(mask, diff ** 2, 0.0)
        n_valid = mask.sum(axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            d = np.where(n_valid > 0, np.sqrt(sq.sum(axis=1) / n_valid), np.inf)
        d = np.where(np.isfinite(d), d, np.inf)

        if non_load_dist is not None:
            total_w = load_weight + non_load_weight
            valid_load = np.isfinite(d)
            valid_nl = ~np.isnan(non_load_dist)
            both = valid_load & valid_nl
            combined = np.full_like(d, np.inf)
            combined[both] = (
                load_weight * d[both] + non_load_weight * non_load_dist[both]
            ) / total_w
            # Fall back to load-only when non-load is missing for a row.
            load_only = valid_load & ~valid_nl
            combined[load_only] = d[load_only]
            d = combined

        order = np.argsort(d)
        order = order[np.isfinite(d[order])]
        order = order[:n_analogs]
        if len(order) == 0:
            continue

        d_top = d[order]
        eps = 1e-6
        inv_dist = 1.0 / (d_top + eps)
        top_dates = work.iloc[[int(i) for i in order]]["date"].to_list()
        decay = _calendar.age_decay_weights(top_dates, target_date, recency_half_life_years)
        raw = inv_dist * decay
        if raw.sum() <= 0:
            weights = np.full(len(d_top), 1.0 / max(1, len(d_top)))
        else:
            weights = raw / raw.sum()

        lmp_col = f"lmp_h{h}"
        for rank, (idx_arr, dist, w) in enumerate(zip(order, d_top, weights), start=1):
            row = work.iloc[int(idx_arr)]
            rows.append({
                "hour_ending": h,
                "rank": rank,
                "date": row["date"],
                "distance": float(dist),
                "weight": float(w),
                "lmp": float(row.get(lmp_col, np.nan)) if lmp_col in row.index else float("nan"),
            })

    return pd.DataFrame(rows, columns=out_cols)
