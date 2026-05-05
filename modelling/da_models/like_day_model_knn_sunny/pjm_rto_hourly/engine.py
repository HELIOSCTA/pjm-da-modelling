"""Engine for pjm_rto_hourly (Sunny variant) — scalar per-HE matching.

Faithful port of Sunny's ``_select_analogs_for_hour``:

  - Same hour-of-day match: target HE h compared only to candidate HE h.
  - Circular DOY season window before the calendar-ladder filter.
  - Sunny's filter ladder via ``calendar.filter_candidates``.
  - Per-feature-group NaN-safe z-fit + sum-Euclidean (NO /n_valid).
  - Linear pre-selection age penalty (days-based) before top-N.
  - Inverse-distance squared weight: ``1 / max(d, 1e-8)**2`` then renormalize.
  - Tie-break by date desc (newer first).
"""

from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from da_models.like_day_model_knn_sunny import calendar as _calendar
from da_models.like_day_model_knn_sunny import configs
from da_models.like_day_model_knn_sunny.configs import ModelSpec

logger = logging.getLogger(__name__)


HOURS: tuple[int, ...] = tuple(range(1, 25))


def _circular_doy_distance(doy: np.ndarray, target_doy: int) -> np.ndarray:
    direct = np.abs(doy - float(target_doy))
    return np.minimum(direct, 366.0 - direct)


def _fit_zscore(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """NaN-safe pool-only z-fit. Faithful to forecast.py:68-79."""
    valid = ~np.isnan(values)
    counts = valid.sum(axis=0).astype(float)
    safe = np.where(valid, values, 0.0)
    means = safe.sum(axis=0) / np.where(counts == 0.0, 1.0, counts)
    means = np.where(counts == 0.0, 0.0, means)
    centered = np.where(valid, values - means, 0.0)
    variances = (centered**2).sum(axis=0) / np.where(counts == 0.0, 1.0, counts)
    stds = np.sqrt(variances)
    stds = np.where((stds == 0.0) | (counts == 0.0) | np.isnan(stds), 1.0, stds)
    return means, stds


def _nan_aware_distance(query_z: np.ndarray, pool_z: np.ndarray) -> np.ndarray:
    """Per-row sum-Euclidean over dimensions where both are finite.

    NO division by n_valid — faithful to Sunny's forecast.py:82-91.
    Returns NaN for rows with no finite dimensions.
    """
    diff = pool_z - query_z[np.newaxis, :]
    valid = ~np.isnan(diff)
    sq = np.where(valid, diff**2, 0.0)
    n_valid = valid.sum(axis=1).astype(float)
    out = np.full(len(pool_z), np.nan, dtype=float)
    has_valid = n_valid > 0
    out[has_valid] = np.sqrt(sq[has_valid].sum(axis=1))
    return out


def _effective_weights(
    spec: ModelSpec,
    override: dict[str, float] | None,
) -> dict[str, float]:
    if override is None:
        return spec.feature_group_weights
    valid = set(spec.feature_groups.keys())
    bad = set(override) - valid
    if bad:
        raise ValueError(
            f"Unknown weight-override keys: {sorted(bad)}. Valid: {sorted(valid)}"
        )
    raw = {g: float(override.get(g, 0.0)) for g in valid}
    total = sum(raw.values())
    if total <= 0:
        raise ValueError(f"Weight override sums to {total}; need > 0.")
    return {k: v / total for k, v in raw.items()}


def find_twins(
    query: pd.DataFrame,
    pool: pd.DataFrame,
    target_date: date,
    spec: ModelSpec,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    season_window_days: int = configs.SEASON_WINDOW_DAYS,
    min_pool_size: int = configs.MIN_POOL_SIZE,
    dates_meta: pd.DataFrame | None = None,
    same_dow_group: bool = False,
    same_weekend_group: bool = False,
    same_weekend_group_for_weekends: bool = False,
    exclude_holidays: bool = False,
    exclude_dates: list[str] | None = None,
    recency_half_life_days: float = configs.RECENCY_HALF_LIFE_DAYS,
    feature_group_weights_override: dict[str, float] | None = None,
    funnel: _calendar.FunnelCounts | None = None,
) -> pd.DataFrame:
    """Per-hour analog table. Long format: 24 * n_analogs rows.

    Columns: hour_ending, rank, date, distance, weight, lmp.
    """
    out_cols = ["hour_ending", "rank", "date", "distance", "weight", "lmp"]
    weights = _effective_weights(spec, feature_group_weights_override)

    work_all = pool.copy()
    if exclude_dates:
        drop_set = {pd.to_datetime(d).date() for d in exclude_dates}
        work_all = work_all[~work_all["date"].isin(drop_set)].copy()

    if funnel is not None:
        funnel.record(
            "raw history",
            f"build_pool: {len(work_all)} rows",
            before=len(work_all),
            after=len(work_all),
        )

    rows: list[dict] = []

    for h in HOURS:
        q_rows = query[query["hour_ending"] == h]
        if len(q_rows) == 0:
            continue
        q_row = q_rows.iloc[0]

        work = work_all[
            (work_all["hour_ending"] == h) & (work_all["date"] < target_date)
        ].copy()
        if len(work) == 0:
            continue

        if season_window_days > 0:
            target_doy = pd.Timestamp(target_date).dayofyear
            doys = pd.to_datetime(work["date"]).dt.dayofyear.to_numpy(dtype=float)
            keep = _circular_doy_distance(doys, target_doy) <= float(season_window_days)
            work = work[keep]
            if len(work) == 0:
                continue

        target_dow = int(q_row.get("day_of_week_number", 0))
        target_holiday = int(q_row.get("is_nerc_holiday", 0))
        target_weekend = int(q_row.get("is_weekend", 0))

        chosen, _stage = _calendar.filter_candidates(
            work,
            target_dow=target_dow,
            target_holiday=target_holiday,
            target_weekend=target_weekend,
            same_dow_group=same_dow_group,
            same_weekend_group=same_weekend_group,
            same_weekend_group_for_weekends=same_weekend_group_for_weekends,
            exclude_holidays=exclude_holidays,
            min_pool_size=min_pool_size,
            funnel=funnel,
        )
        if chosen is None or len(chosen) == 0:
            continue

        n = len(chosen)
        weighted_sum = np.zeros(n, dtype=float)
        weight_sum = np.zeros(n, dtype=float)

        for group_name, cols in spec.feature_groups.items():
            w = float(weights.get(group_name, 0.0))
            if w <= 0:
                continue
            present = [c for c in cols if c in chosen.columns and c in q_row.index]
            if not present:
                continue
            pool_vals = chosen[present].to_numpy(dtype=float)
            query_vals = np.asarray([q_row[c] for c in present], dtype=float)
            means, stds = _fit_zscore(pool_vals)
            pool_z = (pool_vals - means) / stds
            query_z = (query_vals - means) / stds
            d = _nan_aware_distance(query_z, pool_z)
            finite = np.isfinite(d)
            weighted_sum[finite] += w * d[finite]
            weight_sum[finite] += w

        distances = np.full(n, np.inf, dtype=float)
        valid = weight_sum > 0
        distances[valid] = weighted_sum[valid] / weight_sum[valid]

        candidate_dates = chosen["date"].to_list()
        distances = _calendar.linear_age_penalty(
            distances, candidate_dates, target_date, recency_half_life_days
        )

        chosen_local = chosen.copy()
        chosen_local["distance"] = distances
        chosen_local = chosen_local[np.isfinite(chosen_local["distance"])]
        if len(chosen_local) == 0:
            continue
        chosen_local = chosen_local.sort_values(
            ["distance", "date"], ascending=[True, False]
        ).head(int(n_analogs))
        if len(chosen_local) == 0:
            continue

        d = chosen_local["distance"].to_numpy(dtype=float)
        inv = 1.0 / np.square(np.maximum(d, 1e-8))
        wgt = inv / inv.sum()

        for rank, (_, r) in enumerate(chosen_local.iterrows(), start=1):
            rows.append(
                {
                    "hour_ending": h,
                    "rank": rank,
                    "date": r["date"],
                    "distance": float(r["distance"]),
                    "weight": float(wgt[rank - 1]),
                    "lmp": float(r.get("lmp", np.nan))
                    if pd.notna(r.get("lmp"))
                    else float("nan"),
                }
            )

    return pd.DataFrame(rows, columns=out_cols)
