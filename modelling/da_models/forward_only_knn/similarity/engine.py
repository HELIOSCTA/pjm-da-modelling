"""Forward-only analog selection engine."""
from __future__ import annotations

import logging
from datetime import date

import numpy as np
import pandas as pd

from da_models.forward_only_knn import configs
from da_models.forward_only_knn.similarity import filtering, metrics

logger = logging.getLogger(__name__)


def _safe_int(value: object, fallback: int) -> int:
    """Convert nullable numeric-like value to int with fallback."""
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return fallback
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _resolved_groups(
    pool: pd.DataFrame,
    query: pd.Series,
    feature_weights: dict[str, float],
) -> dict[str, list[str]]:
    """Resolve active feature columns by group for this pool/query pair."""
    groups: dict[str, list[str]] = {}
    for group_name, weight in feature_weights.items():
        if weight <= 0:
            continue
        cols = [c for c in configs.FEATURE_GROUPS.get(group_name, []) if c in pool.columns and c in query.index]
        if not cols:
            continue
        groups[group_name] = cols
    return groups


def _circular_day_distance(day_of_year: np.ndarray, target_day_of_year: int) -> np.ndarray:
    """Circular day-of-year distance with 366-day wraparound."""
    direct = np.abs(day_of_year - float(target_day_of_year))
    return np.minimum(direct, 366.0 - direct)


def _compute_distances(
    query: pd.Series,
    pool: pd.DataFrame,
    groups: dict[str, list[str]],
    feature_weights: dict[str, float],
) -> np.ndarray:
    """Compute weighted NaN-aware distances with pool-only normalization."""
    n = len(pool)
    weighted_sum = np.zeros(n, dtype=float)
    weight_sum = np.zeros(n, dtype=float)

    for group_name, cols in groups.items():
        weight = float(feature_weights.get(group_name, 0.0))
        if weight <= 0:
            continue

        pool_vals = pool[cols].to_numpy(dtype=float)
        query_vals = query[cols].to_numpy(dtype=float)

        means, stds = metrics.fit_pool_zscore(pool_vals)
        pool_scaled = metrics.apply_zscore(pool_vals, means, stds)
        query_scaled = metrics.apply_zscore(query_vals.reshape(1, -1), means, stds).reshape(-1)

        for i in range(n):
            dist, n_valid = metrics.nan_aware_euclidean(query_scaled, pool_scaled[i])
            if n_valid == 0 or np.isnan(dist):
                continue
            weighted_sum[i] += weight * dist
            weight_sum[i] += weight

    distances = np.full(n, np.inf, dtype=float)
    valid = weight_sum > 0
    distances[valid] = weighted_sum[valid] / weight_sum[valid]
    return distances


def find_twins(
    query: pd.Series,
    pool: pd.DataFrame,
    target_date: date,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    feature_weights: dict[str, float] | None = None,
    min_pool_size: int = configs.MIN_POOL_SIZE,
    same_dow_group: bool = configs.FILTER_SAME_DOW_GROUP,
    exclude_holidays: bool = configs.FILTER_EXCLUDE_HOLIDAYS,
    season_window_days: int = configs.FILTER_SEASON_WINDOW_DAYS,
    exclude_dates: list[date] | None = None,
    recency_half_life_days: int = configs.RECENCY_HALF_LIFE_DAYS,
    weight_method: str = "inverse_distance",
    apply_outage_regime_filter: bool = configs.FILTER_OUTAGE_REGIME,
    outage_tolerance_std: float = configs.FILTER_OUTAGE_TOLERANCE_STD,
    outage_filter_col: str = configs.FILTER_OUTAGE_COL,
) -> pd.DataFrame:
    """Find nearest historical delivery days to query conditions."""
    if feature_weights is None:
        feature_weights = dict(configs.FEATURE_GROUP_WEIGHTS)

    work = pool.copy()
    work = work[pd.to_datetime(work["date"]).dt.date < target_date].copy()

    if exclude_dates:
        work = work[~work["date"].isin(exclude_dates)].copy()

    if season_window_days > 0:
        target_doy = pd.Timestamp(target_date).dayofyear
        work_doy = pd.to_datetime(work["date"]).dt.dayofyear.to_numpy(dtype=float)
        keep = _circular_day_distance(work_doy, target_doy) <= float(season_window_days)
        work = work[keep].copy()
        logger.info(
            "Season window filter: +/- %s days kept %s candidates",
            season_window_days,
            len(work),
        )

    if len(work) == 0:
        raise ValueError(
            "No historical pool rows available before target_date after season-window filtering"
        )

    target_dow = _safe_int(query.get("day_of_week_number"), (target_date.weekday() + 1) % 7)
    target_dow_group = _safe_int(query.get("dow_group"), 0)
    target_is_holiday = bool(_safe_int(query.get("is_nerc_holiday"), 0))

    filtered = filtering.apply_filter_ladder(
        pool=work,
        target_dow=target_dow,
        target_dow_group=target_dow_group,
        target_is_holiday=target_is_holiday,
        min_pool_size=min_pool_size,
        same_dow_group=same_dow_group,
        exclude_holidays=exclude_holidays,
    )
    if len(filtered) == 0:
        raise ValueError("No candidates after calendar filtering")

    if apply_outage_regime_filter:
        # Preserve the calendar-filtered pool so the fallback keeps the DOW
        # match if the outage filter cuts below min_pool_size.
        pre_outage_pool = filtered.copy()
        target_outage = query.get(outage_filter_col)
        try:
            target_outage_val = (
                None if target_outage is None or pd.isna(target_outage)
                else float(target_outage)
            )
        except (TypeError, ValueError):
            target_outage_val = None
        filtered, og_stats = filtering.outage_regime_filter(
            pool=filtered,
            target_outage=target_outage_val,
            outage_col=outage_filter_col,
            tolerance_std=outage_tolerance_std,
        )
        if og_stats["applied"]:
            logger.info(
                "Outage regime filter: %s -> %s (target=%.0f MW, z=%+.2f, tol=%.2f)",
                og_stats["before"], og_stats["after"],
                og_stats["target_outage"], og_stats["z_target"],
                og_stats["tolerance_std"],
            )
        else:
            logger.info(
                "Outage regime filter skipped: %s",
                og_stats.get("skipped_reason", "unknown"),
            )
        filtered = filtering.ensure_minimum_pool(
            filtered, pre_outage_pool, target_date, min_size=min_pool_size,
        )

    if len(filtered) == 0:
        raise ValueError("No candidates after outage regime filtering")

    groups = _resolved_groups(filtered, query, feature_weights)
    if not groups:
        raise ValueError("No active feature groups with shared columns between pool and query")

    distances = _compute_distances(query, filtered, groups, feature_weights)

    age_days = (pd.to_datetime(target_date) - pd.to_datetime(filtered["date"])).dt.days.to_numpy(dtype=float)
    age_days = np.maximum(age_days, 0.0)
    recency_multiplier = 1.0 + (age_days / float(max(recency_half_life_days, 1)))
    distances = distances * recency_multiplier

    ranked = filtered.copy()
    ranked["distance"] = distances
    ranked = ranked[np.isfinite(ranked["distance"])].copy()
    if len(ranked) == 0:
        raise ValueError("All candidate distances are invalid (no overlapping non-NaN features)")

    ranked["date"] = pd.to_datetime(ranked["date"]).dt.date
    ranked = ranked.sort_values(["distance", "date"], kind="mergesort").reset_index(drop=True)

    n_select = min(n_analogs, len(ranked))
    ranked = ranked.head(n_select).copy()

    top_dist = ranked["distance"].to_numpy(dtype=float)
    top_weights = metrics.compute_analog_weights(top_dist, method=weight_method)
    ranked["weight"] = top_weights
    ranked["rank"] = np.arange(1, len(ranked) + 1)

    d_min = float(top_dist.min())
    d_max = float(top_dist.max())
    if d_max > d_min:
        ranked["similarity"] = 1.0 - ((ranked["distance"] - d_min) / (d_max - d_min))
    else:
        ranked["similarity"] = 1.0

    keep_cols = ["date", "rank", "distance", "similarity", "weight"] + [
        c for c in configs.LMP_LABEL_COLUMNS if c in ranked.columns
    ]
    logger.info(
        "Found %s twins for %s (pool=%s, groups=%s)",
        len(ranked),
        target_date,
        len(filtered),
        len(groups),
    )
    return ranked[keep_cols]


def find_analogs(
    pool: pd.DataFrame,
    query: pd.Series,
    *,
    target_date: date,
    config: configs.ForwardOnlyKNNConfig,
) -> pd.DataFrame:
    """Backward-compatible wrapper around ``find_twins``."""
    weights = config.resolved_feature_weights(
        include_gas=True,
        include_outages=True,
        include_renewables=True,
    )
    return find_twins(
        query=query,
        pool=pool,
        target_date=target_date,
        n_analogs=config.n_analogs,
        feature_weights=weights,
        min_pool_size=config.min_pool_size,
        same_dow_group=config.same_dow_group,
        exclude_holidays=config.exclude_holidays,
        season_window_days=config.season_window_days,
        recency_half_life_days=config.recency_half_life_days,
        weight_method=config.weight_method,
        apply_outage_regime_filter=config.apply_outage_regime_filter,
        outage_tolerance_std=config.outage_tolerance_std,
        outage_filter_col=config.outage_filter_col,
    )
