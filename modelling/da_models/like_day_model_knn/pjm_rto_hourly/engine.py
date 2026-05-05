"""Engine for pjm_rto_hourly - 3-hour window features x per-hour matching (AnEn-NWP-style).

One match per (target_date, target_hour). For each target HE h, the
feature vector is the windowed cols (load_h*, solar_h*, wind_h*) at
hours [h - flt_radius, h + flt_radius] (clipped to [1, 24]).

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

from da_models.common.configs import HOURS
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
    same_weekend_group: bool = False,
    same_weekend_group_for_weekends: bool = False,
    exclude_holidays: bool = False,
    exclude_dates: list[str] | None = None,
    max_age_years: int | None = None,
    funnel: _calendar.FunnelCounts | None = None,
) -> pd.DataFrame:
    work = pool.copy()
    before_chrono = len(work)
    work = work[pd.to_datetime(work["date"]).dt.date < target_date].copy()
    if funnel is not None:
        funnel.record(
            "chronological cut",
            f"date < target ({target_date})",
            before=before_chrono,
            after=len(work),
        )
    if len(work) == 0:
        return work
    needs_filter = (
        same_dow_group
        or same_weekend_group
        or same_weekend_group_for_weekends
        or exclude_holidays
        or exclude_dates
        or max_age_years
    )
    if needs_filter and (dates_meta is not None or max_age_years):
        work = _calendar.apply_calendar_filter(
            pool=work,
            target_date=target_date,
            dates_meta=dates_meta,
            same_dow_group=same_dow_group,
            same_weekend_group=same_weekend_group,
            same_weekend_group_for_weekends=same_weekend_group_for_weekends,
            exclude_holidays=exclude_holidays,
            exclude_dates=exclude_dates,
            max_age_years=max_age_years,
            min_pool_size=min_pool_size,
            funnel=funnel,
        )
        if len(work) == 0:
            return work
    if season_window_days > 0:
        target_doy = pd.Timestamp(target_date).dayofyear
        doys = pd.to_datetime(work["date"]).dt.dayofyear.to_numpy(dtype=float)
        keep = _circular_day_distance(doys, target_doy) <= float(season_window_days)
        candidates = work[keep]
        before_season = len(work)
        if len(candidates) >= min_pool_size:
            work = candidates.copy()
            logger.info(
                "pjm_rto_hourly season window +/-%dd kept %d candidates",
                season_window_days,
                len(work),
            )
            if funnel is not None:
                funnel.record(
                    "season window",
                    f"+/-{season_window_days}d (DOY circular)",
                    before=before_season,
                    after=len(work),
                )
        else:
            logger.warning(
                "pjm_rto_hourly season window kept only %d candidates "
                "(< min %d) - falling back to full history (%d)",
                len(candidates),
                min_pool_size,
                len(work),
            )
            if funnel is not None:
                funnel.record(
                    "season window",
                    f"+/-{season_window_days}d (DOY circular)",
                    before=before_season,
                    after=before_season,
                    relaxed=True,
                    would_survive=len(candidates),
                )
    return work


# Group prefixes whose features participate in the per-HE dynamic window
# (vs. the broadcast non-load path). Each prefix corresponds to a column
# stem of the form ``{stem}_h{HE}``. Adding a new windowed feature family
# means: define domain cols as ``{stem}_h{HE}``, name groups so they
# start with ``{stem}_`` (or ARE ``{stem}``), and add the stem here.
#
# Note on naming: groups like ``load_ramp_1h`` and ``load_ramp_3h`` start
# with ``load_`` so they pass the prefix check below — but their stems
# are the full ``load_ramp_1h`` / ``load_ramp_3h`` strings. The
# ``_WINDOWED_COL_STEMS`` list determines what cols ``_window_columns``
# emits per HE; the prefix list only gates ``_is_windowed_group``.
_WINDOWED_GROUP_PREFIXES: tuple[str, ...] = (
    "load_",
    "solar_",
    "wind_",
    "net_load_",
    "temp_",
)
_WINDOWED_COL_STEMS: tuple[str, ...] = (
    "load",
    "load_ramp_1h",
    "load_ramp_3h",
    "solar",
    "wind",
    "net_load",
    "temp",
)


def _is_windowed_group(group_name: str) -> bool:
    return any(group_name.startswith(p) for p in _WINDOWED_GROUP_PREFIXES)


def _window_columns(target_hour: int, flt_radius: int) -> list[str]:
    """Windowed feature column names for a target hour and +/- flt_radius window.

    Returns one set of cols per windowed stem (load_h, solar_h, wind_h).
    Caller filters down to columns actually present in the pool/query.
    """
    lo = max(1, target_hour - flt_radius)
    hi = min(24, target_hour + flt_radius)
    return [f"{stem}_h{h}" for stem in _WINDOWED_COL_STEMS for h in range(lo, hi + 1)]


def _effective_weights(
    spec: ModelSpec,
    override: dict[str, float] | None,
) -> dict[str, float]:
    """Resolved feature-group weights for this run.

    With ``override=None``, returns the spec-derived (already-renormalized)
    weights. With an override dict, validates that every key is a valid
    spec group, fills missing keys with 0, then renormalizes to sum to 1.0
    — same convention as ``domains.resolved_feature_group_weights``.
    Raises ``ValueError`` on unknown keys or zero/negative total.
    """
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


def _combined_non_load_distance(
    spec: ModelSpec,
    pool: pd.DataFrame,
    query: pd.Series,
    weights: dict[str, float],
) -> tuple[np.ndarray | None, float]:
    """Weighted-average per-group RMS-z distance over broadcast (non-windowed) groups.

    Broadcast group features (e.g. outage_level, gas_level) are constant
    across target hours, so the combined distance is computed once per
    pool row and reused for all 24 target hours. Returns
    ``(distance_array, total_weight)``; ``distance_array`` is ``None``
    when the spec has no broadcast groups.
    """
    non_load_groups = [
        (g, float(weights.get(g, 0.0)))
        for g in spec.feature_groups
        if not _is_windowed_group(g) and float(weights.get(g, 0.0)) > 0
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
        sq = np.where(mask, diff**2, 0.0)
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


def find_twins(
    query: pd.Series,
    pool: pd.DataFrame,
    target_date: date,
    spec: ModelSpec = configs.PJM_RTO_HOURLY_SPEC,
    n_analogs: int = configs.DEFAULT_N_ANALOGS,
    season_window_days: int = configs.SEASON_WINDOW_DAYS,
    min_pool_size: int = configs.MIN_POOL_SIZE,
    dates_meta: pd.DataFrame | None = None,
    same_dow_group: bool = False,
    same_weekend_group: bool = False,
    same_weekend_group_for_weekends: bool = False,
    exclude_holidays: bool = False,
    exclude_dates: list[str] | None = None,
    max_age_years: int | None = None,
    recency_half_life_days: float = configs.RECENCY_HALF_LIFE_DAYS,
    feature_group_weights_override: dict[str, float] | None = None,
    funnel: _calendar.FunnelCounts | None = None,
) -> pd.DataFrame:
    """Per-hour analog table. Shape: 24 * n_analogs rows.

    Columns: hour_ending, rank, date, distance, weight, lmp.

    ``feature_group_weights_override`` (when given) replaces the
    spec-derived weights for this call only. Validated and renormalized
    via ``_effective_weights``.

    ``funnel`` (when given) accumulates per-stage candidate counts as the
    pool is filtered down — see ``calendar.FunnelCounts``. Stage 0 (raw
    history) is recorded here; subsequent stages are recorded inside
    ``_candidate_pool`` and ``apply_calendar_filter``.
    """
    out_cols = ["hour_ending", "rank", "date", "distance", "weight", "lmp"]

    weights = _effective_weights(spec, feature_group_weights_override)

    if funnel is not None:
        funnel.record(
            "raw history",
            f"build_pool: {len(pool)} dates with feature coverage",
            before=len(pool),
            after=len(pool),
        )

    work = _candidate_pool(
        pool,
        target_date,
        season_window_days,
        min_pool_size,
        dates_meta=dates_meta,
        same_dow_group=same_dow_group,
        same_weekend_group=same_weekend_group,
        same_weekend_group_for_weekends=same_weekend_group_for_weekends,
        exclude_holidays=exclude_holidays,
        exclude_dates=exclude_dates,
        max_age_years=max_age_years,
        funnel=funnel,
    )
    if len(work) == 0:
        logger.warning(
            "pjm_rto_hourly: pool has no rows before target_date=%s",
            target_date,
        )
        return pd.DataFrame(columns=out_cols)

    flt_radius = int(spec.flt_radius)
    rows: list[dict] = []

    # Pre-compute broadcast (non-windowed) groups' combined distance (constant
    # across hours). Windowed groups (load/solar/wind/net_load) are evaluated
    # per HE below via a per-group weighted RMS-z so spec.feature_group_weights
    # actually controls each group's share of the windowed distance — the
    # prior implementation pooled all windowed cols into one RMS-z, which
    # discarded within-windowed group weights and gave wind/solar a column-
    # count-driven share (~3.2x their spec-intended weight on the FULL spec).
    non_load_dist, non_load_weight = _combined_non_load_distance(
        spec, work, query, weights
    )
    # Snapshot spec weights before the per-HE loop reassigns ``weights`` to
    # the analog inverse-distance weights at the bottom of each iteration.
    # (Renamed the analog vector to ``analog_weights`` below; the snapshot
    # remains for safety against future refactors.)
    group_weights: dict[str, float] = dict(weights)
    load_weight = sum(
        float(w) for g, w in group_weights.items() if _is_windowed_group(g)
    )
    if load_weight <= 0:
        # No windowed groups in the spec; treat the dynamic window as the full
        # remaining weight so distances stay finite.
        load_weight = max(0.0, 1.0 - non_load_weight)

    for h in HOURS:
        window_cols_set = set(_window_columns(h, flt_radius))

        # Per-group weighted RMS-z. Each windowed group computes a NaN-aware
        # RMS-z over the intersection of its cols with the per-HE window
        # (z-fit per group → scale-invariant per group), then groups combine
        # via spec weights. The windowed-vs-broadcast outer combine below is
        # unchanged.
        weighted_d = np.zeros(len(work), dtype=float)
        contributed = np.zeros(len(work), dtype=float)

        for group_name, group_cols in spec.feature_groups.items():
            if not _is_windowed_group(group_name):
                continue
            w = float(group_weights.get(group_name, 0.0))
            if w <= 0:
                continue
            windowed = [c for c in group_cols if c in window_cols_set]
            cols_present = [
                c for c in windowed if c in work.columns and c in query.index
            ]
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
            sq = np.where(mask, diff**2, 0.0)
            n_valid = mask.sum(axis=1)
            with np.errstate(invalid="ignore", divide="ignore"):
                d_group = np.where(
                    n_valid > 0,
                    np.sqrt(sq.sum(axis=1) / n_valid),
                    np.nan,
                )

            finite = np.isfinite(d_group)
            weighted_d[finite] += w * d_group[finite]
            contributed[finite] += w

        # Combine groups: weighted average over groups that had any finite
        # contribution. Rows with no windowed group contribution get inf.
        denom = np.where(contributed > 0, contributed, 1.0)
        d = np.where(contributed > 0, weighted_d / denom, np.inf)

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

        # Linear pre-selection age penalty: ``d *= 1 + age_days / half_life``.
        # Applied BEFORE top-N argsort so older candidates are less likely
        # to be picked at all. Replaces the previous post-selection
        # exponential weight decay (which only changed how analogs blended,
        # not which were selected). Faithful to sunny's
        # ``calendar.linear_age_penalty``.
        if recency_half_life_days and recency_half_life_days > 0:
            finite_mask = np.isfinite(d)
            if finite_mask.any():
                d = d.copy()
                pool_dates = work["date"].to_list()
                d[finite_mask] = _calendar.linear_age_penalty(
                    d[finite_mask],
                    [pool_dates[i] for i in np.flatnonzero(finite_mask)],
                    target_date,
                    float(recency_half_life_days),
                )

        order = np.argsort(d)
        order = order[np.isfinite(d[order])]
        order = order[:n_analogs]
        if len(order) == 0:
            continue

        d_top = d[order]
        eps = 1e-6
        inv_dist = 1.0 / (d_top + eps)
        if inv_dist.sum() <= 0:
            analog_weights = np.full(len(d_top), 1.0 / max(1, len(d_top)))
        else:
            analog_weights = inv_dist / inv_dist.sum()

        lmp_col = f"lmp_h{h}"
        for rank, (idx_arr, dist, w) in enumerate(
            zip(order, d_top, analog_weights), start=1
        ):
            row = work.iloc[int(idx_arr)]
            rows.append(
                {
                    "hour_ending": h,
                    "rank": rank,
                    "date": row["date"],
                    "distance": float(dist),
                    "weight": float(w),
                    "lmp": float(row.get(lmp_col, np.nan))
                    if lmp_col in row.index
                    else float("nan"),
                }
            )

    return pd.DataFrame(rows, columns=out_cols)
