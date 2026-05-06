"""Engine for pjm_rto_hourly — long-format per-HE matching.

After the T4 wide→long cutover, the pool is one row per (date, hour_ending)
and the query is a 24-row DataFrame. The engine filters the pool to the
target HE, computes a per-group NaN-aware sum-Euclidean distance against
the query at that HE, and combines groups via spec weights. Same K=20-
per-HE structure as before; ``flt_radius`` / windowed-vs-broadcast split
are gone (everything is "scalar at HE" now).

Sunny parity highlights:
  - Per-group z-fit on the HE-filtered pool subset.
  - Sum-Euclidean per group (no /n_valid normalization).
  - Linear pre-selection age penalty: ``d *= 1 + age_days / half_life``.
  - Inverse-distance squared analog weighting: ``w_i ∝ 1/(d_i + ε)²``.
  - Tie-break by date desc on equal distance (newer wins).
  - Filter ladder + holiday-mirror semantics in calendar.apply_calendar_filter.
"""

from __future__ import annotations

import logging
import warnings
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
    pool_long: pd.DataFrame,
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
    """Apply chronological / season / calendar filters to a long-format pool.

    The filter functions in ``calendar.apply_calendar_filter`` operate on
    a date-level frame. Here we extract unique dates, run the filters,
    then reduce the long pool to the surviving dates.
    """
    if "date" not in pool_long.columns:
        raise ValueError("_candidate_pool: long pool missing 'date' column")

    # Date-level subset for filter logic.
    work_dates = (
        pool_long[["date"]].drop_duplicates().sort_values("date").reset_index(drop=True)
    )
    before_chrono = len(work_dates)
    work_dates = work_dates[
        pd.to_datetime(work_dates["date"]).dt.date < target_date
    ].copy()
    if funnel is not None:
        funnel.record(
            "chronological cut",
            f"date < target ({target_date})",
            before=before_chrono,
            after=len(work_dates),
        )
    if len(work_dates) == 0:
        return pool_long.iloc[0:0]

    # Season window (sunny parity: applied before the calendar ladder).
    if season_window_days > 0:
        target_doy = pd.Timestamp(target_date).dayofyear
        doys = pd.to_datetime(work_dates["date"]).dt.dayofyear.to_numpy(dtype=float)
        keep = _circular_day_distance(doys, target_doy) <= float(season_window_days)
        candidates = work_dates[keep]
        before_season = len(work_dates)
        if len(candidates) >= min_pool_size:
            work_dates = candidates.copy()
            if funnel is not None:
                funnel.record(
                    "season window",
                    f"+/-{season_window_days}d (DOY circular)",
                    before=before_season,
                    after=len(work_dates),
                )
        else:
            if funnel is not None:
                funnel.record(
                    "season window",
                    f"+/-{season_window_days}d (DOY circular)",
                    before=before_season,
                    after=before_season,
                    relaxed=True,
                    would_survive=len(candidates),
                )
        if len(work_dates) == 0:
            return pool_long.iloc[0:0]

    # Calendar filter ladder.
    needs_filter = (
        same_dow_group
        or same_weekend_group
        or same_weekend_group_for_weekends
        or exclude_holidays
        or exclude_dates
        or max_age_years
    )
    if needs_filter and (dates_meta is not None or max_age_years):
        work_dates = _calendar.apply_calendar_filter(
            pool=work_dates,
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

    if len(work_dates) == 0:
        return pool_long.iloc[0:0]

    surviving = set(work_dates["date"].tolist())
    return pool_long[pool_long["date"].isin(surviving)].reset_index(drop=True)


def _effective_weights(
    spec: ModelSpec,
    override: dict[str, float] | None,
) -> dict[str, float]:
    """Resolved feature-group weights for this run.

    With ``override=None``, returns the spec-derived (already-renormalized)
    weights. With an override dict, validates that every key is a valid
    spec group, fills missing keys with 0, then renormalizes to sum to 1.0.
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


def find_twins(
    query: pd.DataFrame,
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
    """Per-HE analog table on the long pool. Shape: 24 * n_analogs rows.

    Columns: ``hour_ending``, ``rank``, ``date``, ``distance``, ``weight``,
    ``lmp``.
    """
    out_cols = ["hour_ending", "rank", "date", "distance", "weight", "lmp"]

    weights = _effective_weights(spec, feature_group_weights_override)

    if funnel is not None:
        funnel.record(
            "raw history",
            f"build_pool: {pool['date'].nunique()} dates with feature coverage",
            before=pool["date"].nunique(),
            after=pool["date"].nunique(),
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

    # Index query rows by HE for quick lookup.
    if "hour_ending" not in query.columns:
        raise ValueError(
            "find_twins: query must be a long-format DataFrame with"
            " 'hour_ending' column (one row per HE)"
        )
    query_by_he = {int(r["hour_ending"]): r for _, r in query.iterrows()}

    rows: list[dict] = []
    group_weights: dict[str, float] = dict(weights)

    for h in HOURS:
        work_he = work[work["hour_ending"] == h]
        if len(work_he) == 0:
            continue
        if h not in query_by_he:
            continue
        query_he = query_by_he[h]

        # Per-group weighted sum-Euclidean across groups. Each group
        # produces a NaN-aware d_group; groups combine via spec weights
        # as a *sum* (no /contributed normalization) so partial coverage
        # cannot underprice a row. Rows that fail to contribute to every
        # non-zero-weight group get d=inf and are dropped — partial-
        # coverage rows would otherwise beat fully-covered ones (e.g. a
        # 2010 row with only calendar features matching DOW exactly
        # would land at d=0 while a 2024 row with full coverage hits a
        # higher honest distance).
        nonzero_groups = [
            (g, c)
            for g, c in spec.feature_groups.items()
            if group_weights.get(g, 0.0) > 0
        ]
        d = np.zeros(len(work_he), dtype=float)
        full_coverage = np.ones(len(work_he), dtype=bool)

        for group_name, group_cols in nonzero_groups:
            w = float(group_weights[group_name])
            cols_present = [
                c for c in group_cols if c in work_he.columns and c in query_he.index
            ]
            if not cols_present:
                # Group has zero columns under this query/pool — treat as
                # uniformly missing across all rows (no contribution, no
                # exclusion either, since nothing to match against).
                continue

            pool_vals = work_he[cols_present].to_numpy(dtype=float)
            query_vals = np.asarray([query_he[c] for c in cols_present], dtype=float)

            # Query NaN-mask: if the query is fully missing for this group
            # at this HE (e.g. load_ramp_* at HE=1 needs prior-day load
            # which the query frame doesn't carry), there is nothing to
            # match against — skip the group rather than letting it
            # collapse the full-coverage gate to all-False.
            query_finite = ~np.isnan(query_vals)
            if not query_finite.any():
                continue
            cols_present = [c for c, ok in zip(cols_present, query_finite) if ok]
            pool_vals = pool_vals[:, query_finite]
            query_vals = query_vals[query_finite]

            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    category=RuntimeWarning,
                    message="Mean of empty slice",
                )
                warnings.filterwarnings(
                    "ignore",
                    category=RuntimeWarning,
                    message="Degrees of freedom <= 0",
                )
                means = np.nanmean(pool_vals, axis=0)
                stds = np.nanstd(pool_vals, axis=0)
            stds = np.where(stds == 0, 1.0, stds)
            pool_z = (pool_vals - means) / stds
            query_z = (query_vals - means) / stds

            diff = query_z - pool_z
            mask = ~np.isnan(diff)
            sq = np.where(mask, diff**2, 0.0)
            n_valid = mask.sum(axis=1)
            with np.errstate(invalid="ignore"):
                d_group = np.where(
                    n_valid > 0,
                    np.sqrt(sq.sum(axis=1)),
                    np.nan,
                )

            finite = np.isfinite(d_group)
            d[finite] += w * d_group[finite]
            full_coverage &= finite

        d = np.where(full_coverage, d, np.inf)

        # Linear pre-selection age penalty (sunny parity).
        if recency_half_life_days and recency_half_life_days > 0:
            finite_mask = np.isfinite(d)
            if finite_mask.any():
                d = d.copy()
                pool_dates = work_he["date"].to_list()
                d[finite_mask] = _calendar.linear_age_penalty(
                    d[finite_mask],
                    [pool_dates[i] for i in np.flatnonzero(finite_mask)],
                    target_date,
                    float(recency_half_life_days),
                )

        # Sort by (distance asc, date desc) — newer wins on ties.
        date_ord = np.array(
            [pd.Timestamp(d_).toordinal() for d_ in work_he["date"]],
            dtype=np.int64,
        )
        order = np.lexsort((-date_ord, d))
        order = order[np.isfinite(d[order])]
        order = order[:n_analogs]
        if len(order) == 0:
            continue

        d_top = d[order]
        eps = 1e-6
        inv_dist = 1.0 / (d_top + eps) ** 2
        if inv_dist.sum() <= 0:
            analog_weights = np.full(len(d_top), 1.0 / max(1, len(d_top)))
        else:
            analog_weights = inv_dist / inv_dist.sum()

        # Materialize analog rows.
        for rank, (idx_arr, dist, w_blend) in enumerate(
            zip(order, d_top, analog_weights), start=1
        ):
            row = work_he.iloc[int(idx_arr)]
            lmp_val = row.get("lmp", np.nan)
            rows.append(
                {
                    "hour_ending": h,
                    "rank": rank,
                    "date": row["date"],
                    "distance": float(dist),
                    "weight": float(w_blend),
                    "lmp": float(lmp_val) if pd.notna(lmp_val) else float("nan"),
                }
            )

    return pd.DataFrame(rows, columns=out_cols)
