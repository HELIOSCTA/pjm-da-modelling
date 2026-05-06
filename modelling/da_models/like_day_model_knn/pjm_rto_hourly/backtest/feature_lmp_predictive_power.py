"""Cross-day feature -> LMP predictive-power diagnostic for pjm_rto_hourly.

Sibling diagnostic ``feature_lmp_correlation.py`` correlates a feature's
24-hour profile against LMP's 24-hour profile **within a single day**.
That correlation is contaminated by the diurnal confound: load and LMP
both peak HE17-19 by physics, so a high within-day Pearson reflects the
shared HE-of-day cycle as much as any actual feature -> price signal.

This script flips the axis. For each ``(feature, HE)`` pair, it
correlates ``feature_h{HE}`` against ``lmp_h{HE}`` **across dates in the
post-funnel KNN candidate set** (n = candidates the engine actually
sees for ``TARGET_DATE``, typically a few hundred), with HE held fixed.

The pool funnel — chronological cut, recency cap, NERC holiday
exclusion, same-DOW group, season window — is the exact same one
``find_twins`` runs in production, reused here via
``pjm_rto_hourly.engine._candidate_pool`` plus a ``FunnelCounts``
accumulator so the candidate set the diagnostic measures is bit-for-bit
the set the KNN engine searches. This dispatches the diurnal (HE held
fixed), seasonal (season window), and DoW (same DoW group) confounds
simultaneously, leaving only the cross-day variation the engine can
actually exploit.

Reported as a single unified per-HE table (one block, four metric
rows per feature):

  - distance   = Mantel pairwise-distance Pearson  [similarity -> similarity]
  - std_ratio  = target-specific univariate K-NN LMP-std compression
                 (K=N; <1 = compresses)
  - pearson    = level/linear association              [supporting]
  - spearman   = monotone/rank, outlier-robust         [supporting]

Distance correlation answers "does this feature carry signal in the
pool on average?" std_ratio answers the operationally-tighter
question: "for THIS target, does matching on this feature alone place
us in a low-LMP-variance neighborhood?" That second question is what
the production engine actually exploits at forecast time, so it leads
alongside distance correlation.

Pearson and Spearman are demoted to supporting diagnostics: KNN does
not exploit linear correlation directly, but they distinguish, e.g.,
a feature with a clean linear price relationship from one with a
V-shape that distance correlation captures and KNN can still exploit.

Aggregated per feature: OnPk (HE8..HE23), OffPk (HE1..HE7+HE24), Flat
(all 24) means of the per-HE correlations.

Plus a partial correlation block: ``corr(net_load_h{HE}, lmp_h{HE} |
load_h{HE})`` — net_load's marginal predictive power once load is
already accounted for. Closed-form OLS residuals; no scipy. (Partial
*distance* correlation is a research statistic; out of scope here.)

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.backtest.feature_lmp_predictive_power
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/backtest/feature_lmp_predictive_power.py
"""

from __future__ import annotations

import sys
import time
from dataclasses import replace
from datetime import date
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.common.stats.correlation import (  # noqa: E402
    distance_correlation,
    pearson,
    spearman,
)
from da_models.like_day_model_knn import _shared, configs  # noqa: E402
from da_models.like_day_model_knn.calendar import (  # noqa: E402
    FunnelCounts,
    resolve_day_type,
)
from da_models.like_day_model_knn.pjm_rto_hourly.builder import build_pool  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.engine import (  # noqa: E402
    _candidate_pool,
)
from da_models.like_day_model_knn.pjm_rto_hourly.printers import (  # noqa: E402
    print_pool_funnel,
)
from utils.logging_utils import (  # noqa: E402
    Colors,
    print_divider,
    print_header,
    print_section,
    supports_color,
)

_COLOR_ON: bool = supports_color()
_HL_LMP: str = (Colors.BOLD + Colors.BRIGHT_RED) if _COLOR_ON else ""
_HL_LEADER: str = Colors.BOLD if _COLOR_ON else ""
_HL_DIM: str = Colors.DIM if _COLOR_ON else ""
_RS: str = Colors.RESET if _COLOR_ON else ""


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = date(2026, 5, 6)
MODEL_NAME: str = configs.PJM_RTO_HOURLY_SUNNY_ALIGNED_SPEC.name
HUB: str = configs.HUB
FEATURES: tuple[str, ...] = ("load", "solar", "wind", "net_load")

# Number of nearest neighbors for the target-specific KNN-confidence
# diagnostic. Sourced from ``configs.DEFAULT_N_ANALOGS`` so this
# diagnostic uses the same K the production engine reads from
# ``KnnModelConfig`` when no override is given.
K_NEIGHBORS: int = configs.DEFAULT_N_ANALOGS

_HOURS: list[int] = list(range(1, 25))
_LMP_COLS: list[str] = [f"lmp_h{h}" for h in _HOURS]

_HOUR_BUCKETS: dict[str, list[int]] = {
    "OnPk": list(range(8, 24)),
    "OffPk": list(range(1, 8)) + [24],
    "Flat": list(_HOURS),
}

_DOW_ABBR: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _per_he_corrs(
    pool: pd.DataFrame,
    feature: str,
    target_stem: str,
    corr_fn,
) -> dict[int, float | None]:
    """For each HE, correlate ``feature_h{HE}`` with ``{target_stem}_h{HE}``
    across pool rows. Returns ``{HE: corr | None}``."""
    out: dict[int, float | None] = {}
    for h in _HOURS:
        fcol = f"{feature}_h{h}"
        tcol = f"{target_stem}_h{h}"
        if fcol not in pool.columns or tcol not in pool.columns:
            out[h] = None
            continue
        x = pool[fcol].to_numpy(dtype=float)
        y = pool[tcol].to_numpy(dtype=float)
        out[h] = corr_fn(x, y)
    return out


def _bucket_avg(
    per_he: dict[int, float | None], bucket_hours: list[int]
) -> float | None:
    """Mean of finite per-HE corrs over ``bucket_hours``. None if all
    bucket values are None / NaN."""
    vals = [per_he.get(h) for h in bucket_hours]
    finite = [v for v in vals if v is not None and np.isfinite(v)]
    if not finite:
        return None
    return float(np.mean(finite))


def _ols_residuals(y: np.ndarray, x: np.ndarray) -> np.ndarray | None:
    """Residuals of OLS y = a + b*x on the finite-pair mask. Returns
    None if <3 pairs or x has zero variance. Closed-form so we keep
    the no-scipy / no-sklearn rule."""
    mask = np.isfinite(y) & np.isfinite(x)
    if mask.sum() < 3:
        return None
    xv, yv = x[mask], y[mask]
    if np.std(xv) == 0:
        return None
    A = np.column_stack([np.ones_like(xv), xv])
    coef, *_ = np.linalg.lstsq(A, yv, rcond=None)
    fitted = A @ coef
    resid = np.full_like(x, np.nan, dtype=float)
    resid[mask] = yv - fitted
    return resid


def _partial_corr_per_he(
    pool: pd.DataFrame,
    target_stem: str,
    control: str,
    primary: str,
) -> dict[int, float | None]:
    """Per HE: corr(primary_h{HE}, target_h{HE} | control_h{HE}).

    Two OLS regressions per HE: target on control, primary on control.
    Pearson the two residual vectors. Returns ``{HE: corr | None}``.
    """
    out: dict[int, float | None] = {}
    for h in _HOURS:
        ccol = f"{control}_h{h}"
        pcol = f"{primary}_h{h}"
        tcol = f"{target_stem}_h{h}"
        if not all(c in pool.columns for c in (ccol, pcol, tcol)):
            out[h] = None
            continue
        c = pool[ccol].to_numpy(dtype=float)
        p = pool[pcol].to_numpy(dtype=float)
        t = pool[tcol].to_numpy(dtype=float)
        r_p = _ols_residuals(p, c)
        r_t = _ols_residuals(t, c)
        if r_p is None or r_t is None:
            out[h] = None
            continue
        out[h] = pearson(r_p, r_t)
    return out


def _knn_confidence_per_he(
    post_funnel_pool: pd.DataFrame,
    raw_pool: pd.DataFrame,
    target_date: date,
    feature: str,
    k: int,
) -> dict[int, dict | None]:
    """Univariate K-NN confidence for a single feature, per HE.

    For each HE: take the target's ``feature_h{HE}`` value, find the K
    pool rows (post-funnel) closest by absolute distance on that single
    column, then summarise the LMP distribution of those K neighbours.

    Returns ``{HE: {target_value, kth_distance, knn_lmp_mean,
    knn_lmp_std, std_ratio} | None}``. Per-HE result is ``None`` when
    the target feature value is missing, fewer than K finite
    feature/LMP rows survive, or pool LMP std is non-positive.

    Distance is raw absolute difference on the single feature column —
    production stacks ``flt_radius=1`` window cols and z-scores per
    pool, but per-feature univariate compression is well-defined on
    raw distances and saves us from picking an ad-hoc normalization
    here.
    """
    out: dict[int, dict | None] = {}
    target_rows = raw_pool[raw_pool["date"] == target_date]
    if len(target_rows) == 0:
        return {h: None for h in _HOURS}
    target_row = target_rows.iloc[0]

    for h in _HOURS:
        fcol = f"{feature}_h{h}"
        lcol = f"lmp_h{h}"
        if fcol not in post_funnel_pool.columns or lcol not in post_funnel_pool.columns:
            out[h] = None
            continue
        if fcol not in target_row.index:
            out[h] = None
            continue
        target_value = target_row[fcol]
        if pd.isna(target_value):
            out[h] = None
            continue
        target_value = float(target_value)

        feat_vals = post_funnel_pool[fcol].to_numpy(dtype=float)
        lmp_vals = post_funnel_pool[lcol].to_numpy(dtype=float)
        mask = np.isfinite(feat_vals) & np.isfinite(lmp_vals)
        if mask.sum() < k:
            out[h] = None
            continue

        feat_finite = feat_vals[mask]
        lmp_finite = lmp_vals[mask]
        distances = np.abs(feat_finite - target_value)
        nn_idx = np.argpartition(distances, k - 1)[:k]
        nn_dists = distances[nn_idx]
        nn_lmps = lmp_finite[nn_idx]

        pool_lmp_finite = lmp_vals[np.isfinite(lmp_vals)]
        pool_lmp_std = (
            float(np.std(pool_lmp_finite, ddof=0)) if pool_lmp_finite.size > 0 else 0.0
        )
        knn_lmp_std = float(np.std(nn_lmps, ddof=0))
        std_ratio: float | None
        if pool_lmp_std <= 0:
            std_ratio = None
        else:
            std_ratio = knn_lmp_std / pool_lmp_std

        out[h] = {
            "target_value": target_value,
            "kth_distance": float(nn_dists.max()),
            "knn_lmp_mean": float(nn_lmps.mean()),
            "knn_lmp_std": knn_lmp_std,
            "std_ratio": std_ratio,
        }
    return out


def _bucket_avg_std_ratio(
    per_he: dict[int, dict | None], bucket_hours: list[int]
) -> float | None:
    """Mean std_ratio over a bucket; None if no HE has a finite ratio."""
    vals: list[float] = []
    for h in bucket_hours:
        cell = per_he.get(h)
        if cell is None:
            continue
        r = cell.get("std_ratio")
        if r is None or not np.isfinite(r):
            continue
        vals.append(float(r))
    if not vals:
        return None
    return float(np.mean(vals))


# ── printing helpers ───────────────────────────────────────────────────────


def _corr_cell(v: float | None, width: int = 7) -> str:
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return f"{'n/a':>{width}}"
    return f"{v:>+{width}.3f}"


def _print_pool_summary(
    pool: pd.DataFrame,
    spec_name: str,
    hub: str,
    target_date: date | None,
    features: tuple[str, ...],
    flt_radius: int,
    n_pool: int,
    width: int = 100,
) -> None:
    spec = configs.MODEL_REGISTRY.get(spec_name)
    domain_list = ", ".join(spec.domains) if spec is not None else "?"
    dmin = pd.Timestamp(pool["date"].min()).date()
    dmax = pd.Timestamp(pool["date"].max()).date()
    target_str = (
        f"{target_date} ({_DOW_ABBR[target_date.weekday()]})"
        if target_date is not None
        else "n/a (no funnel applied)"
    )

    print()
    print_header("FEATURE -> LMP PREDICTIVE POWER CONFIGURATION", "=", width)
    print()
    print(f"  Target date     {target_str}")
    print(f"  Hub             {hub}")
    print(f"  Spec            {spec_name}")
    print(f"  Domains         {domain_list}")
    print(f"  flt_radius      {flt_radius}  (seasonal window halfwidth, days)")
    print(f"  Pool size       {n_pool:,} rows  (post-funnel candidate set)")
    print(f"  Pool date range {dmin}  ->  {dmax}")
    print(f"  Features        {', '.join(features)}")
    print("  Targets         lmp_total")
    print("  Axis            cross-day per HE  (HE held fixed; n = post-funnel rows)")
    print(
        "  Buckets         OnPk = HE8..HE23 (16h)  |"
        "  OffPk = HE1..HE7 + HE24 (8h)  |  Flat = all 24"
    )
    print()

    print_section("Column presence check")
    for f in features:
        cols = [f"{f}_h{h}" for h in _HOURS]
        missing = [c for c in cols if c not in pool.columns]
        if missing:
            print(f"  {f:<10} MISSING: {len(missing)}/24 cols ({missing[0]} ...)")
        else:
            print(f"  {f:<10} 24/24 columns present")
    lmp_missing = [c for c in _LMP_COLS if c not in pool.columns]
    print(
        f"  {'lmp':<10} "
        + (
            f"MISSING: {len(lmp_missing)}/24"
            if lmp_missing
            else "24/24 columns present"
        )
    )
    print_divider("=", width, dim=False)


def _ratio_cell(v: float | None, width: int = 7) -> str:
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return f"{'n/a':>{width}}"
    return f"{v:>{width}.3f}"


def _print_combined_per_he_table(
    features_in_order: tuple[str, ...],
    metrics_per_feature: dict[str, list[tuple[str, dict[int, float | None], bool]]],
    title: str,
    width: int,
    feature_of_the_day_callout: str | None,
    legend: list[str],
) -> None:
    """Unified per-HE metrics table.

    ``metrics_per_feature[feat]`` is an ordered list of
    ``(metric_label, per_he_dict, is_signed)`` tuples. ``is_signed=True``
    -> ``+0.NNN`` (correlations); ``False`` -> ``0.NNN`` (std_ratio).
    """
    print()
    print_header(title, "=", width)
    print()
    for line in legend:
        print(line)
    if feature_of_the_day_callout:
        print()
        print(feature_of_the_day_callout)
    print()

    cell_w = 7
    metric_w = 10
    feat_w = max(8, max((len(f) for f in features_in_order), default=8))
    indent_w = max(feat_w, metric_w + 2)

    head = f"  {'':<{indent_w}} "
    for h in _HOURS:
        head += f"{'HE' + str(h):>{cell_w}} "
    head += f"|  {'OnPk':>{cell_w}} {'OffPk':>{cell_w}} {'Flat':>{cell_w}}"
    print(head)
    print_divider("-", len(head), dim=False)

    for feat in features_in_order:
        print(f"  {feat}")
        for metric_label, per_he, is_signed in metrics_per_feature[feat]:
            cell_fn = _corr_cell if is_signed else _ratio_cell
            line = f"    {metric_label:<{metric_w}}{'':<{indent_w - metric_w - 2}} "
            for h in _HOURS:
                line += f"{cell_fn(per_he.get(h), cell_w)} "
            on_avg = _bucket_avg(per_he, _HOUR_BUCKETS["OnPk"])
            off_avg = _bucket_avg(per_he, _HOUR_BUCKETS["OffPk"])
            flat_avg = _bucket_avg(per_he, _HOUR_BUCKETS["Flat"])
            line += (
                f"|  {cell_fn(on_avg, cell_w)} "
                f"{cell_fn(off_avg, cell_w)} "
                f"{cell_fn(flat_avg, cell_w)}"
            )
            print(line)
        print_divider("-", len(head), dim=False)


def _print_partial_correlations(
    partial_lmp: dict[int, float | None],
    width: int,
) -> None:
    print()
    print_header(
        "PARTIAL CORRELATION  --  corr(net_load, lmp | load)",
        "=",
        width,
    )
    print(
        "  Net_load's MARGINAL predictive power, after load is already accounted for."
    )
    print(
        "  Computed per HE via OLS: regress net_load on load, regress lmp on"
        " load, Pearson the two residual vectors."
    )
    print(
        "  ~0  -> net_load adds nothing beyond load (renewables either tiny or"
        " redundant given load)."
    )
    print(
        "  +/- -> net_load carries an independent signal (renewables move price"
        " through the supply curve in a way load alone doesn't capture)."
    )
    print()

    cell_w = 8
    head = f"  {'':<6} "
    for h in _HOURS:
        head += f"{'HE' + str(h):>{cell_w}} "
    head += f"{'OnPk':>{cell_w}} {'OffPk':>{cell_w}} {'Flat':>{cell_w}}"
    print(head)
    print_divider("-", len(head), dim=False)

    line = f"  {'corr':<6} "
    for h in _HOURS:
        line += f"{_corr_cell(partial_lmp.get(h), cell_w)} "
    line += (
        f"{_corr_cell(_bucket_avg(partial_lmp, _HOUR_BUCKETS['OnPk']), cell_w)} "
        f"{_corr_cell(_bucket_avg(partial_lmp, _HOUR_BUCKETS['OffPk']), cell_w)} "
        f"{_corr_cell(_bucket_avg(partial_lmp, _HOUR_BUCKETS['Flat']), cell_w)}"
    )
    print(line)
    print_divider("-", len(head), dim=False)


# ── pool funnel reuse ──────────────────────────────────────────────────────


def _apply_knn_funnel(
    raw_pool: pd.DataFrame,
    spec,
    target_date: date,
    cache_dir: Path | None,
) -> tuple[pd.DataFrame, FunnelCounts, str]:
    """Run the production KNN funnel on ``raw_pool`` for ``target_date``.

    Reuses ``engine._candidate_pool`` so the candidate set this diagnostic
    measures is identical to the one ``find_twins`` searches in
    ``forecast_single_day.run``. Stage 0 (raw history) is recorded here
    to mirror what ``find_twins`` does before delegating to
    ``_candidate_pool`` for the remaining stages.

    Knobs come from package defaults (``MAX_AGE_YEARS``,
    ``FILTER_EXCLUDE_HOLIDAYS``, ``FILTER_SAME_DOW_GROUP``,
    ``SEASON_WINDOW_DAYS``, ``MIN_POOL_SIZE``) — same source the
    production ``KnnModelConfig`` reads from when no override is given.
    """
    funnel = FunnelCounts()
    funnel.record(
        "raw history",
        f"build_pool: {len(raw_pool)} dates with feature coverage",
        before=len(raw_pool),
        after=len(raw_pool),
    )
    dates_meta = _shared.load_dates_daily(cache_dir)
    work = _candidate_pool(
        raw_pool,
        target_date,
        season_window_days=configs.SEASON_WINDOW_DAYS,
        min_pool_size=configs.MIN_POOL_SIZE,
        dates_meta=dates_meta,
        same_dow_group=configs.FILTER_SAME_DOW_GROUP,
        same_weekend_group=configs.FILTER_SAME_WEEKEND_GROUP,
        same_weekend_group_for_weekends=configs.FILTER_SAME_WEEKEND_GROUP_FOR_WEEKENDS,
        exclude_holidays=configs.FILTER_EXCLUDE_HOLIDAYS,
        exclude_dates=list(configs.EXCLUDE_DATES),
        max_age_years=configs.MAX_AGE_YEARS,
        funnel=funnel,
    )
    return work, funnel, resolve_day_type(target_date)


# ── main ───────────────────────────────────────────────────────────────────


def run(
    target_date: date | None = TARGET_DATE,
    features: tuple[str, ...] = FEATURES,
    model_name: str = MODEL_NAME,
    hub: str = HUB,
) -> dict:
    """Cross-day per-HE feature -> LMP predictive-power diagnostic.

    Returns a dict with the per-HE Pearson/Spearman/distance tables,
    per-HE partial correlations, and the resolved pool. Prints all
    blocks to stdout. No artefacts written.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if target_date is None:
        raise ValueError(
            "target_date is required: the diagnostic operates on the "
            "post-funnel KNN candidate set, which is target-specific."
        )

    base_spec = configs.MODEL_REGISTRY[model_name]
    spec_for_build = replace(base_spec, flt_radius=int(base_spec.flt_radius))

    print("\n[predpow] building pool...")
    t0 = time.perf_counter()
    raw_pool = build_pool(spec=spec_for_build, cache_dir=configs.CACHE_DIR)
    print(
        f"[predpow] pool built in {time.perf_counter() - t0:.1f}s "
        f"({len(raw_pool)} raw rows)"
    )

    pool, funnel, day_type = _apply_knn_funnel(
        raw_pool, spec_for_build, target_date, configs.CACHE_DIR
    )
    print(
        f"[predpow] funnel applied: {len(raw_pool)} -> {len(pool)} "
        f"candidates for target_date={target_date} ({day_type})"
    )
    print(f"[predpow] features: {features}")

    width = 100

    pearson_lmp: dict[str, dict[int, float | None]] = {
        f: _per_he_corrs(pool, f, "lmp", pearson) for f in features
    }
    spearman_lmp: dict[str, dict[int, float | None]] = {
        f: _per_he_corrs(pool, f, "lmp", spearman) for f in features
    }
    distance_lmp: dict[str, dict[int, float | None]] = {
        f: _per_he_corrs(pool, f, "lmp", distance_correlation) for f in features
    }
    knn_conf: dict[str, dict[int, dict | None]] = {
        f: _knn_confidence_per_he(pool, raw_pool, target_date, f, K_NEIGHBORS)
        for f in features
    }
    std_ratio_lmp: dict[str, dict[int, float | None]] = {
        f: {
            h: (cell["std_ratio"] if cell is not None else None)
            for h, cell in knn_conf[f].items()
        }
        for f in features
    }

    partial_lmp = _partial_corr_per_he(
        pool, target_stem="lmp", control="load", primary="net_load"
    )

    onpk_ratios: dict[str, float | None] = {
        f: _bucket_avg_std_ratio(knn_conf[f], _HOUR_BUCKETS["OnPk"]) for f in features
    }
    finite_onpk = {
        f: r for f, r in onpk_ratios.items() if r is not None and np.isfinite(r)
    }
    fotd_callout: str | None = None
    if finite_onpk:
        winner = min(finite_onpk, key=lambda f: finite_onpk[f])
        fotd_callout = (
            f"  Feature of the day for OnPk: {_HL_LEADER}{winner}{_RS}"
            f" (std_ratio_onpk = {finite_onpk[winner]:.2f})"
        )

    _print_pool_summary(
        pool=pool,
        spec_name=model_name,
        hub=hub,
        target_date=target_date,
        features=features,
        flt_radius=int(base_spec.flt_radius),
        n_pool=len(pool),
        width=width,
    )
    print_pool_funnel(funnel, target_date, day_type, hub)

    metrics_per_feature: dict[str, list[tuple[str, dict[int, float | None], bool]]] = {
        f: [
            ("distance", distance_lmp[f], True),
            ("std_ratio", std_ratio_lmp[f], False),
            ("pearson", pearson_lmp[f], True),
            ("spearman", spearman_lmp[f], True),
        ]
        for f in features
    }
    legend = [
        "  Four metrics per feature:",
        "    distance   = Mantel pairwise-distance Pearson"
        " (KNN-relevant: similarity -> similarity)",
        "    std_ratio  = target-specific univariate K-NN LMP-std"
        f" compression (K={K_NEIGHBORS}; <1 = compresses)",
        "    pearson    = level/linear association"
        " (diagnostic - KNN does not exploit directly)",
        "    spearman   = monotone/rank association"
        " (diagnostic - outlier-robust Pearson)",
    ]
    _print_combined_per_he_table(
        features_in_order=features,
        metrics_per_feature=metrics_per_feature,
        title="FEATURE -> LMP CROSS-DAY METRICS  --  per HE summary",
        width=width,
        feature_of_the_day_callout=fotd_callout,
        legend=legend,
    )

    _print_partial_correlations(partial_lmp, width)
    print()

    return {
        "pool": pool,
        "funnel": funnel,
        "pearson_lmp": pearson_lmp,
        "spearman_lmp": spearman_lmp,
        "distance_lmp": distance_lmp,
        "knn_conf": knn_conf,
        "partial_lmp": partial_lmp,
        "target_date": target_date,
    }


if __name__ == "__main__":
    raise NotImplementedError(
        "T4: needs long-format migration. Pool is now row-per-(date, HE)"
        " with scalar feature cols; this script's wide-format reads"
        " (load_h*, lmp_h* etc.) and per-HE Spearman aggregation must"
        " be rewritten for the long schema. Slated for T4 Session 2."
    )
    run()
