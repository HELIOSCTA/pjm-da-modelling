"""Single-day feature-ablation diagnostic for pjm_rto_hourly KNN.

Sibling diagnostic ``feature_lmp_predictive_power.py`` measures
**marginal** feature -> LMP signal (does the feature carry information
about LMP, on its own, on the post-funnel pool). That is the right
question for ranking candidate features, but it is NOT the question
"does adding this feature improve the production forecast?" — that
needs a like-for-like ablation against the actual engine, weights and
all. This script is that ablation.

For one target date with known actuals, run the production engine
N+1 times — once with the full spec (baseline), then once per feature
family with that family removed — and compare hourly forecasts.
A feature earns its slot if removing it materially worsens MAE / rMAE;
it does not if removing it leaves metrics unchanged or improves them.

Output structure mirrors ``forecast_single_day.py`` for the first two
sections (configuration block, pool funnel) so the ablation reads as a
natural extension of the production forecast — same look-and-feel up to
where production prints its single forecast row. The ablation then
**stacks** the per-scenario forecast rows in one hourly table
(``Date | Type | HE1..HE24 | OnPeak | OffPeak | Flat``), with one
``Actual`` row at the top and one ``Forecast`` row per scenario sorted
by delta_mae desc — directly comparable across scenarios at a glance.
A compact metrics block follows for MAE / rMAE / CRPS / coverage.

Two ablation mechanisms, applied per feature type:

  Windowed features (load / solar / wind / net_load) — NaN-out the
    feature's columns in the pool and query before passing to
    ``find_twins``. The engine z-scores per pool with NaN-aware
    masking (``engine.py`` ~L311-322 and L211-215), so columns that
    are entirely NaN drop out of the windowed Euclidean cleanly.

  Broadcast features (outage / gas) — set the group's weight to 0
    via ``feature_group_weights_override``. The engine's
    ``_combined_non_load_distance`` filters non-windowed groups by
    ``weight > 0`` (engine.py L187-191), so weight=0 is exactly
    equivalent to dropping the group.

The mismatch is intentional. Setting a windowed group's weight to 0
does NOT remove its columns from the windowed Euclidean —
``_WINDOWED_COL_STEMS`` in engine.py is hardcoded, weights only affect
the windowed-vs-broadcast weight split. For windowed features, NaN
ablation is the only clean removal; for broadcast features, the
weight override is cleaner because it preserves the broadcast group's
share of the combined distance for the remaining groups.

Reads pool, query, and dates_meta once; reuses across all scenarios
via ``forecast_single_day.run()``'s reusable-artefact kwargs. Single
build cost, ~6-12 forecasts.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.backtest.feature_ablation_single_day
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/backtest/feature_ablation_single_day.py
"""

from __future__ import annotations

import sys
import time
import warnings
from dataclasses import replace
from datetime import date
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.common.forecast.output import actuals_from_pool  # noqa: E402
from da_models.like_day_model_knn import _shared, configs  # noqa: E402
from da_models.like_day_model_knn.calendar import (  # noqa: E402
    FunnelCounts,
    resolve_day_type,
)
from da_models.like_day_model_knn.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool,
    build_query_row,
)
from da_models.like_day_model_knn.pjm_rto_hourly.engine import (  # noqa: E402
    _candidate_pool,
)
from da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as forecast_run,
)
from da_models.like_day_model_knn.pjm_rto_hourly.printers import (  # noqa: E402
    print_pool_funnel,
)
from utils.logging_utils import (  # noqa: E402
    Colors,
    print_divider,
    print_header,
    supports_color,
)

_COLOR_ON: bool = supports_color()
_HL_LEADER: str = Colors.BOLD if _COLOR_ON else ""
_HL_LOSS: str = Colors.BRIGHT_RED if _COLOR_ON else ""
_HL_WIN: str = Colors.BRIGHT_GREEN if _COLOR_ON else ""
_HL_ACTUAL: str = (Colors.BOLD + Colors.BRIGHT_RED) if _COLOR_ON else ""
_RS: str = Colors.RESET if _COLOR_ON else ""


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
# Single-day ablation needs a target with full actuals so MAE/rMAE/CRPS
# are defined. None -> most recent weekday in the pool with full LMP coverage.
TARGET_DATE: date | None = date(2026, 5, 6)

# FULL spec includes net_load_h1..net_load_h24 alongside load/solar/wind so
# net_load can be ablated as a peer of the components. Production today uses
# the 5-feature ``pjm_rto_hourly`` spec; switch only after ablation justifies it.
MODEL_NAME: str = configs.PJM_RTO_HOURLY_FULL_SPEC.name
HUB: str = configs.HUB

# Map ablation_name -> {nan_cols: [...]} OR {zero_weight_groups: [...]}.
# Windowed features must use nan_cols; broadcast features must use
# zero_weight_groups (see module docstring for why).
_LOAD_COLS: list[str] = [f"load_h{h}" for h in range(1, 25)]
_SOLAR_COLS: list[str] = [f"solar_h{h}" for h in range(1, 25)]
_WIND_COLS: list[str] = [f"wind_h{h}" for h in range(1, 25)]
_NET_LOAD_COLS: list[str] = [f"net_load_h{h}" for h in range(1, 25)]

ABLATIONS: dict[str, dict] = {
    "load": {"nan_cols": _LOAD_COLS},
    "solar": {"nan_cols": _SOLAR_COLS},
    "wind": {"nan_cols": _WIND_COLS},
    "net_load": {"nan_cols": _NET_LOAD_COLS},
    "outage": {"zero_weight_groups": ["outage_level"]},
    "gas": {"zero_weight_groups": ["gas_level"]},
}

_DOW_ABBR: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


# ── Ablation helpers ───────────────────────────────────────────────────────


def _apply_nan_ablation(
    pool: pd.DataFrame,
    query: pd.Series,
    cols: list[str],
) -> tuple[pd.DataFrame, pd.Series]:
    """Copy pool/query and set ``cols`` to NaN where present.

    Engine handles all-NaN columns by masking them out of the per-row
    Euclidean (``mask = ~np.isnan(diff)`` then ``n_valid > 0``). The
    feature drops out of the distance metric entirely.
    """
    pool_copy = pool.copy()
    present_pool = [c for c in cols if c in pool_copy.columns]
    if present_pool:
        pool_copy[present_pool] = np.nan
    query_copy = query.copy()
    for c in cols:
        if c in query_copy.index:
            query_copy[c] = np.nan
    return pool_copy, query_copy


def _zero_weight_override(
    spec,
    zero_groups: list[str],
) -> dict[str, float]:
    """Spec's raw weights with ``zero_groups`` set to 0.0.

    Returned dict is renormalized to sum=1.0 inside ``_effective_weights``;
    here we hand back raw multipliers so the override path is identical
    to the scenarios.py convention.
    """
    raw = dict(spec.raw_feature_group_weights)
    valid = set(raw.keys())
    bad = [g for g in zero_groups if g not in valid]
    if bad:
        raise ValueError(
            f"Unknown group(s) in zero_weight_groups: {bad}. Valid: {sorted(valid)}"
        )
    for g in zero_groups:
        raw[g] = 0.0
    return raw


def _resolve_target_date(pool: pd.DataFrame, target_date: date | None) -> date:
    """Default to the most recent weekday in the pool with full LMP actuals."""
    if target_date is not None:
        return target_date
    candidates = sorted(pool["date"].unique(), reverse=True)
    for d in candidates:
        d = d if isinstance(d, date) else pd.Timestamp(d).date()
        if d.weekday() >= 5:
            continue
        if actuals_from_pool(pool, d) is not None:
            return d
    raise RuntimeError(
        "No weekday pool dates with full LMP actuals; cannot default target_date."
    )


def _compute_funnel_for_display(
    pool: pd.DataFrame,
    target_date: date,
    dates_meta: pd.DataFrame,
) -> FunnelCounts:
    """Run the production funnel against ``pool`` so we can render it.

    Mirrors ``feature_lmp_predictive_power._apply_knn_funnel`` — calls
    ``_candidate_pool`` directly with a fresh ``FunnelCounts``, using
    package defaults so the displayed funnel matches what
    ``forecast_run`` (and therefore each scenario) actually sees. The
    funnel is scenario-independent because all ablation scenarios share
    the same pool + filter knobs.
    """
    funnel = FunnelCounts()
    funnel.record(
        "raw history",
        f"build_pool: {len(pool)} dates with feature coverage",
        before=len(pool),
        after=len(pool),
    )
    _candidate_pool(
        pool,
        target_date,
        season_window_days=configs.SEASON_WINDOW_DAYS,
        min_pool_size=configs.MIN_POOL_SIZE,
        dates_meta=dates_meta,
        same_dow_group=configs.FILTER_SAME_DOW_GROUP,
        exclude_holidays=configs.FILTER_EXCLUDE_HOLIDAYS,
        exclude_dates=list(configs.EXCLUDE_DATES),
        max_age_years=configs.MAX_AGE_YEARS,
        funnel=funnel,
    )
    return funnel


# ── Scenario execution ─────────────────────────────────────────────────────


def _run_scenario(
    *,
    scenario_name: str,
    target_date: date,
    pool: pd.DataFrame,
    query: pd.Series,
    dates_meta: pd.DataFrame,
    model_name: str,
    feature_group_weights_override: dict[str, float] | None,
) -> dict:
    """One forecast run, scenario-tagged. Captures metrics + duration + status
    + the canonical ``output_table`` (used by the hourly stack later)."""
    started = time.perf_counter()
    base: dict = {
        "scenario": scenario_name,
        "status": "ok",
        "error_message": None,
        "output_table": None,
        "analogs": None,
    }
    try:
        result = forecast_run(
            target_date=target_date,
            model_name=model_name,
            pool=pool,
            query=query,
            dates_meta=dates_meta,
            feature_group_weights_override=feature_group_weights_override,
            quiet=True,
            write_analog_store=False,
        )
    except Exception as exc:
        base.update(
            {
                "status": "failed",
                "error_message": f"{type(exc).__name__}: {exc}",
                "duration_s": round(time.perf_counter() - started, 3),
            }
        )
        return base

    metrics = result.get("metrics") or {}
    base.update(
        {
            "n_pool": result.get("n_pool"),
            "n_analogs_used": result.get("n_analogs_used"),
            "has_actuals": result.get("has_actuals"),
            "output_table": result.get("output_table"),
            "analogs": result.get("analogs"),
            "mae": metrics.get("mae"),
            "rmse": metrics.get("rmse"),
            "rmae": metrics.get("rmae"),
            "crps": metrics.get("crps"),
            "mean_pinball": metrics.get("mean_pinball"),
            "coverage_80pct": metrics.get("coverage_80pct"),
            "coverage_90pct": metrics.get("coverage_90pct"),
            "sharpness_80pct": metrics.get("sharpness_80pct"),
            "sharpness_90pct": metrics.get("sharpness_90pct"),
            "duration_s": round(time.perf_counter() - started, 3),
        }
    )
    return base


# ── Output ─────────────────────────────────────────────────────────────────


def _print_config(
    target_date: date,
    n_pool: int,
    model_name: str,
    ablations: dict[str, dict],
    width: int = 100,
) -> None:
    spec = configs.MODEL_REGISTRY.get(model_name)
    domain_list = ", ".join(spec.domains) if spec is not None else "?"
    target_dow = _DOW_ABBR[target_date.weekday()]

    print()
    print_header("FEATURE ABLATION CONFIGURATION", "=", width)
    print()
    print(f"  Target date     {target_date}  ({target_dow})")
    print(f"  Hub             {HUB}")
    print(f"  Spec            {model_name}")
    print(f"  Domains         {domain_list}")
    print(f"  Pool size       {n_pool:,} rows")
    print(f"  Scenarios       1 baseline + {len(ablations)} ablation(s)")
    print()
    print("  Ablation mechanisms:")
    for name, payload in ablations.items():
        if "nan_cols" in payload:
            mech = (
                f"NaN {len(payload['nan_cols'])} cols "
                "(windowed -> drop from per-HE Euclidean)"
            )
        elif "zero_weight_groups" in payload:
            groups = ", ".join(payload["zero_weight_groups"])
            mech = f"weight=0 for [{groups}] (broadcast -> filtered by weight>0)"
        else:
            mech = "?"
        print(f"    ablate_{name:<10} {mech}")
    print()
    print_divider("=", width, dim=False)


def _format_metric(v: float | None, width: int, fmt: str = ".2f") -> str:
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return f"{'n/a':>{width}}"
    return f"{v:>{width}{fmt}}"


def _format_pct(v: float | None, width: int) -> str:
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return f"{'n/a':>{width}}"
    return f"{v * 100:>{width - 1}.1f}%"


def _format_delta(v: float | None, width: int) -> str:
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return f"{'-':>{width}}"
    return f"{v:>+{width}.2f}"


def _scenario_perturbation(
    scenario_row: dict | None,
    baseline_row: dict | None,
) -> float | None:
    """Mean |scenario_forecast - baseline_forecast| across 24 HEs.

    Used as the sort key and headline magnitude when DA LMP actuals are
    not available (forecast-only mode). Measures *how much an ablation
    moves the forecast away from baseline*, regardless of whether the
    move is toward or away from truth — same diagnostic signal as
    delta_mae for ranking which features actively shape the forecast.
    """
    if scenario_row is None or baseline_row is None:
        return None
    s_ot = scenario_row.get("output_table")
    b_ot = baseline_row.get("output_table")
    if s_ot is None or b_ot is None:
        return None
    s_fc = s_ot[s_ot["Type"] == "Forecast"]
    b_fc = b_ot[b_ot["Type"] == "Forecast"]
    if not len(s_fc) or not len(b_fc):
        return None
    s = s_fc.iloc[0]
    b = b_fc.iloc[0]
    diffs: list[float] = []
    for h in range(1, 25):
        sv = s.get(f"HE{h}")
        bv = b.get(f"HE{h}")
        if pd.notna(sv) and pd.notna(bv):
            diffs.append(abs(float(sv) - float(bv)))
    return float(np.mean(diffs)) if diffs else None


def _ablation_sort_key(
    scenario_row: dict,
    baseline_row: dict | None,
    baseline_mae: float | None,
) -> float:
    """Order key for ablation rows: prefer signed delta_mae desc (largest
    worsening on top); fall back to forecast perturbation magnitude desc
    when actuals aren't available."""
    mae = scenario_row.get("mae")
    if pd.notna(mae) and baseline_mae is not None:
        return -(float(mae) - baseline_mae)
    pert = _scenario_perturbation(scenario_row, baseline_row)
    return -(pert if pert is not None else 0.0)


def _per_date_summary(analogs: pd.DataFrame) -> pd.DataFrame:
    """Aggregate analogs (24*K rows) to per-date summary, sorted by weight desc.

    Engine emits one analog row per (hour_ending, rank) — same date can
    appear in many HEs. Per-date weight is the sum across HEs; that's the
    same aggregation ``print_analog_features`` uses.
    """
    if analogs is None or len(analogs) == 0:
        return pd.DataFrame(
            columns=["date", "n_hours", "mean_distance", "summed_weight", "w_norm"]
        )
    by_date = (
        analogs.groupby("date", as_index=False)
        .agg(
            n_hours=("hour_ending", "nunique"),
            mean_distance=("distance", "mean"),
            summed_weight=("weight", "sum"),
        )
        .sort_values("summed_weight", ascending=False)
        .reset_index(drop=True)
    )
    total = float(by_date["summed_weight"].sum())
    by_date["w_norm"] = by_date["summed_weight"] / total if total > 0 else 0.0
    return by_date


def _print_analog_weight_matrix(
    rows: list[dict],
    target_date: date,
    width: int = 140,
    max_swap_ins: int = 30,
    top_n_bold: int = 5,
) -> None:
    """Combined date x scenario weight matrix with per-column summary header.

    Header (top of table, before any date rows):
      - n_uniq        : total unique analog dates picked by each scenario
      - jaccard       : |scenario ∩ baseline| / |scenario ∪ baseline|
      - delta_mae     : MAE delta vs baseline (>0 = ablation worsens forecast)

    Body: one row per analog date any scenario picked; cells = % weight
    share within that scenario. **Bold** cells = the scenario's top-N
    picks (where this date is one of the highest-weighted analogs in
    that scenario's column). Sorted top-down by baseline weight desc;
    dates NOT in baseline appear at the bottom highlighted red — those
    are the swap-ins each ablation introduces by changing the distance
    metric.

    Combines what was previously two separate sections (per-scenario
    rollup + per-date matrix) into a single annotated table.
    """
    ok = [r for r in rows if r.get("status") == "ok" and r.get("analogs") is not None]
    if not ok:
        return

    baseline = next((r for r in ok if r["scenario"] == "baseline"), None)
    if baseline is None:
        return
    baseline_mae = float(baseline["mae"]) if pd.notna(baseline.get("mae")) else None

    summaries: dict[str, pd.DataFrame] = {
        r["scenario"]: _per_date_summary(r["analogs"]) for r in ok
    }
    baseline_dates: set = set(summaries["baseline"]["date"].tolist())

    # Column order: baseline first, then ablations sorted by delta_mae desc
    # (or by forecast perturbation desc when actuals are unavailable).
    ordered_scenarios: list[str] = ["baseline"]
    ablation_rows = [r for r in ok if r["scenario"] != "baseline"]
    ablation_rows.sort(key=lambda r: _ablation_sort_key(r, baseline, baseline_mae))
    ordered_scenarios.extend(r["scenario"] for r in ablation_rows)

    # Compact display labels: drop the redundant "ablate_" prefix so cells
    # align cleanly. Header still shows full scenario name once via legend.
    def _label(scen: str) -> str:
        return scen[len("ablate_") :] if scen.startswith("ablate_") else scen

    labels = {scen: _label(scen) for scen in ordered_scenarios}

    # Build {(scenario, date): w_norm} lookup.
    weights: dict[tuple[str, object], float] = {}
    all_dates: set = set()
    for scen, summary in summaries.items():
        for _, row in summary.iterrows():
            weights[(scen, row["date"])] = float(row["w_norm"])
            all_dates.add(row["date"])

    # Top-N picks per column for bold annotation.
    top_n_dates: dict[str, set] = {
        scen: set(summaries[scen].head(top_n_bold)["date"].tolist())
        for scen in ordered_scenarios
    }

    # Per-column summary stats (for header rows).
    n_uniq_by: dict[str, int] = {
        scen: len(summaries[scen]) for scen in ordered_scenarios
    }
    jaccard_by: dict[str, float | None] = {}
    for scen in ordered_scenarios:
        if scen == "baseline":
            jaccard_by[scen] = None
            continue
        scen_dates = set(summaries[scen]["date"].tolist())
        inter = baseline_dates & scen_dates
        union = baseline_dates | scen_dates
        jaccard_by[scen] = (len(inter) / len(union)) if union else 1.0
    delta_mae_by: dict[str, float | None] = {}
    delta_fcst_by: dict[str, float | None] = {}
    for scen in ordered_scenarios:
        if scen == "baseline":
            delta_mae_by[scen] = None
            delta_fcst_by[scen] = None
            continue
        scen_row = next((r for r in ok if r["scenario"] == scen), None)
        scen_mae = scen_row.get("mae") if scen_row is not None else None
        delta_mae_by[scen] = (
            (float(scen_mae) - baseline_mae)
            if (baseline_mae is not None and pd.notna(scen_mae))
            else None
        )
        delta_fcst_by[scen] = _scenario_perturbation(scen_row, baseline)
    has_actuals = baseline_mae is not None
    delta_label = "delta_mae" if has_actuals else "delta_fcst"
    delta_by = delta_mae_by if has_actuals else delta_fcst_by

    rows_in: list[tuple] = []
    rows_out: list[tuple] = []
    for d in all_dates:
        max_w = max(
            (weights.get((scen, d), 0.0) for scen in ordered_scenarios),
            default=0.0,
        )
        baseline_w = weights.get(("baseline", d))
        bucket = rows_in if d in baseline_dates else rows_out
        bucket.append((d, baseline_w, max_w))

    rows_in.sort(key=lambda t: -(t[1] or 0.0))
    rows_out.sort(key=lambda t: -t[2])

    # Always show all in-baseline rows (the baseline neighbour set is the
    # diagnostic anchor — truncating it would hide what the ablations are
    # being compared against). Cap swap-ins (out-of-baseline dates) at
    # ``max_swap_ins`` so a long tail of low-weight one-off picks doesn't
    # drown the table.
    n_swap_total = len(rows_out)
    rows_out = rows_out[:max_swap_ins]
    ordered_dates = rows_in + rows_out
    n_swap_hidden = max(0, n_swap_total - len(rows_out))

    print()
    print_header(
        f"FEATURE ABLATION ANALOG WEIGHT MATRIX  --  {target_date}",
        "=",
        width,
    )
    print()
    print(
        "  One row per analog date any scenario picked; cells = % share of"
        " that scenario's total analog weight."
    )
    print(
        f"  Bold cells = scenario's top-{top_n_bold} picks (highest-weighted"
        " in that column)."
    )
    print(
        "  Sorted top-down by baseline weight desc; dates NOT in baseline"
        " appear at the bottom in RED (those are the swap-ins each ablation"
        " introduces)."
    )
    if has_actuals:
        print(
            "  Header rows: n_uniq = total dates picked, jaccard = overlap"
            " with baseline's neighbour set, delta_mae = MAE move vs"
            " baseline (>0 worse, <0 better)."
        )
    else:
        print(
            "  Header rows: n_uniq = total dates picked, jaccard = overlap"
            " with baseline's neighbour set, delta_fcst = mean |scenario -"
            " baseline| over 24 HEs (forecast perturbation magnitude; no"
            " actuals available for delta_mae on this target)."
        )
    print("  ``—`` = scenario did not pick that date / metric undefined for baseline.")
    print()

    date_w = 12
    dow_w = 4
    label_span = date_w + 1 + dow_w  # date + sep + dow
    cell_w = max(9, max(len(label) for label in labels.values()) + 1)

    head = f"  {'date':<{date_w}} {'dow':<{dow_w}}"
    for scen in ordered_scenarios:
        head += f" {labels[scen]:>{cell_w}}"
    line_w = len(head)

    print(head)
    print_divider("-", line_w, dim=False)

    # Three summary rows at the top: n_uniq, jaccard, delta_mae.
    line = f"  {'n_uniq':<{label_span}}"
    for scen in ordered_scenarios:
        line += f" {n_uniq_by[scen]:>{cell_w}d}"
    print(f"{_HL_LEADER}{line}{_RS}")

    line = f"  {'jaccard':<{label_span}}"
    for scen in ordered_scenarios:
        v = jaccard_by[scen]
        cell = f"{'—':>{cell_w}}" if v is None else f"{v:>{cell_w}.2f}"
        line += f" {cell}"
    print(f"{_HL_LEADER}{line}{_RS}")

    line = f"  {delta_label:<{label_span}}"
    for scen in ordered_scenarios:
        v = delta_by[scen]
        if v is None:
            cell = f"{'—':>{cell_w}}"
        else:
            cell = f"{v:>+{cell_w}.2f}"
        line += f" {cell}"
    print(f"{_HL_LEADER}{line}{_RS}")

    print_divider("-", line_w, dim=False)

    n_in_baseline = sum(1 for d, _, _ in ordered_dates if d in baseline_dates)
    for i, (d, _, _) in enumerate(ordered_dates):
        d_obj = d if isinstance(d, date) else pd.Timestamp(d).date()
        dow = _DOW_ABBR[d_obj.weekday()]
        line = f"  {str(d_obj):<{date_w}} {dow:<{dow_w}}"
        for scen in ordered_scenarios:
            w = weights.get((scen, d))
            if w is None or not np.isfinite(w):
                cell = f"{'—':>{cell_w}}"
            else:
                cell_text = f"{w * 100:>{cell_w - 1}.1f}%"
                if d in top_n_dates[scen]:
                    cell = f"{_HL_LEADER}{cell_text}{_RS}"
                else:
                    cell = cell_text
            line += f" {cell}"
        is_in_baseline = d in baseline_dates
        if not is_in_baseline:
            line = f"{_HL_LOSS}{line}{_RS}"
        print(line)
        # Visual separator between in-baseline rows and the red swap-in tail.
        if i == n_in_baseline - 1 and n_in_baseline < len(ordered_dates):
            print_divider("-", line_w, dim=False)
            tail = (
                f"  -- {len(ordered_dates) - n_in_baseline} date(s) NOT in"
                " baseline (swap-ins from ablations) --"
            )
            print(f"{_HL_LOSS}{tail}{_RS}")

    print_divider("-", line_w, dim=False)
    if n_swap_hidden > 0:
        print(
            f"  ({n_swap_hidden} additional swap-in date(s) suppressed; raise"
            " max_swap_ins in _print_analog_weight_matrix to show all)"
        )


def _stacked_hourly_table(
    rows: list[dict],
    target_date: date,
) -> pd.DataFrame:
    """Stack one row per scenario into the canonical hourly layout.

    Pulls the ``Actual`` row from the baseline ``output_table`` (one
    copy at the top), then one ``Forecast`` row per scenario, relabeled
    to the scenario name. Rows ordered: Actual, baseline, then
    ablations sorted by delta_mae desc (matches the metrics block).
    """
    ok = [
        r for r in rows if r.get("status") == "ok" and r.get("output_table") is not None
    ]
    if not ok:
        return pd.DataFrame()

    baseline = next((r for r in ok if r["scenario"] == "baseline"), None)
    baseline_mae = (
        float(baseline["mae"])
        if baseline is not None and pd.notna(baseline.get("mae"))
        else None
    )

    cols = (
        ["Date", "Type"]
        + [f"HE{h}" for h in range(1, 25)]
        + ["OnPeak", "OffPeak", "Flat"]
    )

    table_rows: list[dict] = []
    if baseline is not None:
        actual_rows = baseline["output_table"][
            baseline["output_table"]["Type"] == "Actual"
        ]
        if len(actual_rows):
            ar = actual_rows.iloc[0].to_dict()
            ar["Date"] = str(target_date)
            ar["Type"] = "Actual"
            table_rows.append(ar)

    def _forecast_row(r: dict, label: str) -> dict | None:
        ot = r["output_table"]
        forecast = ot[ot["Type"] == "Forecast"]
        if not len(forecast):
            return None
        fr = forecast.iloc[0].to_dict()
        fr["Date"] = str(target_date)
        fr["Type"] = label
        return fr

    if baseline is not None:
        fr = _forecast_row(baseline, "baseline")
        if fr is not None:
            table_rows.append(fr)

    ablations = [r for r in ok if r["scenario"] != "baseline"]
    ablations.sort(key=lambda r: _ablation_sort_key(r, baseline, baseline_mae))
    for r in ablations:
        fr = _forecast_row(r, r["scenario"])
        if fr is not None:
            table_rows.append(fr)

    return pd.DataFrame(table_rows, columns=cols)


def _print_hourly_summary(
    rows: list[dict],
    target_date: date,
    width: int = 220,
) -> None:
    """Hourly forecast comparison — one Actual row + one row per
    scenario in the canonical Date | Type | HE1..HE24 | OnPk | OffPk | Flat
    layout. Mirrors ``print_forecast`` but stacked across scenarios so
    where each ablation diverges from the baseline is visible cell by
    cell. Sorted ablations by delta_mae desc (worst regressions on top).
    """
    ok = [r for r in rows if r.get("status") == "ok"]
    baseline = next((r for r in ok if r["scenario"] == "baseline"), None)
    baseline_mae = (
        float(baseline["mae"])
        if baseline is not None and pd.notna(baseline.get("mae"))
        else None
    )

    table = _stacked_hourly_table(rows, target_date)
    if table.empty:
        print("\n  (no successful scenarios; nothing to render)")
        return

    print()
    print_header(
        f"FEATURE ABLATION HOURLY FORECAST  --  {target_date}  ({HUB})",
        "=",
        width,
    )
    has_actuals = baseline_mae is not None
    print()
    if has_actuals:
        print(
            "  One Actual row + one Forecast row per scenario. Ablations"
            " sorted by delta_mae desc (worst regressions on top)."
        )
        print(
            "  Read down a column to see how each ablation perturbs that"
            " hour's forecast away from the baseline / actuals."
        )
    else:
        print(
            "  No DA LMP actuals available — Actual row omitted. Ablations"
            " sorted by delta_fcst desc (largest forecast perturbation on"
            " top)."
        )
        print(
            "  Read down a column to see how each ablation moves that hour's"
            " forecast away from baseline (correctness can't be evaluated)."
        )
    print()

    # Build column-aligned table identical to printers.print_forecast
    he_w = 6
    sum_w = 7
    type_w = 14
    header = f"{'Date':<12} {'Type':<{type_w}}"
    for h in range(1, 25):
        header += f" {h:>{he_w}}"
    for label in ("OnPk", "OffPk", "Flat"):
        header += f" {label:>{sum_w}}"
    table_w = len(header)

    print(header)
    print_divider("-", table_w, dim=False)

    for _, row in table.iterrows():
        is_actual = row["Type"] == "Actual"
        is_baseline = row["Type"] == "baseline"
        line = f"{str(row['Date']):<12} {str(row['Type']):<{type_w}}"
        for h in range(1, 25):
            v = row.get(f"HE{h}")
            line += f" {v:>{he_w}.1f}" if pd.notna(v) else f" {'':>{he_w}}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            v = row.get(col)
            line += f" {v:>{sum_w}.2f}" if pd.notna(v) else f" {'':>{sum_w}}"

        if is_actual:
            line = f"{_HL_ACTUAL}{line}{_RS}"
        elif is_baseline:
            line = f"{_HL_LEADER}{line}{_RS}"
        else:
            scenario_row = next(
                (r for r in ok if r["scenario"] == row["Type"]),
                None,
            )
            if scenario_row is not None:
                if has_actuals and pd.notna(scenario_row.get("mae")):
                    d = float(scenario_row["mae"]) - baseline_mae
                    if d > 0.5:
                        line = f"{_HL_LOSS}{line}{_RS}"
                    elif d < -0.25:
                        line = f"{_HL_WIN}{line}{_RS}"
                else:
                    # No actuals: highlight by perturbation magnitude
                    # ($/MWh mean abs gap from baseline forecast).
                    pert = _scenario_perturbation(scenario_row, baseline)
                    if pert is not None and pert > 1.0:
                        line = f"{_HL_LOSS}{line}{_RS}"
        print(line)

    print_divider("-", table_w, dim=False)


def _print_metrics_summary(
    rows: list[dict],
    width: int = 110,
) -> None:
    """Compact per-scenario metrics block.

    Same numbers as the prior daily-summary table but without re-printing
    the hourly profile or its commentary — the hourly section above
    carries the visual story; this is the numerical leaderboard.
    """
    df = pd.DataFrame(rows)
    ok = df[df["status"] == "ok"].copy()
    failed = df[df["status"] == "failed"]

    print()
    print_header("PER-SCENARIO METRICS", "=", width)
    print()

    if "baseline" not in ok["scenario"].values:
        print("  No successful baseline run; cannot compute deltas.")
        if len(ok):
            with pd.option_context("display.max_rows", None, "display.width", None):
                print(ok.to_string(index=False))
        if len(failed):
            print("\n  Failed scenarios:")
            for _, r in failed.iterrows():
                print(f"    {r['scenario']:<20} {r['error_message']}")
        return

    baseline_mae_val = ok.loc[ok["scenario"] == "baseline", "mae"].iloc[0]
    has_actuals = pd.notna(baseline_mae_val)
    if not has_actuals:
        # Forecast-only mode: MAE/rMAE/CRPS undefined (no actuals). Print a
        # compact perturbation leaderboard instead of the full metrics block.
        rows_dict = {r["scenario"]: r for r in rows if r.get("status") == "ok"}
        baseline_row = rows_dict.get("baseline")
        ordered = sorted(
            (s for s in rows_dict if s != "baseline"),
            key=lambda s: -(_scenario_perturbation(rows_dict[s], baseline_row) or 0.0),
        )

        print(
            "  No DA LMP actuals on this target — MAE/rMAE/CRPS undefined."
            " Showing forecast-perturbation magnitudes instead:"
        )
        print()
        name_w = 16
        head = f"  {'scenario':<{name_w}} {'analogs':>8} {'delta_fcst':>11} {'sec':>5}"
        print(head)
        print_divider("-", len(head), dim=False)
        baseline_n = (
            int(baseline_row["n_analogs_used"])
            if baseline_row and pd.notna(baseline_row.get("n_analogs_used"))
            else 0
        )
        baseline_dur = (
            f"{float(baseline_row['duration_s']):>5.1f}"
            if baseline_row and pd.notna(baseline_row.get("duration_s"))
            else "  n/a"
        )
        bl_line = f"  {'baseline':<{name_w}} {baseline_n:>8d} {'-':>11} {baseline_dur}"
        print(f"{_HL_LEADER}{bl_line}{_RS}")
        for scen in ordered:
            r = rows_dict[scen]
            pert = _scenario_perturbation(r, baseline_row)
            line = (
                f"  {scen:<{name_w}} "
                f"{int(r['n_analogs_used']) if pd.notna(r.get('n_analogs_used')) else 0:>8d} "
                f"{_format_delta(pert, 11)} "
                f"{_format_metric(r.get('duration_s'), 5, '.1f')}"
            )
            if pert is not None and pert > 1.0:
                line = f"{_HL_LOSS}{line}{_RS}"
            print(line)
        print_divider("-", len(head), dim=False)
        print()
        print(
            "  delta_fcst = mean |scenario - baseline| over 24 HEs."
            " Larger = the ablation moved the forecast more, i.e. that"
            " feature was actively shaping baseline's output."
        )
        print(
            "  Without actuals you can't tell direction (toward or away from"
            " truth); pair with the analog-weight matrix above to see which"
            " dates each ablation swapped in/out and decide whether the"
            " perturbation is plausibly an improvement."
        )
        if len(failed) > 0:
            print()
            print("  Failed scenarios:")
            for _, r in failed.iterrows():
                print(f"    {r['scenario']:<20} {r['error_message']}")
        return

    baseline_mae = float(baseline_mae_val)
    baseline_rmae = ok.loc[ok["scenario"] == "baseline", "rmae"].iloc[0]
    baseline_rmae = float(baseline_rmae) if pd.notna(baseline_rmae) else None
    ok["delta_mae"] = ok["mae"].astype(float) - baseline_mae
    if baseline_rmae is not None:
        ok["delta_rmae"] = ok["rmae"].astype(float) - baseline_rmae
    else:
        ok["delta_rmae"] = np.nan

    baseline_row = ok[ok["scenario"] == "baseline"]
    ablation_rows = ok[ok["scenario"] != "baseline"].sort_values(
        "delta_mae", ascending=False, na_position="last"
    )
    ordered = pd.concat([baseline_row, ablation_rows], ignore_index=True)

    name_w = 16
    head = (
        f"  {'scenario':<{name_w}} "
        f"{'analogs':>8} "
        f"{'mae':>9} "
        f"{'rmse':>9} "
        f"{'rmae':>7} "
        f"{'crps':>8} "
        f"{'cov_90':>7} "
        f"{'sharp_90':>9} "
        f"{'d_mae':>9} "
        f"{'d_rmae':>9} "
        f"{'sec':>5}"
    )
    print(head)
    print_divider("-", len(head), dim=False)

    for _, r in ordered.iterrows():
        is_baseline = r["scenario"] == "baseline"
        line = (
            f"  {str(r['scenario']):<{name_w}} "
            f"{int(r['n_analogs_used']) if pd.notna(r['n_analogs_used']) else 0:>8d} "
            f"{_format_metric(r['mae'], 9)} "
            f"{_format_metric(r['rmse'], 9)} "
            f"{_format_metric(r['rmae'], 7, '.3f')} "
            f"{_format_metric(r['crps'], 8, '.3f')} "
            f"{_format_pct(r['coverage_90pct'], 7)} "
            f"{_format_metric(r['sharpness_90pct'], 9)} "
            f"{_format_delta(r['delta_mae'] if not is_baseline else None, 9)} "
            f"{_format_delta(r['delta_rmae'] if not is_baseline else None, 9)} "
            f"{_format_metric(r['duration_s'], 5, '.1f')}"
        )
        if is_baseline:
            line = f"{_HL_LEADER}{line}{_RS}"
        else:
            d = r["delta_mae"]
            if pd.notna(d):
                if d > 0.5:
                    line = f"{_HL_LOSS}{line}{_RS}"
                elif d < -0.25:
                    line = f"{_HL_WIN}{line}{_RS}"
        print(line)

    print_divider("-", len(head), dim=False)
    print()
    print(
        "  Read: ablations sorted by delta_mae desc. The feature whose removal "
        "hurts MAE the most is at the top — that feature carries the most "
        "independent signal for THIS target."
    )
    print(
        "  Features near the bottom (delta_mae near 0 or negative) are either "
        "redundant with another feature or actively adding noise."
    )
    print(
        "  WARNING: this is one date. A feature that looks dead here may matter "
        "on summer scarcity days; re-run across a holdout window before "
        "changing the spec defaults."
    )

    if len(failed) > 0:
        print()
        print("  Failed scenarios:")
        for _, r in failed.iterrows():
            print(f"    {r['scenario']:<20} {r['error_message']}")


# ── main ───────────────────────────────────────────────────────────────────


def run(
    target_date: date | None = TARGET_DATE,
    ablations: dict[str, dict] | None = None,
    model_name: str = MODEL_NAME,
) -> dict:
    """Single-day ablation. Returns ``{rows: list[dict], target_date,
    n_pool, ablations, hourly_table, funnel}``. Prints config + funnel
    + hourly forecast comparison + metrics summary to stdout."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if ablations is None:
        ablations = ABLATIONS

    base_spec = configs.MODEL_REGISTRY[model_name]
    spec_for_build = replace(base_spec, flt_radius=int(base_spec.flt_radius))

    print("\n[ablate] building pool + dates_meta...")
    t0 = time.perf_counter()
    pool = build_pool(spec=spec_for_build, cache_dir=configs.CACHE_DIR)
    dates_meta = _shared.load_dates_daily(configs.CACHE_DIR)
    print(f"[ablate] pool built in {time.perf_counter() - t0:.1f}s ({len(pool)} rows)")

    resolved = _resolve_target_date(pool, target_date)
    if target_date is None:
        print(
            f"[ablate] target_date=None -> most recent weekday w/ actuals: {resolved}"
        )
    else:
        print(f"[ablate] target_date={resolved}")

    has_actuals = actuals_from_pool(pool, resolved) is not None
    if not has_actuals:
        print(
            f"[ablate] target_date={resolved} has no DA LMP actuals — running"
            " forecast-only mode. Sorting and headline metric switch from"
            " delta_mae to delta_fcst (mean |scenario - baseline| over 24 HEs)."
        )

    query = build_query_row(
        target_date=resolved,
        cache_dir=configs.CACHE_DIR,
        spec=spec_for_build,
    )

    funnel = _compute_funnel_for_display(pool, resolved, dates_meta)
    day_type = resolve_day_type(resolved)

    _print_config(
        target_date=resolved,
        n_pool=len(pool),
        model_name=model_name,
        ablations=ablations,
    )
    print_pool_funnel(funnel, resolved, day_type, HUB)

    rows: list[dict] = []

    print(f"\n[ablate] running baseline + {len(ablations)} ablation(s)...")

    # NaN-ablated columns trigger ``RuntimeWarning: Mean of empty slice`` and
    # ``Degrees of freedom <= 0`` from numpy's nanmean/nanvar inside the
    # engine's z-score step. The engine's NaN-aware mask handles them
    # correctly downstream — distances stay finite for unablated cols and
    # the ablated cols cleanly drop out. Suppress the noise here so the
    # per-scenario stdout summary is readable.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=RuntimeWarning, message="Mean of empty slice"
        )
        warnings.filterwarnings(
            "ignore", category=RuntimeWarning, message="Degrees of freedom <= 0"
        )
        rows.append(
            _run_scenario(
                scenario_name="baseline",
                target_date=resolved,
                pool=pool,
                query=query,
                dates_meta=dates_meta,
                model_name=model_name,
                feature_group_weights_override=None,
            )
        )
        last = rows[-1]
        print(
            f"[ablate]   baseline             "
            f"MAE={_format_metric(last.get('mae'), 7).strip():<7}  "
            f"({last['duration_s']:.1f}s)"
        )

        for name, payload in ablations.items():
            scenario_name = f"ablate_{name}"
            if "nan_cols" in payload:
                pool_run, query_run = _apply_nan_ablation(
                    pool, query, payload["nan_cols"]
                )
                override = None
            elif "zero_weight_groups" in payload:
                pool_run, query_run = pool, query
                override = _zero_weight_override(
                    spec_for_build, payload["zero_weight_groups"]
                )
            else:
                raise ValueError(
                    f"Ablation '{name}' has no recognised payload "
                    f"(expected 'nan_cols' or 'zero_weight_groups')."
                )
            rows.append(
                _run_scenario(
                    scenario_name=scenario_name,
                    target_date=resolved,
                    pool=pool_run,
                    query=query_run,
                    dates_meta=dates_meta,
                    model_name=model_name,
                    feature_group_weights_override=override,
                )
            )
            last = rows[-1]
            print(
                f"[ablate]   {scenario_name:<20} "
                f"MAE={_format_metric(last.get('mae'), 7).strip():<7}  "
                f"({last['duration_s']:.1f}s)"
            )

    _print_analog_weight_matrix(rows, resolved)
    _print_hourly_summary(rows, resolved)
    _print_metrics_summary(rows)
    print()

    hourly_table = _stacked_hourly_table(rows, resolved)

    return {
        "rows": rows,
        "target_date": resolved,
        "n_pool": len(pool),
        "ablations": ablations,
        "hourly_table": hourly_table,
        "funnel": funnel,
    }


if __name__ == "__main__":
    run()
