"""Single-day multi-scenario backtest for pjm_rto_hourly.

Runs many (weight + knob) scenarios against ONE target date with known
actuals, then ranks them by MAE / rMAE / CRPS and surfaces each
scenario's forecast OnPeak / OffPeak / Flat alongside the actuals.

Differs from ``backtest/param_sweep.py``:

  - param_sweep walks across MANY weekday targets and aggregates
    metrics — used to pick weights that generalize.
  - single_day_backtest pins ONE target date and shows per-scenario
    forecast detail — used for "how would each config have done on
    this specific day?"

Default target is YESTERDAY (``date.today() - 1d``); edit
``TARGET_DATE`` at the top to pick any past date.

Hard-fails if the target has no actuals in the pool — a backtest
without actuals is meaningless.

Tunable defaults at the top — edit and re-run, no CLI flags. Add your
weight perturbations to ``SCENARIOS``.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.backtest.single_day_backtest
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/backtest/single_day_backtest.py
"""
from __future__ import annotations

import sys
import time
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from colorama import Back, Fore, Style, init as colorama_init  # noqa: E402

colorama_init()

from da_models.like_day_model_knn import _shared, configs  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.backtest.scenarios import (  # noqa: E402
    SCENARIOS,
)
from da_models.like_day_model_knn.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool, build_query_row,
)
from da_models.common.forecast.output import actuals_from_pool  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.metrics import (  # noqa: E402
    evaluate_shape,
)
from da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as forecast_run,
)


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = date(2026, 5, 6)   # None -> yesterday (date.today() - 1d)
MODEL_NAME: str = configs.PJM_RTO_HOURLY_SPEC.name
# Scenarios live in scenarios.py (shared with param_sweep). Edit there
# to add/remove perturbations; both backtest scripts pick up the change.

_RUN_KWARGS_PASSTHROUGH: tuple[str, ...] = (
    "flt_radius", "n_analogs", "season_window_days", "min_pool_size",
)


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() - timedelta(days=1)


def _scenario_overrides_for_run(scenario: dict) -> dict:
    overrides = scenario.get("overrides") or {}
    return {k: v for k, v in overrides.items() if k in _RUN_KWARGS_PASSTHROUGH}


def _execute_scenario(
    *,
    scenario_name: str,
    scenario: dict,
    target_date: date,
    pool: pd.DataFrame,
    query: pd.Series,
    dates_meta: pd.DataFrame,
    model_name: str,
) -> dict:
    started = time.perf_counter()
    base: dict = {
        "scenario_name": scenario_name,
        "status": "ok",
        "error_message": None,
    }
    try:
        result = forecast_run(
            target_date=target_date,
            model_name=model_name,
            pool=pool, query=query, dates_meta=dates_meta,
            feature_group_weights_override=scenario.get("weights"),
            quiet=True,
            write_analog_store=False,
            **_scenario_overrides_for_run(scenario),
        )
    except Exception as exc:
        base.update({
            "status": "failed",
            "error_message": f"{type(exc).__name__}: {exc}",
            "duration_s": round(time.perf_counter() - started, 3),
        })
        return base

    metrics = result.get("metrics") or {}
    output_table = result.get("output_table")
    quantiles_table = result.get("quantiles_table")
    forecast_row = (
        output_table[output_table["Type"] == "Forecast"].iloc[0]
        if output_table is not None and len(output_table) else None
    )
    actual_row = (
        output_table[output_table["Type"] == "Actual"].iloc[0]
        if output_table is not None and (output_table["Type"] == "Actual").any() else None
    )
    error_row = (
        output_table[output_table["Type"] == "Error"].iloc[0]
        if output_table is not None and (output_table["Type"] == "Error").any() else None
    )

    # Per-HE arrays for the hourly metrics table
    hourly_forecast: list[float | None] = (
        [float(forecast_row[f"HE{h}"]) if pd.notna(forecast_row[f"HE{h}"]) else None
         for h in range(1, 25)]
        if forecast_row is not None else [None] * 24
    )
    hourly_actual: list[float | None] = (
        [float(actual_row[f"HE{h}"]) if pd.notna(actual_row[f"HE{h}"]) else None
         for h in range(1, 25)]
        if actual_row is not None else [None] * 24
    )
    hourly_abs_error: list[float | None] = (
        [abs(float(error_row[f"HE{h}"])) if pd.notna(error_row[f"HE{h}"]) else None
         for h in range(1, 25)]
        if error_row is not None else [None] * 24
    )

    # Per-HE 90% PI coverage (from P05/P95 rows in the quantiles table)
    hourly_in_90pi: list[bool | None] = [None] * 24
    if quantiles_table is not None and actual_row is not None:
        p05_rows = quantiles_table[quantiles_table["Type"] == "P05"]
        p95_rows = quantiles_table[quantiles_table["Type"] == "P95"]
        if len(p05_rows) and len(p95_rows):
            p05 = p05_rows.iloc[0]
            p95 = p95_rows.iloc[0]
            for i, h in enumerate(range(1, 25)):
                a = hourly_actual[i]
                lo = float(p05[f"HE{h}"]) if pd.notna(p05[f"HE{h}"]) else None
                hi = float(p95[f"HE{h}"]) if pd.notna(p95[f"HE{h}"]) else None
                if a is None or lo is None or hi is None:
                    continue
                hourly_in_90pi[i] = lo <= a <= hi

    base.update({
        "n_analogs_used": result.get("n_analogs_used"),
        "mae": metrics.get("mae"),
        "rmse": metrics.get("rmse"),
        "mape": metrics.get("mape"),
        "rmae": metrics.get("rmae"),
        "crps": metrics.get("crps"),
        "mean_pinball": metrics.get("mean_pinball"),
        "coverage_80pct": metrics.get("coverage_80pct"),
        "coverage_90pct": metrics.get("coverage_90pct"),
        "coverage_98pct": metrics.get("coverage_98pct"),
        "sharpness_90pct": metrics.get("sharpness_90pct"),
        "forecast_onpeak": float(forecast_row["OnPeak"]) if forecast_row is not None else None,
        "forecast_offpeak": float(forecast_row["OffPeak"]) if forecast_row is not None else None,
        "forecast_flat": float(forecast_row["Flat"]) if forecast_row is not None else None,
        "hourly_forecast": hourly_forecast,
        "hourly_actual": hourly_actual,
        "hourly_abs_error": hourly_abs_error,
        "hourly_in_90pi": hourly_in_90pi,
        "output_table": output_table,
        "duration_s": round(time.perf_counter() - started, 3),
    })
    return base


def _print_comparison(
    rows: list[dict], target_date: date, actuals_summary: dict[str, float],
) -> None:
    df = pd.DataFrame(rows)
    ok = df[df["status"] == "ok"].copy()
    failed = df[df["status"] == "failed"]

    print("\n" + "=" * 110)
    print(f"  SINGLE-DAY BACKTEST — {target_date} ({target_date.strftime('%a')})  |  Hub: WESTERN HUB")
    print(f"  Actuals  OnPeak={actuals_summary['onpeak']:>7.2f}  "
          f"OffPeak={actuals_summary['offpeak']:>7.2f}  "
          f"Flat={actuals_summary['flat']:>7.2f}  ($/MWh)")
    print(f"  Scenarios: {len(df)} ({len(ok)} ok, {len(failed)} failed)")
    print("=" * 110)

    if len(ok) == 0:
        print("\n  No successful scenarios; nothing to summarize.\n")
    else:
        if "default" in ok["scenario_name"].values:
            default_mae = float(ok.loc[ok["scenario_name"] == "default", "mae"].iloc[0])
            ok["delta_mae_vs_default"] = ok["mae"] - default_mae
        else:
            ok["delta_mae_vs_default"] = None

        ok = ok.sort_values("mae", ascending=True, na_position="last").reset_index(drop=True)

        cols = [
            "scenario_name", "mae", "rmse", "rmae", "crps",
            "coverage_90pct", "sharpness_90pct",
            "forecast_onpeak", "forecast_offpeak", "forecast_flat",
            "delta_mae_vs_default",
        ]
        formatters = {
            "mae": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "rmse": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "rmae": lambda v: f"{v:>6.3f}" if pd.notna(v) else "   n/a",
            "crps": lambda v: f"{v:>7.4f}" if pd.notna(v) else "    n/a",
            "coverage_90pct": lambda v: f"{v:>6.1%}" if pd.notna(v) else "   n/a",
            "sharpness_90pct": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "forecast_onpeak": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "forecast_offpeak": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "forecast_flat": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "delta_mae_vs_default": lambda v: f"{v:+7.2f}" if pd.notna(v) else "       -",
        }

        with pd.option_context("display.max_rows", None, "display.width", None):
            print()
            print(ok[cols].to_string(index=False, formatters=formatters))

        if len(ok) >= 2:
            best = ok.iloc[0]
            print(f"\n  Best by MAE: {best['scenario_name']} "
                  f"(MAE ${best['mae']:.2f}, rMAE {best['rmae']:.3f})")

    if len(failed) > 0:
        print("\n  Failed scenarios:")
        for _, r in failed.iterrows():
            print(f"    {r['scenario_name']}: {r['error_message']}")

    print()


_HL_FORECAST_LOCAL = Style.BRIGHT + Fore.RED
_RS_LOCAL = Style.RESET_ALL
# Cell highlights for per-scenario hourly tables. Background-only so the
# Forecast row's bright-red FG persists through the highlighted cells.
_HL_PEAK = Back.YELLOW
_HL_VALLEY = Back.CYAN
_RESET_BG = Back.RESET
# Error-row sign coloring: forecast under actual = red, over = green.
_FG_NEG = Fore.RED
_FG_POS = Fore.GREEN
_RESET_FG = Fore.RESET

# Adaptive 4-regime classification. Both early-day and late-day flat
# regions share the 'offpeak' label so the regime row reads
# off ... mramp ... valley ... eramp ... off, matching the typical
# PJM daily price-shape vocabulary.
_REGIME_LABEL: dict[str, str] = {
    "offpeak":      "off",
    "morning_ramp": "m.ramp",
    "valley":       "valley",
    "evening_ramp": "e.ramp",
}


def _classify_hours(actual: np.ndarray) -> list[str]:
    """Label each of 24 hours with one of {'offpeak', 'morning_ramp',
    'valley', 'evening_ramp'} based on the actual price profile shape.

    Anchors are derived from the data, not the clock:
      1. ``argmax(actual)`` is the evening peak — last hour of
         evening_ramp.
      2. Walk back from the peak: hour ``h`` is in the ramp iff the
         rise *into* hour h (``diffs[h - 1]``) >= an adaptive threshold
         (max of $2/MWh and 30% of the 75-percentile abs diff). The
         hour where the rise breaks is **not** in the ramp — that hour
         is the flat *base* before the climb, semantically still
         offpeak. So ``ramp_start = h`` (the first rising hour), not
         ``h - 1`` (its flat predecessor).
      3. Midday valley = ``argmin(actual)`` restricted to
         ``[HE6, evening_ramp_start)`` so the overnight low isn't
         picked.
      4. Morning peak = ``argmax(actual[HE3:valley_h])``, kept only if
         at least $3/MWh above the valley. Winter-style flat days with
         no distinct morning peak skip the morning_ramp regime entirely.
      5. Same backward-walk from morning peak → morning_ramp_start.

    Robust to flat days (no morning peak → daytime hours before the
    evening ramp are 'valley') and spiky days (threshold scales).
    """
    n = 24
    diffs = np.diff(actual)
    abs_nonzero = np.abs(diffs)[np.abs(diffs) > 0.01]
    ramp_thr = max(
        2.0,
        0.30 * (float(np.percentile(abs_nonzero, 75)) if len(abs_nonzero) else 5.0),
    )

    peak_h = int(np.argmax(actual))

    evening_ramp_start = peak_h
    for h in range(peak_h, 0, -1):
        if diffs[h - 1] >= ramp_thr:
            evening_ramp_start = h
        else:
            break

    lo, hi = 5, max(6, evening_ramp_start)  # HE6..evening_ramp_start
    valley_h = lo + int(np.argmin(actual[lo:hi]))

    morning_peak_h: int | None = None
    if valley_h > 4:
        cand = 2 + int(np.argmax(actual[2:valley_h]))  # HE3..valley
        if actual[cand] - actual[valley_h] >= 3.0:
            morning_peak_h = cand

    morning_ramp_start: int | None = None
    if morning_peak_h is not None:
        morning_ramp_start = morning_peak_h
        for h in range(morning_peak_h, 0, -1):
            if diffs[h - 1] >= ramp_thr:
                morning_ramp_start = h
            else:
                break

    labels: list[str] = ["offpeak"] * n
    for h in range(n):
        if h > peak_h:
            labels[h] = "offpeak"
        elif h >= evening_ramp_start:
            labels[h] = "evening_ramp"
        elif morning_peak_h is not None and h > morning_peak_h:
            labels[h] = "valley"
        elif morning_ramp_start is not None and h >= morning_ramp_start:
            labels[h] = "morning_ramp"
        elif morning_peak_h is None and h >= 5:
            labels[h] = "valley"
    return labels


def _summarize_regime_windows(labels: list[str]) -> str:
    """Compact summary like
    'off=HE1-5,HE22-24  m.ramp=HE6-7  valley=HE8-17  e.ramp=HE18-21'.
    """
    runs: dict[str, list[tuple[int, int]]] = {r: [] for r in _REGIME_LABEL}
    i = 0
    while i < 24:
        regime = labels[i]
        j = i
        while j < 24 and labels[j] == regime:
            j += 1
        runs[regime].append((i + 1, j))  # 1-indexed, inclusive end
        i = j
    parts = []
    for regime in ("offpeak", "morning_ramp", "valley", "evening_ramp"):
        if not runs[regime]:
            continue
        ranges = ",".join(
            f"HE{s}-{e}" if s != e else f"HE{s}" for s, e in runs[regime]
        )
        parts.append(f"{_REGIME_LABEL[regime]}={ranges}")
    return "  ".join(parts)


def _regime_mae(
    actual: np.ndarray, forecast: np.ndarray, labels: list[str],
) -> dict[str, float]:
    """Mean absolute error within each regime. NaN for empty regimes."""
    out: dict[str, float] = {}
    for regime in ("offpeak", "morning_ramp", "valley", "evening_ramp"):
        idxs = [i for i, label in enumerate(labels) if label == regime]
        if not idxs:
            out[regime] = float("nan")
        else:
            out[regime] = float(np.mean(np.abs(forecast[idxs] - actual[idxs])))
    return out


def _print_metric_breakdown(rows: list[dict], target_date: date) -> None:
    """Per-scenario shape + regime-MAE breakdown — one row per scenario.

    Combines the hour-agnostic shape metrics from
    ``evaluate_shape`` with MAE within each adaptively-detected regime
    (off / m.ramp / valley / e.ramp). Regime windows come from
    ``_classify_hours(actual)`` so they shift with the season instead
    of being pinned to fixed clock hours.

    Sorted by ``variogm`` ascending. Pair with the SCENARIOS COMPARISON
    table for level metrics — variogram is translation-invariant and
    must never be the standalone ranking objective.
    """
    ok = [r for r in rows if r["status"] == "ok"]
    if not ok:
        return
    actual_list = ok[0].get("hourly_actual")
    if actual_list is None or any(v is None for v in actual_list):
        return
    actual = np.asarray(actual_list, dtype=float)
    regime_labels = _classify_hours(actual)

    metric_rows: list[dict] = []
    for r in ok:
        forecast_list = r.get("hourly_forecast") or [None] * 24
        if any(v is None for v in forecast_list):
            metric_rows.append({"scenario_name": r["scenario_name"]})
            continue
        forecast = np.asarray(forecast_list, dtype=float)
        shape = evaluate_shape(actual, forecast)
        regime = _regime_mae(actual, forecast, regime_labels)
        metric_rows.append({
            "scenario_name": r["scenario_name"],
            "variogm":  shape["variogram_score_p05"],
            "pkErr":    shape["peak_height_err"],
            "vlErr":    shape["valley_height_err"],
            "tPk":      shape["time_of_peak_err"],
            "tVl":      shape["time_of_valley_err"],
            "pkWinMAE": shape["peak_window_mae"],
            "fdMAE":    shape["first_diff_mae"],
            "off":      regime["offpeak"],
            "mramp":    regime["morning_ramp"],
            "valley":   regime["valley"],
            "eramp":    regime["evening_ramp"],
        })

    df = pd.DataFrame(metric_rows).sort_values(
        "variogm", ascending=True, na_position="last",
    ).reset_index(drop=True)

    cols = [
        "scenario_name",
        "variogm", "pkErr", "vlErr", "tPk", "tVl", "pkWinMAE", "fdMAE",
        "off", "mramp", "valley", "eramp",
    ]
    formatters = {
        "variogm":  lambda v: f"{v:>7.4f}" if pd.notna(v) else "    n/a",
        "pkErr":    lambda v: f"{v:+7.2f}" if pd.notna(v) else "    n/a",
        "vlErr":    lambda v: f"{v:+7.2f}" if pd.notna(v) else "    n/a",
        "tPk":      lambda v: f"{int(v):+3d}h" if pd.notna(v) else "  n/a",
        "tVl":      lambda v: f"{int(v):+3d}h" if pd.notna(v) else "  n/a",
        "pkWinMAE": lambda v: f"{v:>8.2f}" if pd.notna(v) else "     n/a",
        "fdMAE":    lambda v: f"{v:>6.2f}" if pd.notna(v) else "   n/a",
        "off":      lambda v: f"{v:>6.2f}" if pd.notna(v) else "   n/a",
        "mramp":    lambda v: f"{v:>6.2f}" if pd.notna(v) else "   n/a",
        "valley":   lambda v: f"{v:>6.2f}" if pd.notna(v) else "   n/a",
        "eramp":    lambda v: f"{v:>6.2f}" if pd.notna(v) else "   n/a",
    }

    a_max = float(np.max(actual))
    a_min = float(np.min(actual))
    a_argmax = int(np.argmax(actual))
    a_argmin = int(np.argmin(actual))

    print("=" * 140)
    print(f"  METRIC BREAKDOWN — {target_date}  (signed err = forecast - actual; sorted by variogram ascending)")
    print(f"  Actuals: peak ${a_max:.2f} at HE{a_argmax + 1}   valley ${a_min:.2f} at HE{a_argmin + 1}")
    print(f"  Regime windows (auto-detected from actual): {_summarize_regime_windows(regime_labels)}")
    print(f"  variogm = shape KPI; *MAE = $/MWh within window; t* = signed timing err in hours")
    print("=" * 140)
    print()
    with pd.option_context("display.max_rows", None, "display.width", None):
        print(df[cols].to_string(index=False, formatters=formatters))
    print()


def _print_per_scenario_detail(rows: list[dict], target_date: date) -> None:
    """One Actual / Forecast / Error block per scenario.

    Mirrors the per-run table that ``forecast_single_day.print_forecast``
    prints (Date | Type | HE1..HE24 | OnPk | OffPk | Flat). Forecast row
    is colorized bright red — same visual cue as the live forecast.

    Highlights the actual peak/valley cells in the Actual row and the
    forecast peak/valley cells in the Forecast row (yellow bg = peak,
    cyan bg = valley) so the eye can link the footer's shape KPIs back
    to the chart cells they describe.
    """
    ok = [r for r in rows if r["status"] == "ok"]
    if not ok:
        return

    actual_list = ok[0].get("hourly_actual")
    have_actual = (
        actual_list is not None and not any(v is None for v in actual_list)
    )
    actual_arr = np.asarray(actual_list, dtype=float) if have_actual else None
    actual_argmax = int(np.argmax(actual_arr)) if have_actual else None
    actual_argmin = int(np.argmin(actual_arr)) if have_actual else None
    regime_labels = _classify_hours(actual_arr) if have_actual else None

    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    sep_len = len(header)

    regime_row = None
    if regime_labels is not None:
        rline = f"{'':<12} {'Regime':<10}"
        for h in range(1, 25):
            rline += f" {_REGIME_LABEL[regime_labels[h - 1]]:>6}"
        rline += f" {'':>7} {'':>7} {'':>7}"
        regime_row = rline

    for r in ok:
        table = r.get("output_table")
        if table is None or len(table) == 0:
            continue

        forecast_list = r.get("hourly_forecast")
        have_forecast = (
            forecast_list is not None and not any(v is None for v in forecast_list)
        )
        forecast_arr = (
            np.asarray(forecast_list, dtype=float) if have_forecast else None
        )
        forecast_argmax = int(np.argmax(forecast_arr)) if have_forecast else None
        forecast_argmin = int(np.argmin(forecast_arr)) if have_forecast else None
        shape = (
            evaluate_shape(actual_arr, forecast_arr)
            if have_actual and have_forecast else None
        )

        print("=" * 120)
        print(f"  HOURLY DETAIL: {r['scenario_name']}  —  Western Hub ($/MWh) — {target_date}")
        if r.get("mae") is not None:
            print(f"  MAE: ${r['mae']:.2f}/MWh  |  RMSE: ${r['rmse']:.2f}/MWh  |  rMAE: {r['rmae']:.3f}")
        print("=" * 120)
        print(header)
        print("-" * sep_len)
        if regime_row is not None:
            print(regime_row)

        for _, row in table.iterrows():
            row_type = row["Type"]
            line = f"{str(row['Date']):<12} {row_type:<10}"
            for h in range(1, 25):
                val = row.get(f"HE{h}")
                cell = f"{val:>6.1f}" if pd.notna(val) else f"{'':>6}"
                idx = h - 1

                bg = ""
                fg = ""
                if row_type == "Actual":
                    if actual_argmax is not None and idx == actual_argmax:
                        bg = _HL_PEAK
                    elif actual_argmin is not None and idx == actual_argmin:
                        bg = _HL_VALLEY
                elif row_type == "Forecast":
                    if forecast_argmax is not None and idx == forecast_argmax:
                        bg = _HL_PEAK
                    elif forecast_argmin is not None and idx == forecast_argmin:
                        bg = _HL_VALLEY
                elif row_type == "Error" and pd.notna(val):
                    if val < 0:
                        fg = _FG_NEG
                    elif val > 0:
                        fg = _FG_POS

                if bg and fg:
                    line += f" {bg}{fg}{cell}{_RESET_FG}{_RESET_BG}"
                elif bg:
                    line += f" {bg}{cell}{_RESET_BG}"
                elif fg:
                    line += f" {fg}{cell}{_RESET_FG}"
                else:
                    line += f" {cell}"

            for col in ("OnPeak", "OffPeak", "Flat"):
                v = row.get(col)
                if not pd.notna(v):
                    line += f" {'':>7}"
                    continue
                cell = f"{v:>7.2f}"
                if row_type == "Error":
                    if v < 0:
                        line += f" {_FG_NEG}{cell}{_RESET_FG}"
                    elif v > 0:
                        line += f" {_FG_POS}{cell}{_RESET_FG}"
                    else:
                        line += f" {cell}"
                else:
                    line += f" {cell}"
            if row_type == "Forecast":
                line = f"{_HL_FORECAST_LOCAL}{line}{_RS_LOCAL}"
            print(line)

        print("-" * sep_len)

        if shape is not None:
            a_pk_he, a_vl_he = actual_argmax + 1, actual_argmin + 1
            f_pk_he, f_vl_he = forecast_argmax + 1, forecast_argmin + 1
            a_pk, a_vl = float(actual_arr.max()), float(actual_arr.min())
            f_pk, f_vl = float(forecast_arr.max()), float(forecast_arr.min())
            print(
                f"  Peak    actual {_HL_PEAK}HE{a_pk_he:<2} ${a_pk:>6.2f}{_RESET_BG}  "
                f"forecast {_HL_PEAK}HE{f_pk_he:<2} ${f_pk:>6.2f}{_RESET_BG}  "
                f"time {int(shape['time_of_peak_err']):+3d}h  "
                f"height ${shape['peak_height_err']:+6.2f}  "
                f"err@actual ${shape['peak_at_actual_hour_err']:+6.2f}"
            )
            print(
                f"  Valley  actual {_HL_VALLEY}HE{a_vl_he:<2} ${a_vl:>6.2f}{_RESET_BG}  "
                f"forecast {_HL_VALLEY}HE{f_vl_he:<2} ${f_vl:>6.2f}{_RESET_BG}  "
                f"time {int(shape['time_of_valley_err']):+3d}h  "
                f"height ${shape['valley_height_err']:+6.2f}  "
                f"err@actual ${shape['valley_at_actual_hour_err']:+6.2f}"
            )
            print(
                f"  Shape   variogram(p=0.5) {shape['variogram_score_p05']:>7.4f}  "
                f"first-diff MAE {shape['first_diff_mae']:>5.2f}  "
                f"peak-window MAE {shape['peak_window_mae']:>5.2f}"
            )

        print()


def run(
    target_date: date | None = TARGET_DATE,
    scenarios: dict | None = None,
    model_name: str = MODEL_NAME,
) -> list[dict]:
    """Run every scenario against a single target date, then print a sorted
    comparison table. Returns the list of result dicts (one per scenario).
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if scenarios is None:
        scenarios = SCENARIOS
    if not scenarios:
        raise ValueError("SCENARIOS is empty; add at least one entry.")

    resolved_date = _resolve_target_date(target_date)
    if resolved_date >= date.today() + timedelta(days=1):
        raise ValueError(
            f"Backtest needs a past or current date with actuals; got {resolved_date}. "
            f"Use forecast_single_day for live/future forecasts."
        )

    base_spec = configs.MODEL_REGISTRY[model_name]
    spec_for_build = replace(base_spec, flt_radius=int(base_spec.flt_radius))

    print(f"\n[backtest {resolved_date}] building pool + dates_meta...")
    t0 = time.perf_counter()
    pool = build_pool(spec=spec_for_build, cache_dir=configs.CACHE_DIR)
    dates_meta = _shared.load_dates_daily(configs.CACHE_DIR)
    print(f"[backtest {resolved_date}] pool built in {time.perf_counter() - t0:.1f}s "
          f"({len(pool)} rows)")

    actuals = actuals_from_pool(pool, resolved_date)
    if actuals is None:
        raise RuntimeError(
            f"No actuals in the pool for target_date={resolved_date}. "
            "The DA LMP cache may not yet cover this date — try an earlier "
            "target_date, or refresh the pjm_lmps_hourly parquet."
        )
    actuals_summary = {
        "onpeak": float(sum(actuals[h] for h in range(8, 24)) / 16),
        "offpeak": float(sum(actuals[h] for h in list(range(1, 8)) + [24]) / 8),
        "flat": float(sum(actuals.values()) / 24),
    }

    print(f"[backtest {resolved_date}] building query for target...")
    query = build_query_row(
        target_date=resolved_date, cache_dir=configs.CACHE_DIR, spec=spec_for_build,
    )

    print(f"[backtest {resolved_date}] running {len(scenarios)} scenario(s)...")
    rows: list[dict] = []
    for scenario_name, scenario in scenarios.items():
        row = _execute_scenario(
            scenario_name=scenario_name,
            scenario=scenario,
            target_date=resolved_date,
            pool=pool, query=query, dates_meta=dates_meta,
            model_name=model_name,
        )
        tag = "OK" if row["status"] == "ok" else "FAIL"
        mae = row.get("mae")
        mae_str = f"MAE={mae:.2f}" if isinstance(mae, (int, float)) and pd.notna(mae) else "MAE=n/a"
        print(f"[backtest {resolved_date}]   {scenario_name:<24} {tag:<4} "
              f"{mae_str}  ({row['duration_s']:.2f}s)")
        rows.append(row)

    _print_comparison(rows, resolved_date, actuals_summary)
    _print_metric_breakdown(rows, resolved_date)
    _print_per_scenario_detail(rows, resolved_date)
    return rows


if __name__ == "__main__":
    run()
