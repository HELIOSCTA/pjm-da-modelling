"""Parameter sweep over a recent-weekday backtest window for pjm_rto_hourly.

Loops over (target_dates x scenarios), aggregates per-scenario metrics
across the window, prints a comparison table sorted by mean rMAE, and
writes a parquet + JSON sidecar to ``backtest/output/``.

Tunable defaults at the top — edit and re-run, no CLI flags.

The sweep amortizes the heavy ``build_pool`` call across all
(date, scenario) pairs by building it once and reusing it via the new
reusable-artefact kwargs on ``forecast_single_day.run()``.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.backtest.param_sweep
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/backtest/param_sweep.py
"""
from __future__ import annotations

import json
import sys
import time
import uuid
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.like_day_model_knn import _shared, configs  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.backtest.scenarios import (  # noqa: E402
    SCENARIOS,
)
from da_models.like_day_model_knn.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool, build_query_row,
)
from da_models.common.forecast.output import actuals_from_pool  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as forecast_run,
)
from da_models.naive_baselines.pjm_rto_hourly.forecast import (  # noqa: E402
    forecast_d7,
    forecast_epf_naive,
)


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = None              # None -> tomorrow (date.today() + 1d)
BACKTEST_WINDOW_DAYS: int = 14               # walk back N calendar days from
                                             # TARGET_DATE - 1; filter to weekday
                                             # targets that have full actuals
SWEEP_OUTPUT_DIR: Path = Path(__file__).resolve().parent / "output"
MODEL_NAME: str = configs.PJM_RTO_HOURLY_SPEC.name
# rMAE denominator. "d7" = same-hour-last-week (legacy default; preserves
# historical leaderboard numbers). "epf" = Lago/Nogales/Conejo
# DOW-conditional persistence (d-1 Tue-Fri, d-7 otherwise). Switching only
# changes the rMAE column in the leaderboard, never the model's MAE/RMSE.
NAIVE_BASELINE: str = "d7"
# Scenarios live in scenarios.py (shared with single_day_backtest). Edit
# there to add/remove perturbations; both backtest scripts pick up the change.

_NAIVE_FORECASTERS = {
    "d7": forecast_d7,
    "epf": forecast_epf_naive,
}

_RUN_KWARGS_PASSTHROUGH: tuple[str, ...] = (
    "flt_radius", "n_analogs", "season_window_days", "min_pool_size",
)


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _backtest_dates(
    pool: pd.DataFrame, anchor_date: date, lookback_days: int,
) -> list[date]:
    """Last N weekday targets ending at anchor_date, filtered to dates with
    full actuals in ``pool`` (so MAE/RMSE/CRPS can be computed)."""
    out: list[date] = []
    for k in range(1, lookback_days + 1):
        d = anchor_date - timedelta(days=k)
        if d.weekday() >= 5:                 # skip Sat/Sun
            continue
        if actuals_from_pool(pool, d) is None:
            continue
        out.append(d)
    return out


def _sweep_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    return f"{stamp}_{uuid.uuid4().hex[:6]}"


def _scenario_overrides_for_run(scenario: dict) -> dict:
    """Subset of scenario['overrides'] that's safe to pass to forecast_run()."""
    overrides = scenario.get("overrides") or {}
    return {k: v for k, v in overrides.items() if k in _RUN_KWARGS_PASSTHROUGH}


def _load_naive_table(
    target_dates: list[date],
    baseline: str,
    pool: pd.DataFrame,
) -> dict[date, np.ndarray | None]:
    """Pre-compute the naive rMAE denominator profile for every target.

    Returns a date -> length-24 ``np.ndarray`` mapping (or ``None`` when
    the lag source row is missing). Reuses the pool already built by
    the sweep; no extra IO.
    """
    if baseline not in _NAIVE_FORECASTERS:
        raise ValueError(
            f"NAIVE_BASELINE={baseline!r} not in {tuple(_NAIVE_FORECASTERS.keys())}"
        )
    forecaster = _NAIVE_FORECASTERS[baseline]
    table: dict[date, np.ndarray | None] = {}
    for td in target_dates:
        df = forecaster(pool, td)
        if len(df) == 0:
            table[td] = None
            continue
        arr = df["point_forecast"].to_numpy(dtype=float)
        # Match the legacy _naive_last_week contract: return None if any
        # hour is NaN so rMAE stays null rather than poisoned.
        if np.isnan(arr).any():
            table[td] = None
            continue
        table[td] = arr
    return table


def _execute_scenario(
    *,
    scenario_name: str,
    scenario: dict,
    target_date: date,
    pool: pd.DataFrame,
    query: pd.Series,
    dates_meta: pd.DataFrame,
    y_naive_override: np.ndarray | None = None,
) -> dict:
    """Run one (target_date, scenario) cell. Captures metrics + status."""
    started = time.perf_counter()
    base: dict = {
        "target_date": target_date,
        "scenario_name": scenario_name,
        "weights_json": json.dumps(scenario.get("weights")),
        "status": "ok",
        "error_message": None,
    }
    try:
        result = forecast_run(
            target_date=target_date,
            model_name=MODEL_NAME,
            pool=pool, query=query, dates_meta=dates_meta,
            feature_group_weights_override=scenario.get("weights"),
            quiet=True,
            write_analog_store=False,
            y_naive_override=y_naive_override,
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
    forecast_row = output_table[output_table["Type"] == "Forecast"].iloc[0] if output_table is not None and len(output_table) else None
    actual_row = (
        output_table[output_table["Type"] == "Actual"].iloc[0]
        if output_table is not None and (output_table["Type"] == "Actual").any() else None
    )

    base.update({
        "n_pool": result.get("n_pool"),
        "n_analogs_used": result.get("n_analogs_used"),
        "has_actuals": result.get("has_actuals"),
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
        "actual_onpeak": float(actual_row["OnPeak"]) if actual_row is not None else None,
        "actual_offpeak": float(actual_row["OffPeak"]) if actual_row is not None else None,
        "actual_flat": float(actual_row["Flat"]) if actual_row is not None else None,
        "duration_s": round(time.perf_counter() - started, 3),
    })
    overrides = scenario.get("overrides") or {}
    for k in _RUN_KWARGS_PASSTHROUGH:
        base[f"override_{k}"] = overrides.get(k)
    base["naive_baseline"] = NAIVE_BASELINE
    return base


def _print_summary(rows: list[dict], sweep_id: str, target_dates: list[date]) -> None:
    """Cross-scenario summary: mean metrics across dates, sorted by rMAE."""
    df = pd.DataFrame(rows)
    ok = df[df["status"] == "ok"]
    failed = df[df["status"] == "failed"]

    print("\n" + "=" * 100)
    print(f"  PARAM SWEEP — {sweep_id}")
    print(f"  Backtest window: {len(target_dates)} weekday target(s), "
          f"{target_dates[-1] if target_dates else '?'} -> {target_dates[0] if target_dates else '?'}")
    print(f"  Scenarios: {df['scenario_name'].nunique()} | "
          f"Cells: {len(df)} ({len(ok)} ok, {len(failed)} failed)")
    print("=" * 100)

    if len(ok) == 0:
        print("\n  No successful cells; nothing to summarize.\n")
    else:
        agg = (
            ok.groupby("scenario_name")
            .agg(
                n_dates=("target_date", "count"),
                mean_mae=("mae", "mean"),
                mean_rmse=("rmse", "mean"),
                mean_rmae=("rmae", "mean"),
                mean_crps=("crps", "mean"),
                mean_coverage_90pct=("coverage_90pct", "mean"),
                mean_sharpness_90pct=("sharpness_90pct", "mean"),
            )
            .reset_index()
        )

        if "default" in agg["scenario_name"].values:
            default_mae = float(agg.loc[agg["scenario_name"] == "default", "mean_mae"].iloc[0])
            agg["delta_mae_vs_default"] = agg["mean_mae"] - default_mae
        else:
            agg["delta_mae_vs_default"] = None

        agg = agg.sort_values("mean_rmae", ascending=True, na_position="last").reset_index(drop=True)

        cols = [
            "scenario_name", "n_dates", "mean_mae", "mean_rmse", "mean_rmae",
            "mean_crps", "mean_coverage_90pct", "mean_sharpness_90pct",
            "delta_mae_vs_default",
        ]
        formatters = {
            "mean_mae": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "mean_rmse": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "mean_rmae": lambda v: f"{v:>6.3f}" if pd.notna(v) else "   n/a",
            "mean_crps": lambda v: f"{v:>7.4f}" if pd.notna(v) else "    n/a",
            "mean_coverage_90pct": lambda v: f"{v:>6.1%}" if pd.notna(v) else "   n/a",
            "mean_sharpness_90pct": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "delta_mae_vs_default": lambda v: f"{v:+7.2f}" if pd.notna(v) else "       -",
        }

        with pd.option_context("display.max_rows", None, "display.width", None):
            print()
            print(agg[cols].to_string(index=False, formatters=formatters))

        if len(agg) >= 2:
            print(f"\n  Best by mean_rmae: {agg.iloc[0]['scenario_name']} "
                  f"(rMAE {agg.iloc[0]['mean_rmae']:.3f})")

    if len(failed) > 0:
        print("\n  Failed cells:")
        for _, r in failed.iterrows():
            print(f"    {r['scenario_name']} @ {r['target_date']}: {r['error_message']}")

    print("\n" + "!" * 100)
    print("  WARNING: weights tuned on a backtest window — re-validate on a longer holdout")
    print("  before changing domains.py defaults. Single-window optimization can overfit.")
    print("!" * 100 + "\n")


def _persist(rows: list[dict], scenarios: dict, sweep_id: str) -> Path:
    """Write parquet + JSON sidecar; return parquet path."""
    SWEEP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = SWEEP_OUTPUT_DIR / f"{sweep_id}.parquet"
    json_path = SWEEP_OUTPUT_DIR / f"{sweep_id}_scenarios.json"

    df = pd.DataFrame(rows)
    df["sweep_id"] = sweep_id
    df["sweep_started_at_utc"] = datetime.now(timezone.utc).isoformat()
    df.to_parquet(parquet_path, index=False)

    with json_path.open("w", encoding="utf-8") as f:
        json.dump({"sweep_id": sweep_id, "scenarios": scenarios}, f, indent=2, default=str)

    return parquet_path


def run(
    target_date: date | None = TARGET_DATE,
    backtest_window_days: int = BACKTEST_WINDOW_DAYS,
    scenarios: dict | None = None,
    output_dir: Path | None = None,
    model_name: str = MODEL_NAME,
) -> Path:
    """Execute the parameter sweep across (backtest dates x scenarios).

    Returns the parquet path. Prints a cross-scenario summary to stdout.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if scenarios is None:
        scenarios = SCENARIOS
    if not scenarios:
        raise ValueError("SCENARIOS is empty; add at least one entry to sweep.")

    global SWEEP_OUTPUT_DIR
    if output_dir is not None:
        SWEEP_OUTPUT_DIR = Path(output_dir)

    sweep_id = _sweep_id()
    anchor = _resolve_target_date(target_date)

    print("\n" + "*" * 100)
    print(f"  rMAE denominator: NAIVE_BASELINE = {NAIVE_BASELINE!r}  "
          f"({'same-hour-last-week' if NAIVE_BASELINE == 'd7' else 'EPF DOW-conditional persistence'})")
    print("*" * 100)

    # Build pool / dates_meta ONCE — both are target-date-independent. Spec
    # carries the domain set, so build_pool needs the spec to know which
    # feature columns to include.
    base_spec = configs.MODEL_REGISTRY[model_name]
    spec_for_build = replace(base_spec, flt_radius=int(base_spec.flt_radius))

    print(f"\n[sweep {sweep_id}] building pool + dates_meta...")
    t0 = time.perf_counter()
    pool = build_pool(spec=spec_for_build, cache_dir=configs.CACHE_DIR)
    dates_meta = _shared.load_dates_daily(configs.CACHE_DIR)
    print(f"[sweep {sweep_id}] pool built in {time.perf_counter() - t0:.1f}s "
          f"({len(pool)} rows)")

    target_dates = _backtest_dates(pool, anchor, backtest_window_days)
    if not target_dates:
        raise RuntimeError(
            f"No weekday target dates with actuals found in the {backtest_window_days}d "
            f"window ending {anchor}. Try a longer window or check pool LMP coverage."
        )
    print(f"[sweep {sweep_id}] {len(target_dates)} weekday targets: "
          f"{target_dates[-1]} -> {target_dates[0]}")
    print(f"[sweep {sweep_id}] {len(scenarios)} scenarios x {len(target_dates)} dates "
          f"= {len(scenarios) * len(target_dates)} cells")

    naive_table = _load_naive_table(target_dates, NAIVE_BASELINE, pool)
    n_naive_missing = sum(1 for v in naive_table.values() if v is None)
    if n_naive_missing:
        print(f"[sweep {sweep_id}] naive '{NAIVE_BASELINE}' lag missing for "
              f"{n_naive_missing}/{len(target_dates)} target(s) — those cells "
              f"will report rMAE=null")

    rows: list[dict] = []
    for td in target_dates:
        try:
            query = build_query_row(
                target_date=td, cache_dir=configs.CACHE_DIR, spec=spec_for_build,
            )
        except Exception as exc:
            print(f"[sweep {sweep_id}]   skip {td}: build_query_row failed "
                  f"({type(exc).__name__}: {exc})")
            continue
        for scenario_name, scenario in scenarios.items():
            row = _execute_scenario(
                scenario_name=scenario_name,
                scenario=scenario,
                target_date=td,
                pool=pool,
                query=query,
                dates_meta=dates_meta,
                y_naive_override=naive_table.get(td),
            )
            tag = "OK" if row["status"] == "ok" else "FAIL"
            mae = row.get("mae")
            mae_str = f"MAE={mae:.2f}" if isinstance(mae, (int, float)) and pd.notna(mae) else "MAE=n/a"
            print(f"[sweep {sweep_id}]   {td} {scenario_name:<24} {tag:<4} "
                  f"{mae_str}  ({row['duration_s']:.2f}s)")
            rows.append(row)

    parquet_path = _persist(rows, scenarios, sweep_id)
    _print_summary(rows, sweep_id, target_dates)
    print(f"  Wrote: {parquet_path}")
    print(f"  Wrote: {parquet_path.with_name(parquet_path.stem + '_scenarios.json')}\n")

    return parquet_path


if __name__ == "__main__":
    run()
