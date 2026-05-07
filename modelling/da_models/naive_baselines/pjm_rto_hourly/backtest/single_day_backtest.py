"""Single-day display harness for the two naive baselines.

Pins ONE target date and prints the EPF and d-7 forecasts side-by-side
with metrics. Mirrors the section layout of the like-day model's
``backtest/single_day_backtest.py`` so readers can stack them visually.

Hard-fails if the target has no actuals in the pool.

Usage::

    python -m da_models.naive_baselines.pjm_rto_hourly.backtest.single_day_backtest
    python modelling/da_models/naive_baselines/pjm_rto_hourly/backtest/single_day_backtest.py
"""

from __future__ import annotations

import sys
import time
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402
from colorama import Fore, Style, init as colorama_init  # noqa: E402

colorama_init()

from da_models.common.forecast.output import actuals_from_pool  # noqa: E402
from da_models.like_day_model_knn import configs as like_day_configs  # noqa: E402
from da_models.naive_baselines._shared import build_lmp_only_pool  # noqa: E402
from da_models.naive_baselines.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as naive_run,
)


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = date(2026, 4, 30)  # None -> yesterday (date.today() - 1d)
HUB: str = like_day_configs.HUB
CACHE_DIR: Path | None = None
BASELINES: tuple[str, ...] = ("epf", "d7")

_HL_FORECAST = Style.BRIGHT + Fore.RED
_RS = Style.RESET_ALL


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() - timedelta(days=1)


def _execute_baseline(
    *,
    baseline: str,
    target_date: date,
    pool: pd.DataFrame,
) -> dict:
    started = time.perf_counter()
    base: dict = {"baseline": baseline, "status": "ok", "error_message": None}
    try:
        result = naive_run(
            target_date=target_date,
            baseline=baseline,
            pool=pool,
            quiet=True,
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
    output_table = result.get("output_table")
    forecast_row = (
        output_table[output_table["Type"] == "Forecast"].iloc[0]
        if output_table is not None and len(output_table)
        else None
    )
    actual_row = (
        output_table[output_table["Type"] == "Actual"].iloc[0]
        if output_table is not None and (output_table["Type"] == "Actual").any()
        else None
    )
    error_row = (
        output_table[output_table["Type"] == "Error"].iloc[0]
        if output_table is not None and (output_table["Type"] == "Error").any()
        else None
    )

    hourly_forecast = (
        [
            float(forecast_row[f"HE{h}"]) if pd.notna(forecast_row[f"HE{h}"]) else None
            for h in range(1, 25)
        ]
        if forecast_row is not None
        else [None] * 24
    )
    hourly_actual = (
        [
            float(actual_row[f"HE{h}"]) if pd.notna(actual_row[f"HE{h}"]) else None
            for h in range(1, 25)
        ]
        if actual_row is not None
        else [None] * 24
    )
    hourly_abs_error = (
        [
            abs(float(error_row[f"HE{h}"])) if pd.notna(error_row[f"HE{h}"]) else None
            for h in range(1, 25)
        ]
        if error_row is not None
        else [None] * 24
    )

    base.update(
        {
            "baseline_name": result.get("baseline_name"),
            "mae": metrics.get("mae"),
            "rmse": metrics.get("rmse"),
            "mape": metrics.get("mape"),
            "forecast_onpeak": float(forecast_row["OnPeak"])
            if forecast_row is not None
            else None,
            "forecast_offpeak": float(forecast_row["OffPeak"])
            if forecast_row is not None
            else None,
            "forecast_flat": float(forecast_row["Flat"])
            if forecast_row is not None
            else None,
            "hourly_forecast": hourly_forecast,
            "hourly_actual": hourly_actual,
            "hourly_abs_error": hourly_abs_error,
            "output_table": output_table,
            "duration_s": round(time.perf_counter() - started, 3),
        }
    )
    return base


def _print_comparison(
    rows: list[dict],
    target_date: date,
    hub: str,
    actuals_summary: dict[str, float],
) -> None:
    print("\n" + "=" * 100)
    print(
        f"  NAIVE BASELINES — {target_date} ({target_date.strftime('%a')})  |  Hub: {hub}"
    )
    print(
        f"  Actuals  OnPeak={actuals_summary['onpeak']:>7.2f}  "
        f"OffPeak={actuals_summary['offpeak']:>7.2f}  "
        f"Flat={actuals_summary['flat']:>7.2f}  ($/MWh)"
    )
    print(f"  Baselines: {len(rows)}")
    print("=" * 100)

    df = pd.DataFrame(rows)
    ok = df[df["status"] == "ok"].copy()
    failed = df[df["status"] == "failed"]

    if len(ok) == 0:
        print("\n  No successful baselines; nothing to summarize.\n")
    else:
        ok = ok.sort_values("mae", ascending=True, na_position="last").reset_index(
            drop=True
        )
        cols = [
            "baseline_name",
            "mae",
            "rmse",
            "mape",
            "forecast_onpeak",
            "forecast_offpeak",
            "forecast_flat",
        ]
        formatters = {
            "mae": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "rmse": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "mape": lambda v: f"{v:>6.2f}" if pd.notna(v) else "   n/a",
            "forecast_onpeak": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "forecast_offpeak": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
            "forecast_flat": lambda v: f"{v:>7.2f}" if pd.notna(v) else "    n/a",
        }
        with pd.option_context("display.max_rows", None, "display.width", None):
            print()
            print(ok[cols].to_string(index=False, formatters=formatters))

    if len(failed) > 0:
        print("\n  Failed baselines:")
        for _, r in failed.iterrows():
            print(f"    {r['baseline']}: {r['error_message']}")
    print()


def _print_per_baseline_detail(rows: list[dict], target_date: date, hub: str) -> None:
    ok = [r for r in rows if r["status"] == "ok"]
    if not ok:
        return

    header = f"{'Date':<12} {'Type':<10}"
    for h in range(1, 25):
        header += f" {h:>6}"
    header += f" {'OnPk':>7} {'OffPk':>7} {'Flat':>7}"
    sep_len = len(header)

    for r in ok:
        table = r.get("output_table")
        if table is None or len(table) == 0:
            continue
        print("=" * 120)
        print(
            f"  HOURLY DETAIL: {r['baseline_name']}  —  {hub} ($/MWh) — {target_date}"
        )
        if r.get("mae") is not None:
            print(f"  MAE: ${r['mae']:.2f}/MWh  |  RMSE: ${r['rmse']:.2f}/MWh")
        print("=" * 120)
        print(header)
        print("-" * sep_len)
        for _, row in table.iterrows():
            line = f"{str(row['Date']):<12} {row['Type']:<10}"
            for h in range(1, 25):
                val = row.get(f"HE{h}")
                line += f" {val:>6.1f}" if pd.notna(val) else f" {'':>6}"
            for col in ("OnPeak", "OffPeak", "Flat"):
                v = row.get(col)
                line += f" {v:>7.2f}" if pd.notna(v) else f" {'':>7}"
            if row["Type"] == "Forecast":
                line = f"{_HL_FORECAST}{line}{_RS}"
            print(line)
        print("-" * sep_len)
        print()


def run(
    target_date: date | None = TARGET_DATE,
    baselines: tuple[str, ...] = BASELINES,
    hub: str = HUB,
    cache_dir: Path | None = CACHE_DIR,
) -> list[dict]:
    """Print EPF and d-7 forecasts side-by-side for a single target date.

    Returns the list of baseline result dicts.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    resolved_date = _resolve_target_date(target_date)
    if resolved_date >= date.today() + timedelta(days=1):
        raise ValueError(
            f"Backtest needs a past or current date with actuals; got {resolved_date}."
        )

    print(f"\n[naive_backtest {resolved_date}] building LMP-only pool...")
    t0 = time.perf_counter()
    pool = build_lmp_only_pool(hub=hub, cache_dir=cache_dir)
    print(
        f"[naive_backtest {resolved_date}] pool built in "
        f"{time.perf_counter() - t0:.1f}s ({len(pool)} rows)"
    )

    actuals = actuals_from_pool(pool, resolved_date)
    if actuals is None:
        raise RuntimeError(
            f"No actuals in the pool for target_date={resolved_date}. "
            "DA LMP cache may not yet cover this date."
        )
    actuals_summary = {
        "onpeak": float(sum(actuals[h] for h in range(8, 24)) / 16),
        "offpeak": float(sum(actuals[h] for h in list(range(1, 8)) + [24]) / 8),
        "flat": float(sum(actuals.values()) / 24),
    }

    print(f"[naive_backtest {resolved_date}] running {len(baselines)} baseline(s)...")
    rows: list[dict] = []
    for baseline in baselines:
        row = _execute_baseline(baseline=baseline, target_date=resolved_date, pool=pool)
        tag = "OK" if row["status"] == "ok" else "FAIL"
        mae = row.get("mae")
        mae_str = (
            f"MAE={mae:.2f}"
            if isinstance(mae, (int, float)) and pd.notna(mae)
            else "MAE=n/a"
        )
        print(
            f"[naive_backtest {resolved_date}]   {baseline:<6} {tag:<4} "
            f"{mae_str}  ({row['duration_s']:.2f}s)"
        )
        rows.append(row)

    _print_comparison(rows, resolved_date, hub, actuals_summary)
    _print_per_baseline_detail(rows, resolved_date, hub)
    return rows


if __name__ == "__main__":
    run()
