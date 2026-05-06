"""Cross-family engine comparison for a single target date.

Runs four engine configurations on ONE target date and stacks their
hourly forecasts side-by-side, with a per-engine metrics block when
actuals are available:

  knn_flt0   -- like_day_model_knn engine, flt_radius=0
                (scalar match at target HE; sunny-like window scope)
  knn_flt1   -- like_day_model_knn engine, flt_radius=1
                (HE-1, HE, HE+1 window — current spec default)
  knn_flt3   -- like_day_model_knn engine, flt_radius=3
                (HE-3 .. HE+3 window — broader local context)
  sunny      -- like_day_model_knn_sunny engine on its native long pool

Both families consume the same parquet sources and use byte-identical
metric definitions in their respective ``metrics.evaluate_forecast``,
so MAE/RMSE/rMAE/CRPS/coverage are directly comparable.

Output structure:

  1. Configuration block (target date, specs, flt_radius variants).
  2. Hourly forecast stack — Actual row at top + one Forecast row per
     engine, in canonical Date | Type | HE1..HE24 | OnPk | OffPk | Flat
     layout. Sorted by MAE asc when actuals are available so the best
     engine sits directly under the Actual row.
  3. Per-engine metrics block (MAE / RMSE / rMAE / CRPS / coverage /
     sharpness / n_analogs / runtime).

Cross-family import is intentional and bounded to this script — the
project's "forward-only cross-family imports" rule is for production
model code; a comparison harness is sideways by nature.

Usage::

    python -m da_models.like_day_model_knn.pjm_rto_hourly.backtest.engine_comparison
    python modelling/da_models/like_day_model_knn/pjm_rto_hourly/backtest/engine_comparison.py
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

import pandas as pd  # noqa: E402

from da_models.common.forecast.output import actuals_from_pool  # noqa: E402
from da_models.like_day_model_knn import _shared as _knn_shared  # noqa: E402
from da_models.like_day_model_knn import configs as knn_configs  # noqa: E402
from da_models.like_day_model_knn.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool as knn_build_pool,
    build_query_row as knn_build_query_row,
)
from da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as knn_run,
)
from da_models.like_day_model_knn_sunny import configs as sunny_configs  # noqa: E402
from da_models.like_day_model_knn_sunny.pjm_rto_hourly.builder import (  # noqa: E402
    build_pool as sunny_build_pool,
)
from da_models.like_day_model_knn_sunny.pjm_rto_hourly.pipelines.forecast_single_day import (  # noqa: E402
    run as sunny_run,
)
from utils.logging_utils import (  # noqa: E402
    Colors,
    print_divider,
    print_header,
    supports_color,
)

_COLOR_ON: bool = supports_color()
_HL_LEADER: str = Colors.BOLD if _COLOR_ON else ""
_HL_WIN: str = Colors.BRIGHT_GREEN if _COLOR_ON else ""
_HL_LOSS: str = Colors.BRIGHT_RED if _COLOR_ON else ""
_HL_ACTUAL: str = (Colors.BOLD + Colors.BRIGHT_RED) if _COLOR_ON else ""
_RS: str = Colors.RESET if _COLOR_ON else ""


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date = date(2026, 5, 1)
KNN_FLT_RADII: tuple[int, ...] = (0, 1, 3)

# knn spec: the sunny-aligned spec (load + ramps + solar + wind + net_load
# + temp + outages + gas + calendar). Matches what sunny exercises so the
# comparison isolates engine behavior, not feature scope.
KNN_MODEL_NAME: str = knn_configs.PJM_RTO_HOURLY_SUNNY_ALIGNED_SPEC.name
SUNNY_MODEL_NAME: str = sunny_configs.PJM_RTO_HOURLY_SUNNY_SPEC.name

_DOW_ABBR: tuple[str, ...] = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


# ── Scenario execution ─────────────────────────────────────────────────────


def _execute_knn(
    target_date: date,
    flt_radius: int,
    pool: pd.DataFrame,
    query: pd.Series,
    dates_meta: pd.DataFrame,
) -> dict:
    """One knn forecast run, captured to a flat row including output_table."""
    started = time.perf_counter()
    base = {
        "engine": f"knn_flt{flt_radius}",
        "target_date": target_date,
        "status": "ok",
        "error_message": None,
        "output_table": None,
    }
    try:
        result = knn_run(
            target_date=target_date,
            model_name=KNN_MODEL_NAME,
            flt_radius=flt_radius,
            pool=pool,
            query=query,
            dates_meta=dates_meta,
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
            "output_table": result.get("output_table"),
            "mae": metrics.get("mae"),
            "rmse": metrics.get("rmse"),
            "rmae": metrics.get("rmae"),
            "crps": metrics.get("crps"),
            "mean_pinball": metrics.get("mean_pinball"),
            "coverage_90pct": metrics.get("coverage_90pct"),
            "sharpness_90pct": metrics.get("sharpness_90pct"),
            "duration_s": round(time.perf_counter() - started, 3),
        }
    )
    return base


def _execute_sunny(target_date: date, pool: pd.DataFrame) -> dict:
    """One sunny forecast run, captured to a flat row including output_table."""
    started = time.perf_counter()
    base = {
        "engine": "sunny",
        "target_date": target_date,
        "status": "ok",
        "error_message": None,
        "output_table": None,
    }
    try:
        result = sunny_run(
            target_date=target_date,
            model_name=SUNNY_MODEL_NAME,
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
    base.update(
        {
            "n_pool": result.get("n_pool"),
            "n_analogs_used": result.get("n_analogs_used"),
            "output_table": result.get("output_table"),
            "mae": metrics.get("mae"),
            "rmse": metrics.get("rmse"),
            "rmae": metrics.get("rmae"),
            "crps": metrics.get("crps"),
            "mean_pinball": metrics.get("mean_pinball"),
            "coverage_90pct": metrics.get("coverage_90pct"),
            "sharpness_90pct": metrics.get("sharpness_90pct"),
            "duration_s": round(time.perf_counter() - started, 3),
        }
    )
    return base


# ── Output ─────────────────────────────────────────────────────────────────


def _format_metric(v, width: int, fmt: str = ".2f") -> str:
    if v is None or pd.isna(v):
        return f"{'n/a':>{width}}"
    return f"{v:>{width}{fmt}}"


def _format_pct(v, width: int) -> str:
    if v is None or pd.isna(v):
        return f"{'n/a':>{width}}"
    return f"{v * 100:>{width - 1}.1f}%"


def _print_hourly_forecasts(
    rows: list[dict],
    target_date: date,
    width: int = 220,
) -> None:
    """Stacked hourly forecast comparison: Actual + one row per engine.

    Columns: Date | Type | HE1..HE24 | OnPeak | OffPeak | Flat. Engines
    sorted by MAE asc (best directly under Actual) when actuals are
    available; otherwise in input order.
    """
    ok = [
        r for r in rows if r.get("status") == "ok" and r.get("output_table") is not None
    ]
    if not ok:
        print()
        print_header("HOURLY FORECASTS  --  no successful runs", "=", width)
        return

    # Pull the Actual row from any engine's output_table — they all
    # source from the same LMP parquet so the actuals are identical.
    actual_row: dict | None = None
    for r in ok:
        ot = r["output_table"]
        actuals = ot[ot["Type"] == "Actual"]
        if len(actuals):
            actual_row = actuals.iloc[0].to_dict()
            actual_row["Date"] = str(target_date)
            actual_row["Type"] = "Actual"
            break

    # Pull each engine's Forecast row.
    forecast_rows: list[dict] = []
    for r in ok:
        ot = r["output_table"]
        fc = ot[ot["Type"] == "Forecast"]
        if not len(fc):
            continue
        fr = fc.iloc[0].to_dict()
        fr["Date"] = str(target_date)
        fr["Type"] = r["engine"]
        forecast_rows.append((r, fr))

    # Sort engines by MAE asc when actuals exist; else preserve input order.
    has_actuals = actual_row is not None and any(
        pd.notna(r.get("mae")) for r, _ in forecast_rows
    )
    if has_actuals:
        forecast_rows.sort(
            key=lambda pair: (
                float(pair[0]["mae"]) if pd.notna(pair[0].get("mae")) else float("inf")
            )
        )

    target_dow = _DOW_ABBR[target_date.weekday()]
    print()
    print_header(
        f"HOURLY FORECASTS  --  {target_date}  ({target_dow})",
        "=",
        width,
    )
    print()
    print(
        "  Actual row (red) at top + one Forecast row per engine. Engines"
        " sorted by MAE asc (best directly under Actual) when actuals"
        " are available."
    )
    print()

    he_w = 6
    sum_w = 7
    type_w = 14
    header = f"{'Date':<12} {'Type':<{type_w}}"
    for h in range(1, 25):
        header += f" {h:>{he_w}}"
    for label in ("OnPk", "OffPk", "Flat"):
        header += f" {label:>{sum_w}}"
    print(header)
    print_divider("-", len(header), dim=False)

    def _fmt_row(row: dict, color: str = "") -> str:
        line = f"{str(row.get('Date', '')):<12} {str(row.get('Type', '')):<{type_w}}"
        for h in range(1, 25):
            v = row.get(f"HE{h}")
            line += f" {v:>{he_w}.1f}" if pd.notna(v) else f" {'':>{he_w}}"
        for col in ("OnPeak", "OffPeak", "Flat"):
            v = row.get(col)
            line += f" {v:>{sum_w}.2f}" if pd.notna(v) else f" {'':>{sum_w}}"
        if color:
            line = f"{color}{line}{_RS}"
        return line

    if actual_row is not None:
        print(_fmt_row(actual_row, _HL_ACTUAL))
    best_engine = (
        forecast_rows[0][0]["engine"] if forecast_rows and has_actuals else None
    )
    for r, fr in forecast_rows:
        color = _HL_LEADER if r["engine"] == best_engine else ""
        print(_fmt_row(fr, color))
    print_divider("-", len(header), dim=False)


def _print_metrics(rows: list[dict], width: int = 120) -> None:
    """Per-engine metrics for the single target date, sorted by MAE asc."""
    print()
    print_header("PER-ENGINE METRICS", "=", width)
    print()
    df = pd.DataFrame(rows)
    ok = df[df["status"] == "ok"]
    failed = df[df["status"] == "failed"]
    if ok.empty:
        print("  No successful runs.")
        if not failed.empty:
            for _, r in failed.iterrows():
                print(f"    {r['engine']:<12} {r['target_date']}  {r['error_message']}")
        return

    ordered = ok.sort_values("mae", ascending=True, na_position="last").reset_index(
        drop=True
    )
    name_w = max(8, max(len(e) for e in ordered["engine"]) + 1)
    head = (
        f"  {'engine':<{name_w}} "
        f"{'mae':>8} {'rmse':>8} {'rmae':>7} {'crps':>8} "
        f"{'cov_90':>7} {'sharp_90':>9} {'analogs':>8} {'sec':>5}"
    )
    print(head)
    print_divider("-", len(head), dim=False)

    best_mae = ordered["mae"].min() if not ordered["mae"].isna().all() else None
    for _, r in ordered.iterrows():
        line = (
            f"  {str(r['engine']):<{name_w}} "
            f"{_format_metric(r['mae'], 8)} "
            f"{_format_metric(r['rmse'], 8)} "
            f"{_format_metric(r['rmae'], 7, '.3f')} "
            f"{_format_metric(r['crps'], 8, '.3f')} "
            f"{_format_pct(r['coverage_90pct'], 7)} "
            f"{_format_metric(r['sharpness_90pct'], 9)} "
            f"{int(r['n_analogs_used']) if pd.notna(r['n_analogs_used']) else 0:>8d} "
            f"{_format_metric(r['duration_s'], 5, '.1f')}"
        )
        if best_mae is not None and pd.notna(r["mae"]) and r["mae"] == best_mae:
            line = f"{_HL_LEADER}{line}{_RS}"
        print(line)
    print_divider("-", len(head), dim=False)
    print()
    if best_mae is not None:
        winner = ordered.loc[ordered["mae"] == best_mae, "engine"].iloc[0]
        print(f"  Best MAE: {winner}  ({best_mae:.2f} $/MWh)")
    print(
        "  Lower mae / rmse / rmae = better point forecast. Higher cov_90"
        " (closer to 90%) and lower sharp_90 = better calibrated bands."
    )

    if not failed.empty:
        print()
        print("  Failed runs:")
        for _, r in failed.iterrows():
            print(f"    {r['engine']:<12} {r['error_message']}")


# ── main ───────────────────────────────────────────────────────────────────


def run(
    target_date: date = TARGET_DATE,
    knn_flt_radii: tuple[int, ...] = KNN_FLT_RADII,
) -> dict:
    """Single-day cross-family engine comparison. Prints config + hourly
    forecasts + metrics. Returns the row list for notebook consumption."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    target_dow = _DOW_ABBR[target_date.weekday()]

    print_header("ENGINE COMPARISON  --  knn (wide) vs sunny (long)", "=", 110)
    print()
    print(f"  Target date         {target_date}  ({target_dow})")
    print(f"  knn spec            {KNN_MODEL_NAME}")
    print(f"  knn flt_radius      {knn_flt_radii}")
    print(f"  sunny spec          {SUNNY_MODEL_NAME}")
    print()

    print("[engine-cmp] building knn pool...")
    t0 = time.perf_counter()
    knn_base_spec = knn_configs.MODEL_REGISTRY[KNN_MODEL_NAME]
    knn_spec_for_build = replace(
        knn_base_spec, flt_radius=int(knn_base_spec.flt_radius)
    )
    knn_pool = knn_build_pool(spec=knn_spec_for_build, cache_dir=knn_configs.CACHE_DIR)
    knn_dates_meta = _knn_shared.load_dates_daily(knn_configs.CACHE_DIR)
    print(
        f"[engine-cmp] knn pool: {len(knn_pool)} rows in"
        f" {time.perf_counter() - t0:.1f}s"
    )

    print("[engine-cmp] building sunny pool...")
    t0 = time.perf_counter()
    sunny_spec = sunny_configs.MODEL_REGISTRY[SUNNY_MODEL_NAME]
    sunny_pool = sunny_build_pool(spec=sunny_spec, cache_dir=sunny_configs.CACHE_DIR)
    print(
        f"[engine-cmp] sunny pool: {len(sunny_pool)} rows in"
        f" {time.perf_counter() - t0:.1f}s"
    )

    if actuals_from_pool(knn_pool, target_date) is None:
        print(
            f"[engine-cmp] WARNING: target_date={target_date} has no full LMP"
            " actuals in the knn pool — MAE/rMAE/CRPS will be NaN. Pick a"
            " recent weekday with actuals to get a real comparison."
        )

    knn_query = knn_build_query_row(
        target_date=target_date,
        cache_dir=knn_configs.CACHE_DIR,
        spec=knn_spec_for_build,
    )

    rows: list[dict] = []
    print()
    for flt in knn_flt_radii:
        row = _execute_knn(target_date, flt, knn_pool, knn_query, knn_dates_meta)
        tag = "OK" if row["status"] == "ok" else "FAIL"
        mae_str = (
            f"MAE={row['mae']:.2f}"
            if row.get("mae") is not None and pd.notna(row.get("mae"))
            else "MAE=n/a"
        )
        print(
            f"[engine-cmp]   knn_flt{flt}    {tag:<4}"
            f" {mae_str}  ({row['duration_s']:.2f}s)"
        )
        rows.append(row)

    srow = _execute_sunny(target_date, sunny_pool)
    tag = "OK" if srow["status"] == "ok" else "FAIL"
    mae_str = (
        f"MAE={srow['mae']:.2f}"
        if srow.get("mae") is not None and pd.notna(srow.get("mae"))
        else "MAE=n/a"
    )
    print(
        f"[engine-cmp]   sunny         {tag:<4} {mae_str}  ({srow['duration_s']:.2f}s)"
    )
    rows.append(srow)

    _print_hourly_forecasts(rows, target_date)
    _print_metrics(rows)
    print()

    return {
        "rows": rows,
        "target_date": target_date,
    }


if __name__ == "__main__":
    raise NotImplementedError(
        "T4: needs long-format migration. The wide-engine alternative"
        " this script compares against was removed in the T4 cutover;"
        " what remains of the comparison must be rewritten against the"
        " long-format engine alone (or deleted if no longer useful)."
        " Slated for T4 Session 2."
    )
    run()
