"""Backtest the forward-only KNN model over a window of past dates.

For each day D in [start, end]:
  - Run run_forecast(target_date=D)
  - The model's analog selection is already restricted to dates < D, so this
    is leakage-safe by construction (see engine.py:104).
  - If D is in the pool with realized DA LMPs, capture (forecast, actual) pairs
    for HE1..HE24 and the P10/P25/P50/P75/P90 fan.

Caveat: query-side forecast inputs (load forecast, weather forecast, solar/wind)
come from the *current* cache, not snapshots issued on D-1. So this is a
"near-realistic" backtest, not a perfect replay. Good enough for relative
comparison across model variants; treat absolute numbers with a grain of salt.

Outputs (modelling/backtests/output/):
  - backtest_hourly_<YYYY-MM-DD>.csv    : one row per (date, hour)
  - backtest_daily_<YYYY-MM-DD>.csv     : one row per date with daily aggregates
  - backtest_summary_<YYYY-MM-DD>.txt   : headline metrics + slices (printed too)

Usage:
    python modelling/backtests/forward_only_knn_backtest.py
    python modelling/backtests/forward_only_knn_backtest.py --days 365
    python modelling/backtests/forward_only_knn_backtest.py --start 2025-04-27 --end 2026-04-26
    python modelling/backtests/forward_only_knn_backtest.py --days 30 --quiet
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# Ensure modelling/ is importable regardless of CWD.
_MODELLING_ROOT = Path(__file__).resolve().parent.parent
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from da_models.forward_only_knn.pipelines.forecast import run_forecast  # noqa: E402
from utils import logging_utils  # noqa: E402

OUTPUT_DIR = Path(__file__).parent / "output"
HOURS = list(range(1, 25))
QUANTILE_LABELS = ("P10", "P25", "P50", "P75", "P90")


@dataclass
class DayResult:
    target_date: date
    hourly: list[dict]   # one dict per hour with forecast/actual/quantiles
    n_analogs: int
    skipped_reason: str | None = None


def _hourly_records(target_date: date, output_table: pd.DataFrame, quantiles_table: pd.DataFrame) -> list[dict]:
    """Pull (forecast, actual, quantiles) per hour from a run_forecast result."""
    rows_by_type = {row["Type"]: row for _, row in output_table.iterrows()}
    forecast_row = rows_by_type.get("Forecast")
    actual_row = rows_by_type.get("Actual")
    if forecast_row is None or actual_row is None:
        return []

    q_rows_by_type = {row["Type"]: row for _, row in quantiles_table.iterrows()}

    records: list[dict] = []
    for h in HOURS:
        col = f"HE{h}"
        f = forecast_row.get(col)
        a = actual_row.get(col)
        if pd.isna(f) or pd.isna(a):
            continue

        rec: dict = {
            "date": target_date,
            "hour_ending": h,
            "forecast": float(f),
            "actual": float(a),
            "error": float(f) - float(a),
        }
        for q_label in QUANTILE_LABELS:
            q_row = q_rows_by_type.get(q_label)
            if q_row is not None and not pd.isna(q_row.get(col)):
                rec[q_label.lower()] = float(q_row[col])
            else:
                rec[q_label.lower()] = np.nan
        records.append(rec)
    return records


def _run_one_day(target_date: date) -> DayResult:
    try:
        result = run_forecast(target_date=target_date)
    except Exception as exc:
        return DayResult(target_date=target_date, hourly=[], n_analogs=0, skipped_reason=f"error:{exc}")

    if not result.get("has_actuals"):
        return DayResult(target_date=target_date, hourly=[], n_analogs=result.get("n_analogs_used", 0), skipped_reason="no_actuals")

    recs = _hourly_records(target_date, result["output_table"], result["quantiles_table"])
    if not recs:
        return DayResult(target_date=target_date, hourly=[], n_analogs=result.get("n_analogs_used", 0), skipped_reason="no_hours_with_both")

    return DayResult(target_date=target_date, hourly=recs, n_analogs=result.get("n_analogs_used", 0))


def _pinball(actual: np.ndarray, pred_q: np.ndarray, q: float) -> float:
    err = actual - pred_q
    return float(np.mean(np.maximum(q * err, (q - 1.0) * err)))


def _summary(df: pd.DataFrame) -> dict:
    """Headline metrics across the whole window."""
    if len(df) == 0:
        return {}

    abs_err = (df["forecast"] - df["actual"]).abs()
    err = df["forecast"] - df["actual"]

    covered_80 = ((df["actual"] >= df["p10"]) & (df["actual"] <= df["p90"])).mean()
    covered_50 = ((df["actual"] >= df["p25"]) & (df["actual"] <= df["p75"])).mean()

    return {
        "n_days": int(df["date"].nunique()),
        "n_hours": int(len(df)),
        "MAE": float(abs_err.mean()),
        "MedAE": float(abs_err.median()),
        "RMSE": float(np.sqrt((err ** 2).mean())),
        "Bias (mean error)": float(err.mean()),
        "P10-P90 coverage": float(covered_80),
        "P25-P75 coverage": float(covered_50),
        "Pinball P10": _pinball(df["actual"].to_numpy(), df["p10"].to_numpy(), 0.10),
        "Pinball P50": _pinball(df["actual"].to_numpy(), df["p50"].to_numpy(), 0.50),
        "Pinball P90": _pinball(df["actual"].to_numpy(), df["p90"].to_numpy(), 0.90),
    }


def _slice_table(df: pd.DataFrame, by_col: str) -> pd.DataFrame:
    """MAE / Bias / count grouped by a column."""
    g = df.groupby(by_col)
    out = pd.DataFrame({
        "n_hours": g.size(),
        "MAE": (df["forecast"] - df["actual"]).abs().groupby(df[by_col]).mean(),
        "Bias": (df["forecast"] - df["actual"]).groupby(df[by_col]).mean(),
        "actual_mean": df["actual"].groupby(df[by_col]).mean(),
    })
    return out


def _daily_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """One row per date with daily summary."""
    g = df.groupby("date")
    return pd.DataFrame({
        "n_hours": g.size(),
        "MAE": (df["forecast"] - df["actual"]).abs().groupby(df["date"]).mean(),
        "Bias": (df["forecast"] - df["actual"]).groupby(df["date"]).mean(),
        "actual_mean": df["actual"].groupby(df["date"]).mean(),
        "forecast_mean": df["forecast"].groupby(df["date"]).mean(),
    }).reset_index()


def run_backtest(start_date: date, end_date: date, output_dir: Path, log) -> tuple[pd.DataFrame, dict]:
    output_dir.mkdir(parents=True, exist_ok=True)

    dates = pd.date_range(start_date, end_date, freq="D").date
    n = len(dates)
    log.section(f"Backtest {start_date} -> {end_date} ({n} days)")

    all_records: list[dict] = []
    skipped: list[tuple[date, str]] = []

    for i, target_date in enumerate(dates, start=1):
        result = _run_one_day(target_date)
        if result.skipped_reason:
            skipped.append((target_date, result.skipped_reason))
        else:
            all_records.extend(result.hourly)

        if i % 25 == 0 or i == n:
            log.progress(i, n, prefix="  progress")

    if not all_records:
        raise RuntimeError("Backtest produced no usable rows. Check that the cache contains DA LMPs for the requested window.")

    df = pd.DataFrame(all_records).sort_values(["date", "hour_ending"]).reset_index(drop=True)

    # Enrich with calendar slices
    dt = pd.to_datetime(df["date"])
    df["dow_name"] = dt.dt.day_name()
    df["month"] = dt.dt.month

    # Save outputs
    today_tag = date.today().isoformat()
    hourly_path = output_dir / f"backtest_hourly_{today_tag}.csv"
    daily_path = output_dir / f"backtest_daily_{today_tag}.csv"
    summary_path = output_dir / f"backtest_summary_{today_tag}.txt"

    df.to_csv(hourly_path, index=False)
    daily_df = _daily_aggregate(df)
    daily_df.to_csv(daily_path, index=False)

    headline = _summary(df)

    # Slice tables
    by_hour = _slice_table(df, "hour_ending").round(3)
    by_dow = _slice_table(df, "dow_name").round(3)
    by_month = _slice_table(df, "month").round(3)

    worst_days = (
        daily_df.sort_values("MAE", ascending=False)
        .head(20)
        .round(3)
    )

    # Print + save summary
    lines: list[str] = []
    lines.append(f"Backtest window: {start_date} -> {end_date}")
    lines.append(f"Days requested: {n}")
    lines.append(f"Days used:      {df['date'].nunique()}")
    lines.append(f"Days skipped:   {len(skipped)}")
    if skipped:
        reasons = pd.Series([r for _, r in skipped]).value_counts().to_dict()
        lines.append(f"Skip reasons:   {reasons}")
    lines.append("")
    lines.append("=== Headline ($/MWh) ===")
    for k, v in headline.items():
        if isinstance(v, float):
            lines.append(f"  {k:<22}{v:>10.3f}")
        else:
            lines.append(f"  {k:<22}{v:>10}")
    lines.append("")
    lines.append("=== MAE by hour-of-day ===")
    lines.append(by_hour.to_string())
    lines.append("")
    lines.append("=== MAE by day-of-week ===")
    lines.append(by_dow.to_string())
    lines.append("")
    lines.append("=== MAE by month ===")
    lines.append(by_month.to_string())
    lines.append("")
    lines.append("=== Worst 20 days by MAE ===")
    lines.append(worst_days.to_string(index=False))

    summary_path.write_text("\n".join(lines), encoding="utf-8")

    log.section("Results")
    for line in lines:
        log.info(line)

    log.section("Files written")
    log.success(f"Hourly:  {hourly_path}")
    log.success(f"Daily:   {daily_path}")
    log.success(f"Summary: {summary_path}")

    return df, headline


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest forward-only KNN over a date window")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (defaults to end - days + 1)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (defaults to today - 1)")
    parser.add_argument("--days", type=int, default=365, help="Window size if --start not given (default 365)")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Where CSVs and summary land")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-day skip warnings")
    args = parser.parse_args()

    log = logging_utils.init_logging(
        name="forward_only_knn_backtest",
        log_dir=_MODELLING_ROOT / "logs",
    )

    if args.quiet:
        import logging as _logging
        _logging.getLogger("da_models.forward_only_knn.similarity.engine").setLevel(_logging.ERROR)
        _logging.getLogger("da_models.forward_only_knn.similarity.filtering").setLevel(_logging.ERROR)
        _logging.getLogger("da_models.forward_only_knn.features.builder").setLevel(_logging.ERROR)
        _logging.getLogger("da_models.forward_only_knn.pipelines.forecast").setLevel(_logging.ERROR)
        _logging.getLogger("da_models.forward_only_knn.validation.preflight").setLevel(_logging.ERROR)

    end_date = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    start_date = date.fromisoformat(args.start) if args.start else end_date - timedelta(days=args.days - 1)

    if start_date > end_date:
        log.error(f"start_date {start_date} is after end_date {end_date}")
        return 1

    try:
        run_backtest(
            start_date=start_date,
            end_date=end_date,
            output_dir=Path(args.output_dir).expanduser(),
            log=log,
        )
    except Exception as exc:
        log.exception(f"Backtest failed: {exc}")
        return 1
    finally:
        log.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
