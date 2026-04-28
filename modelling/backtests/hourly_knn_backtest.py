"""Backtest the hourly KNN model over a window of past dates.

Mirror of forward_only_knn_backtest.py but for hourly_knn. Same output schema
so the two backtests' summaries can be diffed directly.

Speed: builds the hourly pool ONCE at the start and passes it to every
run_forecast call (the model accepts a pre-built `pool` parameter). 365 days
runs in ~3 minutes vs ~20 minutes if the pool were rebuilt each call.

Weight tuning hook: --weights accepts a JSON dict overriding feature group
weights, e.g. --weights '{"load_at_hour": 4.0, "weather_at_hour": 1.0}'.
The future weight optimizer wraps this script in a search loop.

Outputs (modelling/backtests/output/):
  - hourly_knn_hourly_<YYYY-MM-DD>.csv
  - hourly_knn_daily_<YYYY-MM-DD>.csv
  - hourly_knn_summary_<YYYY-MM-DD>.txt

Usage:
    python modelling/backtests/hourly_knn_backtest.py
    python modelling/backtests/hourly_knn_backtest.py --days 365 --quiet
    python modelling/backtests/hourly_knn_backtest.py --start 2025-04-27 --end 2026-04-26
    python modelling/backtests/hourly_knn_backtest.py --weights '{"load_at_hour": 4.0}'
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

_MODELLING_ROOT = Path(__file__).resolve().parent.parent
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from da_models.hourly_knn import configs as hk_configs  # noqa: E402
from da_models.hourly_knn.forecast import build_hourly_pool, run_forecast  # noqa: E402
from utils import logging_utils  # noqa: E402

OUTPUT_DIR = Path(__file__).parent / "output"
HOURS = list(range(1, 25))
QUANTILE_LABELS = ("P10", "P25", "P50", "P75", "P90")


@dataclass
class DayResult:
    target_date: date
    hourly: list[dict]
    n_analogs: int
    skipped_reason: str | None = None


def _hourly_records(target_date: date, output_table: pd.DataFrame, quantiles_table: pd.DataFrame) -> list[dict]:
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


def _run_one_day(target_date: date, pool: pd.DataFrame, cfg: hk_configs.HourlyKNNConfig) -> DayResult:
    try:
        result = run_forecast(target_date=target_date, config=cfg, pool=pool)
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
    return pd.DataFrame({
        "n_hours": df.groupby(by_col).size(),
        "MAE": (df["forecast"] - df["actual"]).abs().groupby(df[by_col]).mean(),
        "Bias": (df["forecast"] - df["actual"]).groupby(df[by_col]).mean(),
        "actual_mean": df["actual"].groupby(df[by_col]).mean(),
    })


def _daily_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "n_hours": df.groupby("date").size(),
        "MAE": (df["forecast"] - df["actual"]).abs().groupby(df["date"]).mean(),
        "Bias": (df["forecast"] - df["actual"]).groupby(df["date"]).mean(),
        "actual_mean": df["actual"].groupby(df["date"]).mean(),
        "forecast_mean": df["forecast"].groupby(df["date"]).mean(),
    }).reset_index()


def run_backtest(
    start_date: date,
    end_date: date,
    output_dir: Path,
    log,
    feature_group_weights: dict[str, float] | None = None,
    output_prefix: str = "hourly_knn",
) -> tuple[pd.DataFrame, dict]:
    output_dir.mkdir(parents=True, exist_ok=True)

    cfg = hk_configs.HourlyKNNConfig(feature_group_weights=feature_group_weights)
    log.info(f"  weights:    {cfg.resolved_weights()}")

    log.section("Building hourly pool (one-time cost)")
    with log.timer("build_hourly_pool"):
        pool = build_hourly_pool()
    log.info(f"  pool rows:  {len(pool):,} across {pool['date'].nunique()} dates")

    dates = pd.date_range(start_date, end_date, freq="D").date
    n = len(dates)
    log.section(f"Backtest {start_date} -> {end_date} ({n} days)")

    all_records: list[dict] = []
    skipped: list[tuple[date, str]] = []

    for i, target_date in enumerate(dates, start=1):
        result = _run_one_day(target_date, pool, cfg)
        if result.skipped_reason:
            skipped.append((target_date, result.skipped_reason))
        else:
            all_records.extend(result.hourly)

        if i % 25 == 0 or i == n:
            log.progress(i, n, prefix="  progress")

    if not all_records:
        raise RuntimeError("Backtest produced no usable rows. Check cache for DA LMPs in the requested window.")

    df = pd.DataFrame(all_records).sort_values(["date", "hour_ending"]).reset_index(drop=True)
    dt = pd.to_datetime(df["date"])
    df["dow_name"] = dt.dt.day_name()
    df["month"] = dt.dt.month

    today_tag = date.today().isoformat()
    hourly_path = output_dir / f"{output_prefix}_hourly_{today_tag}.csv"
    daily_path = output_dir / f"{output_prefix}_daily_{today_tag}.csv"
    summary_path = output_dir / f"{output_prefix}_summary_{today_tag}.txt"

    df.to_csv(hourly_path, index=False)
    daily_df = _daily_aggregate(df)
    daily_df.to_csv(daily_path, index=False)

    headline = _summary(df)
    by_hour = _slice_table(df, "hour_ending").round(3)
    by_dow = _slice_table(df, "dow_name").round(3)
    by_month = _slice_table(df, "month").round(3)
    worst_days = daily_df.sort_values("MAE", ascending=False).head(20).round(3)

    lines: list[str] = []
    lines.append(f"Model:          hourly_knn")
    lines.append(f"Weights:        {cfg.resolved_weights()}")
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
    parser = argparse.ArgumentParser(description="Backtest the hourly KNN model")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (defaults to end - days + 1)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (defaults to today - 1)")
    parser.add_argument("--days", type=int, default=365, help="Window size if --start not given (default 365)")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Where CSVs and summary land")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-day model log noise")
    parser.add_argument(
        "--weights",
        type=str,
        default=None,
        help='JSON dict of feature group weights, e.g. \'{"load_at_hour": 4.0}\'. Missing groups keep defaults.',
    )
    parser.add_argument(
        "--output-prefix",
        default="hourly_knn",
        help="Filename prefix for outputs. Used by the weight optimizer to tag trials.",
    )
    args = parser.parse_args()

    log = logging_utils.init_logging(
        name="hourly_knn_backtest",
        log_dir=_MODELLING_ROOT / "logs",
    )

    if args.quiet:
        import logging as _logging
        for name in (
            "da_models.hourly_knn.forecast",
            "da_models.common.data.loader",
        ):
            _logging.getLogger(name).setLevel(_logging.ERROR)

    weights_override: dict[str, float] | None = None
    if args.weights:
        try:
            user_weights = json.loads(args.weights)
        except json.JSONDecodeError as exc:
            log.error(f"--weights is not valid JSON: {exc}")
            return 1
        # merge over defaults so the user only has to specify the ones they want to change
        weights_override = dict(hk_configs.FEATURE_GROUP_WEIGHTS)
        weights_override.update({k: float(v) for k, v in user_weights.items()})

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
            feature_group_weights=weights_override,
            output_prefix=args.output_prefix,
        )
    except Exception as exc:
        log.exception(f"Backtest failed: {exc}")
        return 1
    finally:
        log.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
