"""Head-to-head: meteo_regional_hourly vs pjm_rto_hourly on a single day.

Runs both single-day pipelines for the same target date, then prints:
  1. Side-by-side per-HE LMP table (Actual / RTO-Fcst / RTO-Err /
     Regional-Fcst / Regional-Err / Winner).
  2. Headline metrics for both runs against the same naive d-7 baseline.
  3. Analog-overlap diagnostic per HE — if the two models share most
     analogs, the test isn't really exercising the regional-vs-RTO
     hypothesis.

Single-day result is a directional read, not a verdict. Multi-day
backtest is required to confirm the hypothesis.

Usage::

    python -m da_models.like_day_model_knn_sunny.meteo_regional_hourly.pipelines.head_to_head
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.like_day_model_knn_sunny.meteo_regional_hourly.pipelines import (  # noqa: E402
    forecast_single_day as meteo_pipeline,
)
from da_models.like_day_model_knn_sunny.pjm_rto_hourly.pipelines import (  # noqa: E402
    forecast_single_day as rto_pipeline,
)


TARGET_DATE: date | None = date(2026, 5, 4)
WIN_THRESHOLD_USD: float = 2.0  # flag hours where one model wins by more than this


def _reconfigure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _print_header(title: str) -> None:
    line = "=" * 78
    print(line)
    print(title)
    print(line)


def _print_subheader(title: str) -> None:
    print()
    print(title)
    print("-" * 78)


def _build_side_by_side(
    rto_df: pd.DataFrame,
    meteo_df: pd.DataFrame,
    actuals_by_he: dict[int, float],
) -> pd.DataFrame:
    rto = rto_df[["hour_ending", "point_forecast"]].rename(
        columns={"point_forecast": "rto_forecast"}
    )
    meteo = meteo_df[["hour_ending", "point_forecast"]].rename(
        columns={"point_forecast": "regional_forecast"}
    )
    out = rto.merge(meteo, on="hour_ending", how="outer").sort_values("hour_ending")
    out["actual"] = out["hour_ending"].map(actuals_by_he)
    out["rto_err"] = out["rto_forecast"] - out["actual"]
    out["regional_err"] = out["regional_forecast"] - out["actual"]
    return out.reset_index(drop=True)


def _format_value(val: float, width: int = 8, prec: int = 2) -> str:
    if val is None or (isinstance(val, float) and not np.isfinite(val)):
        return f"{'--':>{width}}"
    return f"{val:>{width}.{prec}f}"


def _print_side_by_side_table(table: pd.DataFrame) -> None:
    cols = [
        "HE",
        "Actual",
        "RTO-Fcst",
        "RTO-Err",
        "Reg-Fcst",
        "Reg-Err",
        "Winner",
        "Gap",
    ]
    widths = [3, 8, 9, 8, 9, 8, 7, 7]
    header = "  ".join(f"{c:>{w}}" for c, w in zip(cols, widths))
    print(header)
    print("-" * len(header))

    for _, r in table.iterrows():
        he = int(r["hour_ending"])
        actual = r.get("actual")
        rto_fc = r.get("rto_forecast")
        reg_fc = r.get("regional_forecast")
        rto_err = r.get("rto_err")
        reg_err = r.get("regional_err")

        winner = "--"
        gap_disp = "--"
        if (
            pd.notna(rto_err)
            and pd.notna(reg_err)
            and np.isfinite(rto_err)
            and np.isfinite(reg_err)
        ):
            gap = abs(rto_err) - abs(reg_err)  # positive => regional better
            if gap > WIN_THRESHOLD_USD:
                winner = "REG"
            elif gap < -WIN_THRESHOLD_USD:
                winner = "RTO"
            else:
                winner = "tie"
            gap_disp = f"{gap:+.2f}"

        row = [
            f"{he:>3}",
            _format_value(actual, 8),
            _format_value(rto_fc, 9),
            _format_value(rto_err, 8),
            _format_value(reg_fc, 9),
            _format_value(reg_err, 8),
            f"{winner:>7}",
            f"{gap_disp:>7}",
        ]
        print("  ".join(row))

    actuals = table["actual"].astype(float)
    rto_err = table["rto_err"].astype(float)
    reg_err = table["regional_err"].astype(float)
    rto_mae = float(np.nanmean(np.abs(rto_err))) if rto_err.notna().any() else np.nan
    reg_mae = float(np.nanmean(np.abs(reg_err))) if reg_err.notna().any() else np.nan
    rto_bias = float(np.nanmean(rto_err)) if rto_err.notna().any() else np.nan
    reg_bias = float(np.nanmean(reg_err)) if reg_err.notna().any() else np.nan
    actual_mean = float(np.nanmean(actuals)) if actuals.notna().any() else np.nan

    print("-" * len(header))
    summary_cols = ["", "Avg", "", "MAE", "", "MAE", "", ""]
    print("  ".join(f"{c:>{w}}" for c, w in zip(summary_cols, widths)))
    summary_row = [
        f"{'sum':>3}",
        _format_value(actual_mean, 8),
        f"{'':>9}",
        _format_value(rto_mae, 8),
        f"{'':>9}",
        _format_value(reg_mae, 8),
        f"{'':>7}",
        f"{'':>7}",
    ]
    print("  ".join(summary_row))
    print(
        f"  RTO     bias={rto_bias:+.2f}  MAE={rto_mae:.2f}    "
        f"Regional bias={reg_bias:+.2f}  MAE={reg_mae:.2f}"
    )
    if pd.notna(rto_mae) and pd.notna(reg_mae):
        delta = rto_mae - reg_mae
        verdict = (
            "regional better" if delta > 0 else ("RTO better" if delta < 0 else "tie")
        )
        print(f"  Delta MAE (RTO - Regional) = {delta:+.2f}  -> {verdict}")


def _print_metrics_block(rto_metrics: dict, meteo_metrics: dict) -> None:
    if not rto_metrics and not meteo_metrics:
        print("  (no metrics — actuals not available)")
        return
    keys = ["mae", "rmse", "mape", "rmae", "bias", "crps"]
    keys = [k for k in keys if k in rto_metrics or k in meteo_metrics]
    if not keys:
        keys = sorted(set(rto_metrics) | set(meteo_metrics))
    width_k = max(len(k) for k in keys) + 2
    print(f"  {'metric':<{width_k}}  {'RTO':>10}  {'Regional':>10}  {'delta':>10}")
    print("  " + "-" * (width_k + 38))
    for k in keys:
        rv = rto_metrics.get(k)
        mv = meteo_metrics.get(k)
        rv_disp = (
            f"{rv:>10.4f}"
            if isinstance(rv, (int, float)) and rv is not None
            else f"{'--':>10}"
        )
        mv_disp = (
            f"{mv:>10.4f}"
            if isinstance(mv, (int, float)) and mv is not None
            else f"{'--':>10}"
        )
        if isinstance(rv, (int, float)) and isinstance(mv, (int, float)):
            delta_disp = f"{rv - mv:>+10.4f}"
        else:
            delta_disp = f"{'--':>10}"
        print(f"  {k:<{width_k}}  {rv_disp}  {mv_disp}  {delta_disp}")
    print("  (delta = RTO - Regional; positive => Regional has lower value)")


def _analog_overlap(rto_analogs: pd.DataFrame, meteo_analogs: pd.DataFrame) -> None:
    rto = rto_analogs[["hour_ending", "rank", "date"]].copy()
    meteo = meteo_analogs[["hour_ending", "rank", "date"]].copy()
    rto["date"] = pd.to_datetime(rto["date"]).dt.date
    meteo["date"] = pd.to_datetime(meteo["date"]).dt.date

    print(f"  {'HE':>3}  {'N_RTO':>6}  {'N_REG':>6}  {'Shared':>7}  {'Jaccard':>8}")
    print("  " + "-" * 38)
    jaccards: list[float] = []
    for he in range(1, 25):
        r_set = set(rto[rto["hour_ending"] == he]["date"].tolist())
        m_set = set(meteo[meteo["hour_ending"] == he]["date"].tolist())
        if not r_set and not m_set:
            continue
        shared = r_set & m_set
        union = r_set | m_set
        jac = len(shared) / len(union) if union else 0.0
        jaccards.append(jac)
        print(
            f"  {he:>3}  {len(r_set):>6}  {len(m_set):>6}  "
            f"{len(shared):>7}  {jac:>8.2f}"
        )
    if jaccards:
        avg = float(np.mean(jaccards))
        print(f"  Avg Jaccard across HEs: {avg:.2f}")
        if avg >= 0.7:
            print(
                "  WARNING: high analog overlap — the two models pick mostly the "
                "same dates. The single-day test does not strongly exercise the "
                "regional-vs-RTO hypothesis."
            )
        elif avg <= 0.3:
            print(
                "  Low analog overlap — the two feature sets pick meaningfully "
                "different analogs. The hypothesis is being exercised."
            )


def run(target_date: date | None = TARGET_DATE) -> dict:
    """Run the head-to-head comparison and return both result dicts."""
    _reconfigure_stdio()
    resolved_date = _resolve_target_date(target_date)

    _print_header(
        "HEAD-TO-HEAD: pjm_rto_hourly_sunny  vs  pjm_meteo_regional_hourly_sunny"
    )
    print(f"  Target date: {resolved_date}")
    print(f"  Win threshold (per-HE): ${WIN_THRESHOLD_USD:.2f}/MWh")

    _print_subheader("[1/3] Running pjm_rto_hourly_sunny ...")
    rto_result = rto_pipeline.run(target_date=resolved_date, quiet=True)
    print(
        f"  pool rows = {rto_result['n_pool']}, "
        f"day_type = {rto_result['day_type']}, "
        f"has_actuals = {rto_result['has_actuals']}"
    )

    _print_subheader("[2/3] Running pjm_meteo_regional_hourly_sunny ...")
    meteo_result = meteo_pipeline.run(target_date=resolved_date, quiet=True)
    print(
        f"  pool rows = {meteo_result['n_pool']}, "
        f"day_type = {meteo_result['day_type']}, "
        f"has_actuals = {meteo_result['has_actuals']}"
    )

    rto_df = rto_result["df_forecast"]
    meteo_df = meteo_result["df_forecast"]

    actuals_by_he: dict[int, float] = {}
    output_table = rto_result.get("output_table")
    if (
        output_table is not None
        and len(output_table) > 0
        and "Type" in output_table.columns
    ):
        actual_rows = output_table[output_table["Type"].astype(str) == "Actual"]
        if len(actual_rows) > 0:
            row = actual_rows.iloc[0]
            for he in range(1, 25):
                col = f"HE{he}"
                if col in output_table.columns:
                    v = row.get(col)
                    if pd.notna(v):
                        try:
                            actuals_by_he[he] = float(v)
                        except (TypeError, ValueError):
                            pass

    _print_subheader("[3/3] Side-by-side per-HE forecast vs actual")
    table = _build_side_by_side(rto_df, meteo_df, actuals_by_he)
    _print_side_by_side_table(table)

    _print_subheader("Headline metrics (both runs, same naive d-7 baseline)")
    _print_metrics_block(rto_result.get("metrics", {}), meteo_result.get("metrics", {}))

    _print_subheader("Analog overlap (Jaccard per HE on selected analog dates)")
    _analog_overlap(rto_result["analogs"], meteo_result["analogs"])

    print()
    print("=" * 78)
    print("Single-day result is directional only — multi-day backtest required.")
    print("=" * 78)

    return {
        "target_date": resolved_date,
        "rto": rto_result,
        "meteo": meteo_result,
        "comparison_table": table,
    }


if __name__ == "__main__":
    run()
