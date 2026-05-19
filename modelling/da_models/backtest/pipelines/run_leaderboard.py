"""Read a backtest replay parquet, render the cross-model leaderboard.

Outputs:
  - ``backtest/output/{run_id}_leaderboard.txt`` -- the rendered tables
  - Returns a dict with the underlying DataFrames so a notebook caller
    can re-render or extend without re-reading.

v1 leaderboard tables (per the scope decision):
  1. Point-metric overall: one row per model -- n, MAE, RMSE, bias,
     MAPE%, rMAE-vs-baseline.
  2. Point-metric by ``block`` (OnPeak HE8-23 / OffPeak): catches
     models that are fine off-peak but blow up on-peak (or vice versa).
  3. Point-metric by ``day_type`` (weekday / weekend / holiday).
  4. Quantile bands: coverage of the P10-P90 interval (target 80%),
     sharpness (band width), mean pinball loss.
  5. Price-duration curve at the 5th / 25th / 50th / 75th / 95th
     percentile -- one row per model, plus the actuals -- gives a
     trader-friendly read on "where the model misses the tails".

DM / GW pairwise tests are Tier-2 and live in ``metrics/dm_gw.py``
(not implemented in v1). Family-specific diagnostics live in
``backtest/diagnostics/`` (stubbed in v1).

Usage::

    python -m da_models.backtest.pipelines.run_leaderboard
    python modelling/da_models/backtest/pipelines/run_leaderboard.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[3]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.backtest import configs as C  # noqa: E402
from da_models.backtest import pdc, regime  # noqa: E402
from da_models.backtest.metrics.point import point_metrics_by_model  # noqa: E402
from da_models.backtest.metrics.quantile import quantile_metrics_by_model  # noqa: E402
from utils.logging_utils import init_logging, print_divider, print_header, print_section  # noqa: E402

# -- Defaults (edit here instead of using CLI flags) -----------------------
RUN_ID: str | None = None  # None -> latest *.parquet in OUTPUT_DIR
OUTPUT_DIR: Path = C.OUTPUT_DIR
BASELINE_MODEL: str = C.BASELINE_MODEL_NAME
LOG_DIR: Path = _MODELLING_ROOT / "logs"

_NUM_FMT = lambda v: "" if pd.isna(v) else f"{v:>10.3f}"  # noqa: E731


def _resolve_parquet(run_id: str | None, output_dir: Path) -> Path:
    if run_id is not None:
        p = output_dir / f"{run_id}.parquet"
        if not p.exists():
            raise FileNotFoundError(f"Backtest parquet not found: {p}")
        return p
    candidates = sorted(output_dir.glob("*.parquet"))
    if not candidates:
        raise FileNotFoundError(f"No backtest parquets in {output_dir}")
    return candidates[-1]


def _print_table(title: str, df: pd.DataFrame, cols: list[str]) -> None:
    print_section(title)
    if df.empty:
        print("  (no rows)")
        print()
        return
    keep = [c for c in cols if c in df.columns]
    disp = df[keep].copy()
    for c in keep:
        if c in ("n",) or not pd.api.types.is_numeric_dtype(disp[c]):
            continue
        disp[c] = disp[c].map(_NUM_FMT)
    print(disp.to_string(index=False))
    print()


def _pdc_percentiles(
    pdc_long: pd.DataFrame, pcts: tuple[float, ...] = (5, 25, 50, 75, 95)
) -> pd.DataFrame:
    """One row per model with the price at each ``rank_pct`` percentile of
    the descending-sorted curve. ``rank_pct=5`` means "price at the 5%
    most-expensive hour" -- the top of the curve, what scarcity sets."""
    if pdc_long.empty:
        return pd.DataFrame()
    rows: list[dict] = []
    for name, g in pdc_long.groupby("model_name", sort=False):
        g = g.sort_values("rank_pct")
        row: dict = {"model_name": name}
        for p in pcts:
            v = np.interp(p, g["rank_pct"].to_numpy(), g["price"].to_numpy())
            row[f"p{int(p):02d}"] = float(v)
        rows.append(row)
    return pd.DataFrame(rows)


def run(
    run_id: str | None = RUN_ID,
    output_dir: Path = OUTPUT_DIR,
    baseline_model: str = BASELINE_MODEL,
    quiet: bool = False,
) -> dict:
    """Score a backtest parquet. Returns ``{run_id, parquet, frame,
    overall, by_block, by_day_type, quantile_overall, pdc_long,
    pdc_pcts, output_txt}``."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="backtest_leaderboard", log_dir=LOG_DIR)
    try:
        parquet = _resolve_parquet(run_id, output_dir)
        rid = parquet.stem
        df = pd.read_parquet(parquet)
        pl.info(f"loaded {len(df):,} rows from {parquet.name}")
        df = regime.attach_all_default(df)

        overall = point_metrics_by_model(df, baseline_model=baseline_model)
        by_block = point_metrics_by_model(
            df, group_by=["block"], baseline_model=baseline_model
        )
        by_day_type = point_metrics_by_model(
            df, group_by=["day_type"], baseline_model=baseline_model
        )
        q_overall = quantile_metrics_by_model(df)
        pdc_long = pdc.build_pdc_long(df)
        pdc_pcts = _pdc_percentiles(pdc_long)

        lines: list[str] = []

        def _emit(title: str, frame: pd.DataFrame, cols: list[str]) -> None:
            lines.append(f"=== {title} ===")
            if frame.empty:
                lines.append("(no rows)")
            else:
                keep = [c for c in cols if c in frame.columns]
                disp = frame[keep].copy()
                for c in keep:
                    if c == "n" or not pd.api.types.is_numeric_dtype(disp[c]):
                        continue
                    disp[c] = disp[c].map(_NUM_FMT)
                lines.append(disp.to_string(index=False))
            lines.append("")

        _emit(
            "Point metrics -- overall",
            overall,
            ["model_name", "n", "mae", "rmse", "bias", "mape_pct", "rmae"],
        )
        _emit(
            "Point metrics -- by block (OnPeak HE8-23 / OffPeak)",
            by_block,
            ["model_name", "block", "n", "mae", "rmse", "bias", "rmae"],
        )
        _emit(
            "Point metrics -- by day_type",
            by_day_type,
            ["model_name", "day_type", "n", "mae", "rmse", "bias", "rmae"],
        )
        _emit(
            "Quantile bands -- overall",
            q_overall,
            [
                "model_name",
                "n",
                "coverage_p10_p90",
                "sharpness_p10_p90_mw",
                "pinball_mean",
            ],
        )
        _emit(
            "Price duration curve -- price at percentile",
            pdc_pcts,
            ["model_name", "p05", "p25", "p50", "p75", "p95"],
        )

        text_path = output_dir / f"{rid}_leaderboard.txt"
        text_path.write_text("\n".join(lines))

        if not quiet:
            print_header(
                f"BACKTEST LEADERBOARD  |  run_id {rid}  |  baseline={baseline_model}",
                "=",
                120,
            )
            print(
                f"  Rows: {len(df):,} | dates: {df['target_date'].min()} -> {df['target_date'].max()} | models: {sorted(df['model_name'].unique())}"
            )
            print(f"  Text artefact: {text_path}")
            print()
            _print_table(
                "Point metrics -- overall",
                overall,
                ["model_name", "n", "mae", "rmse", "bias", "mape_pct", "rmae"],
            )
            _print_table(
                "Point metrics -- by block (OnPeak HE8-23 / OffPeak)",
                by_block,
                ["model_name", "block", "n", "mae", "rmse", "bias", "rmae"],
            )
            _print_table(
                "Point metrics -- by day_type",
                by_day_type,
                ["model_name", "day_type", "n", "mae", "rmse", "bias", "rmae"],
            )
            _print_table(
                "Quantile bands -- overall",
                q_overall,
                [
                    "model_name",
                    "n",
                    "coverage_p10_p90",
                    "sharpness_p10_p90_mw",
                    "pinball_mean",
                ],
            )
            _print_table(
                "Price duration curve -- price at percentile",
                pdc_pcts,
                ["model_name", "p05", "p25", "p50", "p75", "p95"],
            )
            print_divider("=", 120, dim=False)
            print()

        return {
            "run_id": rid,
            "parquet": parquet,
            "frame": df,
            "overall": overall,
            "by_block": by_block,
            "by_day_type": by_day_type,
            "quantile_overall": q_overall,
            "pdc_long": pdc_long,
            "pdc_pcts": pdc_pcts,
            "output_txt": text_path,
        }
    finally:
        pl.close()


if __name__ == "__main__":
    run()
