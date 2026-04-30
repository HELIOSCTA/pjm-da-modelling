"""Verify the actuals and forecast loaders by joining RTO load on
``(date, hour_ending)`` and reporting overlap, alignment, and forecast error.

The join only returns rows where the cache holds BOTH an actual and a
forecast for the same (date, hour_ending). With the current cache layout
the forecast file is forward-only, so overlap is typically limited to the
current day's already-elapsed hours — enough to sanity-check that the
column wiring and date alignment are correct, but not enough to compute
meaningful long-run error stats.

Defaults are tuned for the package layout. To run against a different
region or cache, edit ``REGION`` / ``CACHE_DIR`` below or call
``run(region=..., cache_dir=...)`` from a notebook.

Usage (from anywhere)::

    python -m da_models.common.data.verify_data_loader
"""
from __future__ import annotations

import sys
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[3]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.common.data import loader  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ──────────────────────
REGION: str = "RTO"
CACHE_DIR: Path | None = None  # None = use loader's default cache


def join_load_actuals_and_forecast(
    region: str = REGION,
    cache_dir: Path | None = CACHE_DIR,
) -> pd.DataFrame:
    """Inner-join RT actuals and DA forecast for the given region."""
    actuals = loader.load_load_rt(cache_dir=cache_dir)
    forecast = loader.load_load_forecast(cache_dir=cache_dir)

    actuals = actuals[actuals["region"] == region][["date", "hour_ending", "rt_load_mw"]]
    forecast = forecast[forecast["region"] == region][
        ["date", "hour_ending", "forecast_load_mw"]
    ]

    joined = actuals.merge(forecast, on=["date", "hour_ending"], how="inner")
    joined["error_mw"] = joined["forecast_load_mw"] - joined["rt_load_mw"]
    joined["error_pct"] = (joined["error_mw"] / joined["rt_load_mw"]) * 100
    return joined.sort_values(["date", "hour_ending"]).reset_index(drop=True)


def _coverage_summary(
    actuals: pd.DataFrame, forecast: pd.DataFrame, region: str,
) -> str:
    a = actuals[actuals["region"] == region]
    f = forecast[forecast["region"] == region]
    lines = [
        f"actuals : {len(a):>10,} rows  date range "
        f"{a['date'].min()} -> {a['date'].max()}",
        f"forecast: {len(f):>10,} rows  date range "
        f"{f['date'].min()} -> {f['date'].max()}",
    ]
    return "\n".join(lines)


def report(region: str, joined: pd.DataFrame, cache_dir: Path | None) -> None:
    print(f"=== verify_data_loader: actuals JOIN forecast (region={region}) ===")
    print()
    actuals = loader.load_load_rt(cache_dir=cache_dir)
    forecast = loader.load_load_forecast(cache_dir=cache_dir)
    print("Source coverage:")
    print(_coverage_summary(actuals, forecast, region))
    print()

    n = len(joined)
    print(f"Joined rows (overlap): {n:,}")
    if n == 0:
        print()
        print("No overlap. The cache likely only holds forward-looking forecasts.")
        print("Run again after a forecast archive is being built to see meaningful")
        print("error statistics; for now, this just confirms the loaders return")
        print("zero-overlap as expected, not that they are broken.")
        return

    print(
        f"Distinct dates: {joined['date'].nunique()}  "
        f"({joined['date'].min()} -> {joined['date'].max()})"
    )
    print()

    err = joined["error_mw"].dropna()
    pct = joined["error_pct"].dropna()
    print("Forecast error (forecast - actual, MW):")
    print(
        f"  count={len(err):>5,}  "
        f"mean={err.mean():>10,.1f}  "
        f"std={err.std():>10,.1f}  "
        f"MAE={err.abs().mean():>10,.1f}  "
        f"MAPE={pct.abs().mean():>5.2f}%"
    )
    print()

    print("Sample rows (first 8):")
    sample = joined.head(8).copy()
    for col in ("rt_load_mw", "forecast_load_mw", "error_mw", "error_pct"):
        sample[col] = sample[col].round(1)
    print(sample.to_string(index=False))


def run(
    region: str = REGION,
    cache_dir: Path | None = CACHE_DIR,
) -> None:
    """Top-level entry point. Importable and runnable; takes defaults."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    joined = join_load_actuals_and_forecast(region=region, cache_dir=cache_dir)
    report(region, joined, cache_dir=cache_dir)


if __name__ == "__main__":
    run()
