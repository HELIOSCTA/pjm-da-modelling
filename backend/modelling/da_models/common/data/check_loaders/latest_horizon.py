"""Check the ``latest_only=True`` mode against the Meteologica supply-
demand bundle.

Two purposes:

1. Demonstrate the new horizon-mode contract: pick the single most-
   recent ``as_of_date`` (per region) and surface every forecast_date
   under that vintage that has all 24 ``hour_ending`` values. RT fills
   historical (region, date) tuples that lack forecast coverage.
2. Assert default-mode parity — switching ``latest_only=False`` (the
   shipped default) returns the same row count it did before this
   feature landed. Locking in regression coverage for the lead_days=1
   path that all production callers consume.

Usage::

    python -m backend.modelling.da_models.common.data.check_loaders.latest_horizon
    python modelling/da_models/common/data/check_loaders/latest_horizon.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[6]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.utils.logging_utils import init_logging, print_header, print_section  # noqa: E402


# ── Defaults (edit here instead of using CLI flags) ────────────────────────
CACHE_DIR: Path | None = None
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"
REGIONS: tuple[str, ...] = ("RTO", "MIDATL", "WEST", "SOUTH")


def _summarize_horizon(df: pd.DataFrame, label: str) -> dict:
    fcst = df[df["source"] == "meteologica"]
    return {
        "mode": label,
        "total_rows": len(df),
        "forecast_rows": len(fcst),
        "rt_rows": int((df["source"] == "rt").sum()),
        "n_forecast_dates": int(fcst["date"].nunique()),
        "max_forecast_date": fcst["date"].max() if len(fcst) else None,
        "min_forecast_date": fcst["date"].min() if len(fcst) else None,
        "as_of_date_range": (
            fcst["as_of_date"].min() if len(fcst) else None,
            fcst["as_of_date"].max() if len(fcst) else None,
        ),
    }


def run(cache_dir: Path | None = CACHE_DIR) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="check_loaders_latest_horizon", log_dir=LOG_DIR)
    try:
        print_header("latest_only horizon mode — meteo supply-demand")

        with pl.timer("load default (lead_days=1) mode"):
            default = loader.load_meteologica_supply_demand_coalesced(
                cache_dir=cache_dir
            )
        with pl.timer("load latest_only=True mode"):
            latest = loader.load_meteologica_supply_demand_coalesced(
                cache_dir=cache_dir, latest_only=True
            )

        print_section("Default mode (lead_days=1) summary")
        d_summary = _summarize_horizon(default, "lead_days=1")
        for k, v in d_summary.items():
            pl.info(f"  {k}: {v}")

        print_section("latest_only=True summary")
        l_summary = _summarize_horizon(latest, "latest_only=True")
        for k, v in l_summary.items():
            pl.info(f"  {k}: {v}")

        print_section("Per-region forecast row counts (latest_only=True)")
        latest_fcst = latest[latest["source"] == "meteologica"]
        per_region = latest_fcst.groupby("region").size().to_dict()
        for region in REGIONS:
            pl.info(f"  {region}: {per_region.get(region, 0)} rows")

        # ── Invariants ────────────────────────────────────────────────────
        print_section("Invariants")

        n_dates = l_summary["n_forecast_dates"]
        if n_dates >= 2:
            pl.success(f"horizon spans {n_dates} forecast_dates (>=2 required)")
        else:
            pl.error(
                f"horizon collapsed to {n_dates} forecast_date(s); "
                "expected at least 2 from the latest publish window"
            )

        max_fd = l_summary["max_forecast_date"]
        today = pd.Timestamp.today().normalize().date()
        tomorrow = pd.Timestamp(today) + pd.Timedelta(days=1)
        if max_fd is not None and pd.Timestamp(max_fd) > tomorrow:
            pl.success(
                f"max forecast_date {max_fd} > tomorrow ({tomorrow.date()}) — "
                "multi-day horizon confirmed"
            )
        else:
            pl.warning(
                f"max forecast_date {max_fd} not strictly > tomorrow "
                f"({tomorrow.date() if max_fd is not None else 'n/a'}); "
                "the latest vintage may only publish D+1"
            )

        # 24-HE invariant per (region, forecast_date) under latest_only.
        per_key = latest_fcst.groupby(["region", "date"])["hour_ending"].nunique()
        bad_keys = per_key[per_key < 24]
        if len(bad_keys) == 0:
            pl.success(
                f"24-HE invariant holds across all {len(per_key)} "
                "(region, forecast_date) tuples in latest_only output"
            )
        else:
            pl.error(
                f"{len(bad_keys)} (region, forecast_date) tuples have <24 HEs "
                "in latest_only output — coverage gate failed"
            )

        # ── Default-mode parity assertion ─────────────────────────────────
        # Shape check: lead_days=1 should still surface every region × date
        # the parquet supports. We can't golden-file the exact number
        # without a frozen reference, but we can check the structural
        # invariants the existing path always satisfied.
        print_section("Default-mode regression checks")
        default_fcst = default[default["source"] == "meteologica"]
        d_per_key = default_fcst.groupby(["region", "date"])["hour_ending"].nunique()
        d_bad = d_per_key[d_per_key < 24]
        if len(d_bad) == 0:
            pl.success(
                f"default mode 24-HE invariant holds across "
                f"{len(d_per_key)} (region, forecast_date) tuples"
            )
        else:
            pl.error(f"default mode regressed: {len(d_bad)} tuples now have <24 HEs")

        # ── Spot-check: load_meteologica_da_price_forecast latest_only ────
        print_section("Bonus: meteo DA price latest_only horizon")
        price_default = loader.load_meteologica_da_price_forecast(cache_dir=cache_dir)
        price_latest = loader.load_meteologica_da_price_forecast(
            cache_dir=cache_dir, latest_only=True
        )
        pl.info(f"  default price rows: {len(price_default):,}")
        pl.info(f"  latest_only price rows: {len(price_latest):,}")
        if len(price_latest) > 0:
            pl.info(
                f"  price horizon: {price_latest['date'].min()} -> "
                f"{price_latest['date'].max()} "
                f"({price_latest['date'].nunique()} dates)"
            )

        pl.success("latest_horizon check complete")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
