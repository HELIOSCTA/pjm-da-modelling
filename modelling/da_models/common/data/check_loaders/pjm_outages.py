"""Print the PJM outages DA-decision-time forecast (lead=1) per region.

Outages are daily-grain, system-region (no hour dimension), so the wide
table is one row per (Date, Region) with four MW columns plus a
forced-share, rather than HE1..HE24.

Source data is the multi-vintage ``pjm_outages_forecast_history`` mart,
which goes back to 2020 and carries all 8 daily-publication vintages
(``lead_days`` 0..7) per (region, forecast_date). PJM publishes the
seven-day outage forecast once per morning. This script reads with
``lead_days=1`` — the DA-decision-time vintage published the morning
before the operating day (``as_of_date == forecast_date - 1``), i.e.
what was knowable when the DA market cleared. That's the signal the
like-day KNN model consumes for both pool and query, so the script
mirrors the model's view of the data.

Earlier-lead vintages (lead=0 same-day publication, lead=2..7 earlier
publications) are deliberately skipped — comparing them is "forecast
evolution across publication days," which is a separate inspection
question and belongs in a different script.

One section per region in ``REGIONS`` order.

Usage::

    python -m da_models.common.data.check_loaders.pjm_outages
    python modelling/da_models/common/data/check_loaders/pjm_outages.py
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.common.data import loader  # noqa: E402
from utils.logging_utils import init_logging, print_header, print_section  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ────────────────────────
REGIONS: tuple[str, ...] = ("RTO", "MIDATL_DOM", "WEST")
CACHE_DIR: Path | None = None
LOOKBACK_DAYS: int | None = 60  # set to None to print all dates
LOG_DIR: Path = _MODELLING_ROOT / "logs"

OUTAGE_VALUE_COLS: list[str] = [
    "Total MW", "Planned MW", "Maintenance MW", "Forced MW", "Forced %",
]
ORDERED_COLS: list[str] = [
    "As of Date", "Date", *OUTAGE_VALUE_COLS,
]

_MW_COLS: list[str] = ["Total MW", "Planned MW", "Maintenance MW", "Forced MW"]
_FORMATTERS: dict = {
    col: (lambda v: "" if pd.isna(v) else f"{v:>10,.0f}") for col in _MW_COLS
}
_FORMATTERS["Forced %"] = lambda v: "" if pd.isna(v) else f"{v:>7.1%}"
_FORMATTERS["As of Date"] = lambda v: "" if pd.isna(v) else str(v)


def _outages_wide_for_region(
    forecast_lead1: pd.DataFrame,
    region: str,
) -> pd.DataFrame:
    """Project the lead=1 history slice to the canonical wide shape for one region.

    Caller is responsible for any lookback windowing.
    Sorted Date desc.
    """
    df = forecast_lead1[forecast_lead1["region"].astype(str) == region]
    if df.empty:
        return pd.DataFrame(columns=ORDERED_COLS)

    out = df.rename(
        columns={
            "forecast_date": "Date",
            "total_outages_mw": "Total MW",
            "planned_outages_mw": "Planned MW",
            "maintenance_outages_mw": "Maintenance MW",
            "forced_outages_mw": "Forced MW",
        }
    ).copy()
    out["As of Date"] = pd.to_datetime(out["as_of_date"], errors="coerce").dt.date
    total = out["Total MW"]
    forced = out["Forced MW"]
    with np.errstate(invalid="ignore", divide="ignore"):
        out["Forced %"] = forced / total.where(total > 0)
    return (
        out[ORDERED_COLS]
        .sort_values("Date", ascending=False)
        .reset_index(drop=True)
    )


def _trim_lookback(
    forecast_lead1: pd.DataFrame,
    lookback_days: int | None,
) -> pd.DataFrame:
    """Trim the lead=1 frame to the N most recent target dates."""
    if lookback_days is None or forecast_lead1.empty:
        return forecast_lead1
    cutoff = forecast_lead1["forecast_date"].max() - timedelta(days=lookback_days - 1)
    return forecast_lead1[forecast_lead1["forecast_date"] >= cutoff]


def build_pjm_outages_table(
    region: str = REGIONS[0],
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide outages table for ``region``, sorted Date desc.

    ``lookback_days`` trims to the N most recent target dates. ``None``
    returns every date.

    Columns: As of Date | Date | Total MW | Planned MW |
    Maintenance MW | Forced MW | Forced %.
    """
    forecast_lead1 = loader.load_outages_forecast_history(
        cache_dir=cache_dir, lead_days=1,
    )
    forecast_lead1 = _trim_lookback(forecast_lead1, lookback_days)
    return _outages_wide_for_region(forecast_lead1, region)


def _print_pjm_outages_region_block(
    pl,
    forecast_lead1: pd.DataFrame,
    region: str,
) -> None:
    """Print one region's outages section: header, metadata, table."""
    print_section(f"{region} outages")

    table = _outages_wide_for_region(forecast_lead1, region)
    if table.empty:
        pl.warning(f"No outage data for region={region}.")
        return

    date_min = table["Date"].min()
    date_max = table["Date"].max()
    pl.info(f"{region}: rows={len(table):,} | target date range: {date_min} -> {date_max}")

    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", None,
    ):
        print(table.to_string(index=False, formatters=_FORMATTERS))


def run(
    regions: tuple[str, ...] = REGIONS,
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="check_loaders_pjm_outages", log_dir=LOG_DIR)
    try:
        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(
            f"load_outages_forecast_history (lead=1, {lookback_label})"
        )

        with pl.timer("load outages_forecast_history (lead=1, all regions)"):
            forecast_lead1 = loader.load_outages_forecast_history(
                cache_dir=cache_dir, lead_days=1,
            )

        if forecast_lead1.empty:
            pl.warning("Outages lead=1 frame is empty; nothing to print.")
            return

        forecast_lead1 = _trim_lookback(forecast_lead1, lookback_days)

        for region in regions:
            _print_pjm_outages_region_block(pl, forecast_lead1, region)

        pl.success(f"Printed {len(regions)} region(s): {', '.join(regions)}.")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
