"""Print PJM outages loaders (actual + DA-decision-time forecast) side-by-side.

Outages are daily-grain, system-region (no hour dimension), so the wide
table is one row per (Date, Region, Source) with four MW columns plus a
forced-share, rather than HE1..HE24.

This is a deliberate side-by-side view rather than a strict coalesce:
- ``loader.load_outages_actual`` covers ~2020-01-01 → yesterday (same-day
  publication, lead=0).
- ``loader.load_outages_forecast_history(lead_days=1)`` returns the
  DA-decision-time vintage — published the morning before the operating
  day (``as_of_date == forecast_date - 1``). The new history mart bakes
  ``lead_days`` directly, so we filter on it rather than recomputing
  date deltas.

The two signals overlap on every settled date with a published forecast,
and on overlap days having both rows lets you eyeball forecast accuracy.

Source flags ``Actual`` (As of Date = NaT) or ``Forecast`` (As of Date =
``as_of_date``, which equals forecast_date - 1 by construction).

Prints one section per region in ``REGIONS`` order.

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
    "Source", "As of Date", "Date", "Region", *OUTAGE_VALUE_COLS,
]

_MW_COLS: list[str] = ["Total MW", "Planned MW", "Maintenance MW", "Forced MW"]
_FORMATTERS: dict = {
    col: (lambda v: "" if pd.isna(v) else f"{v:>10,.0f}") for col in _MW_COLS
}
_FORMATTERS["Forced %"] = lambda v: "" if pd.isna(v) else f"{v:>7.1%}"
_FORMATTERS["As of Date"] = lambda v: "" if pd.isna(v) else str(v)


def _normalize_outage_frame(
    df: pd.DataFrame,
    *,
    source_label: str,
    as_of_col: str | None,
) -> pd.DataFrame:
    """Project an outage frame to the canonical wide shape.

    Renames ``total/planned/maintenance/forced_outages_mw`` to display columns,
    sets ``Source`` to ``source_label``, and computes ``Forced %``. Pulls
    ``As of Date`` from ``as_of_col`` when provided, NaT otherwise.
    """
    if df.empty:
        return pd.DataFrame(columns=ORDERED_COLS)
    # The history loader emits ``forecast_date``; the actuals loader emits ``date``.
    df = df.copy()
    if "date" not in df.columns and "forecast_date" in df.columns:
        df["date"] = df["forecast_date"]
    out = df.rename(
        columns={
            "date": "Date",
            "region": "Region",
            "total_outages_mw": "Total MW",
            "planned_outages_mw": "Planned MW",
            "maintenance_outages_mw": "Maintenance MW",
            "forced_outages_mw": "Forced MW",
        }
    ).copy()
    out["Source"] = source_label
    if as_of_col is not None and as_of_col in out.columns:
        out["As of Date"] = pd.to_datetime(out[as_of_col], errors="coerce").dt.date
    else:
        out["As of Date"] = pd.NaT
    total = out["Total MW"]
    forced = out["Forced MW"]
    with np.errstate(invalid="ignore", divide="ignore"):
        out["Forced %"] = forced / total.where(total > 0)
    return out[ORDERED_COLS]


def _outages_wide_for_region(
    actuals: pd.DataFrame,
    forecast_da: pd.DataFrame,
    region: str,
) -> pd.DataFrame:
    """Combined Actual + DA-cutoff Forecast wide table for one region.

    Caller is responsible for any lookback windowing on the input frames.
    Sorted Date desc; same-date rows order Actual before Forecast.
    """
    a_region = actuals[actuals["region"].astype(str) == region]
    f_region = forecast_da[forecast_da["region"].astype(str) == region]

    actual_wide = _normalize_outage_frame(
        a_region, source_label="Actual", as_of_col=None,
    )
    forecast_wide = _normalize_outage_frame(
        f_region, source_label="Forecast", as_of_col="forecast_execution_date",
    )

    combined = pd.concat([actual_wide, forecast_wide], ignore_index=True)
    if combined.empty:
        return combined
    return (
        combined.sort_values(["Date", "Source"], ascending=[False, True])
        .reset_index(drop=True)
    )


def _trim_lookback(
    actuals: pd.DataFrame,
    forecast: pd.DataFrame,
    lookback_days: int | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Trim both frames to the N most recent dates (using max date across both).

    ``actuals`` is keyed on ``date``; ``forecast`` (history loader) on
    ``forecast_date``.
    """
    if lookback_days is None:
        return actuals, forecast
    a_dt = actuals["date"] if "date" in actuals.columns else None
    f_dt = forecast["forecast_date"] if "forecast_date" in forecast.columns else (
        forecast["date"] if "date" in forecast.columns else None
    )
    latest_dates = []
    if a_dt is not None and not actuals.empty:
        latest_dates.append(a_dt.max())
    if f_dt is not None and not forecast.empty:
        latest_dates.append(f_dt.max())
    if not latest_dates:
        return actuals, forecast
    cutoff = max(latest_dates) - timedelta(days=lookback_days - 1)
    a_out = actuals[actuals["date"] >= cutoff] if a_dt is not None else actuals
    if "forecast_date" in forecast.columns:
        f_out = forecast[forecast["forecast_date"] >= cutoff]
    elif "date" in forecast.columns:
        f_out = forecast[forecast["date"] >= cutoff]
    else:
        f_out = forecast
    return a_out, f_out


def build_pjm_outages_table(
    region: str = REGIONS[0],
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide outages table for ``region``, sorted Date desc.

    ``lookback_days`` trims both frames to the N most recent dates (inclusive
    of the latest date across actual + forecast). ``None`` returns every date.

    Columns: Source | As of Date | Date | Region | Total MW | Planned MW |
    Maintenance MW | Forced MW | Forced %.
    """
    actuals = loader.load_outages_actual(cache_dir=cache_dir)
    forecast_da = loader.load_outages_forecast_history(
        cache_dir=cache_dir, lead_days=1,
    )
    actuals, forecast_da = _trim_lookback(actuals, forecast_da, lookback_days)
    return _outages_wide_for_region(actuals, forecast_da, region)


def _print_pjm_outages_region_block(
    pl,
    actuals: pd.DataFrame,
    forecast_da: pd.DataFrame,
    region: str,
    lookback_days: int | None,
) -> None:
    """Print one region's outages section: header, metadata, table."""
    print_section(f"{region} outages")

    table = _outages_wide_for_region(actuals, forecast_da, region)
    if table.empty:
        pl.warning(f"No outage data for region={region}.")
        return

    source_counts = table["Source"].value_counts().to_dict()
    date_min = table["Date"].min()
    date_max = table["Date"].max()
    pl.info(f"{region}: rows={len(table):,} | date range: {date_min} -> {date_max}")
    pl.info(
        f"{region}: source mix: "
        + ", ".join(f"{k}={v:,}" for k, v in source_counts.items())
    )

    actual_dates = table.loc[table["Source"] == "Actual", "Date"]
    if not actual_dates.empty:
        pl.info(f"{region}: latest actual date: {actual_dates.max()}")

    forecast_dates = table.loc[table["Source"] == "Forecast", "Date"]
    if not forecast_dates.empty:
        n_fc = len(forecast_dates)
        pl.info(
            f"{region}: forecast horizon: {forecast_dates.min()} -> "
            f"{forecast_dates.max()} ({n_fc} day{'s' if n_fc != 1 else ''})"
        )
    elif lookback_days is not None:
        pl.warning(
            f"{region}: no DA-decision-time forecast rows in window — "
            "expected lead_days == 1 vintage missing."
        )

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
            "load_outages_actual + load_outages_forecast_history "
            f"(lead_days=1, {lookback_label})"
        )

        with pl.timer("load outages_actual + outages_forecast_history (all regions)"):
            actuals = loader.load_outages_actual(cache_dir=cache_dir)
            forecast_da = loader.load_outages_forecast_history(
                cache_dir=cache_dir, lead_days=1,
            )

        if actuals.empty and forecast_da.empty:
            pl.warning("Both outage frames are empty; nothing to print.")
            return

        actuals, forecast_da = _trim_lookback(actuals, forecast_da, lookback_days)

        for region in regions:
            _print_pjm_outages_region_block(
                pl, actuals, forecast_da, region, lookback_days,
            )

        pl.success(f"Printed {len(regions)} region(s): {', '.join(regions)}.")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
