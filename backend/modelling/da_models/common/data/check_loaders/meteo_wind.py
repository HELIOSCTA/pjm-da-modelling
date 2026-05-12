"""Print the coalesced Meteologica wind loader as a wide table.

Mirror of ``check_loaders/pjm_wind.py`` but for the Meteologica forecast
+ PJM RT-fallback signal (``loader.load_meteologica_wind_coalesced``).
Use this to inspect Meteologica's coverage and vintage shape alongside
the PJM-native version before wiring it into the model as an alt source.

Source column flags ``meteologica`` (DA-cutoff vintage with all 24 HEs
present) or ``rt`` (PJM RT actuals filling gaps and pre-Meteologica
history). As of Date is reconstructed from ``date - 1`` for Meteologica
rows; RT rows carry NaT.

Prints one section per region in ``REGIONS`` order (RTO first, then the
sub-zones).

Usage::

    python -m backend.modelling.da_models.common.data.check_loaders.meteo_wind
    python modelling/da_models/common/data/check_loaders/meteo_wind.py
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[6]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.utils.logging_utils import init_logging, print_header, print_section  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ────────────────────────
REGIONS: tuple[str, ...] = ("RTO", "MIDATL", "WEST", "SOUTH")
CACHE_DIR: Path | None = None
LOOKBACK_DAYS: int | None = 60  # set to None to print all dates
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"

HE_COLS: list[str] = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HE_COLS: list[str] = [f"HE{h}" for h in range(8, 24)]
OFFPEAK_HE_COLS: list[str] = [c for c in HE_COLS if c not in ONPEAK_HE_COLS]
ORDERED_COLS: list[str] = [
    "Source",
    "Forecast Executed",
    "As of Date",
    "Date",
    "Region",
    "OnPeak",
    "OffPeak",
    "Flat",
    *HE_COLS,
]

_NUMERIC_COLS: list[str] = ["OnPeak", "OffPeak", "Flat", *HE_COLS]
_FORMATTERS: dict = {
    col: (lambda v: "" if pd.isna(v) else f"{v:>10,.0f}") for col in _NUMERIC_COLS
}
_FORMATTERS["As of Date"] = lambda v: "" if pd.isna(v) else str(v)
_FORMATTERS["Forecast Executed"] = lambda v: (
    "" if pd.isna(v) else pd.Timestamp(v).strftime("%Y-%m-%d %H:%M")
)


def _meteo_wind_wide_for_region(
    coalesced: pd.DataFrame,
    region: str,
) -> pd.DataFrame:
    """Pivot the coalesced Meteologica wind frame to wide for a single region.

    Caller is responsible for any lookback windowing on ``coalesced``.
    """
    df = coalesced[coalesced["region"].astype(str) == region]
    if df.empty:
        return pd.DataFrame(columns=ORDERED_COLS)

    pivot = df.pivot_table(
        index=["date", "region", "source"],
        columns="hour_ending",
        values="wind_mw",
        aggfunc="mean",
    ).reindex(columns=range(1, 25))
    pivot.columns = [f"HE{h}" for h in pivot.columns]
    pivot["OnPeak"] = pivot[ONPEAK_HE_COLS].mean(axis=1)
    pivot["OffPeak"] = pivot[OFFPEAK_HE_COLS].mean(axis=1)
    pivot["Flat"] = pivot[HE_COLS].mean(axis=1)
    pivot = pivot.reset_index()

    fc_dt = df[
        ["date", "region", "source", "forecast_execution_datetime_local"]
    ].drop_duplicates(subset=["date", "region", "source"], keep="first")
    pivot = pivot.merge(fc_dt, on=["date", "region", "source"], how="left")

    date_ts = pd.to_datetime(pivot["date"])
    pivot["As of Date"] = date_ts - pd.Timedelta(days=1)
    pivot.loc[pivot["source"] != "meteologica", "As of Date"] = pd.NaT
    pivot["As of Date"] = pivot["As of Date"].dt.date

    pivot = pivot.rename(
        columns={
            "date": "Date",
            "region": "Region",
            "source": "Source",
            "forecast_execution_datetime_local": "Forecast Executed",
        }
    )
    pivot["Source"] = pivot["Source"].map({"meteologica": "Meteologica", "rt": "RT"})

    return (
        pivot[ORDERED_COLS].sort_values("Date", ascending=False).reset_index(drop=True)
    )


def build_meteo_wind_table(
    region: str = REGIONS[0],
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide Meteologica wind table for ``region``, sorted Date desc.

    ``lookback_days`` trims the frame to the N most recent dates (inclusive of
    the latest date in the data). ``None`` returns every date.

    Columns: Source | As of Date | Date | Region | OnPeak | OffPeak | Flat | HE1..HE24.
    """
    coalesced = loader.load_meteologica_wind_coalesced(cache_dir=cache_dir)
    if lookback_days is not None and not coalesced.empty:
        cutoff = coalesced["date"].max() - timedelta(days=lookback_days - 1)
        coalesced = coalesced[coalesced["date"] >= cutoff]
    return _meteo_wind_wide_for_region(coalesced, region)


def _print_meteo_wind_region_block(
    pl,
    coalesced: pd.DataFrame,
    region: str,
    lookback_days: int | None,
) -> None:
    """Print one region's Meteologica wind section: header, metadata, RT-fallback warning, table."""
    print_section(f"{region} wind")

    table = _meteo_wind_wide_for_region(coalesced, region)
    if table.empty:
        pl.warning(f"No wind data for region={region}.")
        return

    source_counts = table["Source"].value_counts().to_dict()
    date_min = table["Date"].min()
    date_max = table["Date"].max()
    pl.info(f"{region}: rows={len(table):,} | date range: {date_min} -> {date_max}")
    pl.info(
        f"{region}: source mix: "
        + ", ".join(f"{k}={v:,}" for k, v in source_counts.items())
    )

    rt_dates = table.loc[table["Source"] == "RT", "Date"].tolist()
    if rt_dates and lookback_days is not None:
        pl.warning(
            f"{region}: {len(rt_dates)} RT-fallback rows in window "
            f"(Meteologica missing or partial): "
            + ", ".join(str(d) for d in rt_dates[:10])
            + (" ..." if len(rt_dates) > 10 else "")
        )

    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        None,
    ):
        print(table.to_string(index=False, formatters=_FORMATTERS))


def _print_forward_horizon(
    pl,
    cache_dir: Path | None,
    regions: tuple[str, ...],
) -> None:
    """Print the forward multi-day horizon from the latest publish."""
    print_header("Forward horizon (latest publish, all regions)")

    with pl.timer("load coalesced Meteologica wind (latest_only=True)"):
        latest = loader.load_meteologica_wind_coalesced(
            cache_dir=cache_dir, latest_only=True
        )

    if latest.empty:
        pl.warning("latest_only frame is empty; no forward horizon to print.")
        return

    fcst = latest[latest["source"] == "meteologica"]
    if fcst.empty:
        pl.warning("No forecast rows in latest_only frame.")
        return

    pl.info(
        f"As of {fcst['as_of_date'].max()}: "
        f"{fcst['date'].nunique()} forecast_date(s) "
        f"({fcst['date'].min()} -> {fcst['date'].max()})"
    )

    for region in regions:
        print_section(f"{region} wind — forward horizon")
        table = _meteo_wind_wide_for_region(latest, region)
        table = table[table["Source"] == "Meteologica"]
        if table.empty:
            pl.warning(f"No forward-horizon rows for region={region}.")
            continue
        table = table.sort_values("Date", ascending=True).reset_index(drop=True)
        with pd.option_context(
            "display.max_rows",
            None,
            "display.max_columns",
            None,
            "display.width",
            None,
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

    pl = init_logging(name="check_loaders_meteo_wind", log_dir=LOG_DIR)
    try:
        _print_forward_horizon(pl, cache_dir, regions)

        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(f"Historical realization ({lookback_label})")

        with pl.timer("load coalesced Meteologica wind (lead_days=1)"):
            coalesced = loader.load_meteologica_wind_coalesced(cache_dir=cache_dir)

        if coalesced.empty:
            pl.warning("Coalesced Meteologica wind frame is empty; nothing to print.")
            return

        if lookback_days is not None:
            cutoff = coalesced["date"].max() - timedelta(days=lookback_days - 1)
            coalesced = coalesced[coalesced["date"] >= cutoff]

        for region in regions:
            _print_meteo_wind_region_block(pl, coalesced, region, lookback_days)

        pl.success(f"Printed {len(regions)} region(s): {', '.join(regions)}.")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
