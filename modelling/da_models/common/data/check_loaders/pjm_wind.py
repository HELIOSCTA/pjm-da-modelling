"""Print the coalesced PJM wind loader as a wide table.

Mirrors ``check_loaders/pjm_load.py`` for wind. PJM-native wind forecast is
system-wide (no region) and actuals come from ``net_load_actual.wind_gen_mw``
filtered to RTO, so the output is RTO-only — one row per Date with Source,
As of Date, OnPeak / OffPeak / Flat summaries, and HE1..HE24.

Source flags whether the row came from the DA-cutoff wind forecast parquet
(preferred where 24-hour coverage exists) or RT actuals (fallback for
pre-backfill dates and partial-coverage days). As of Date is reconstructed
from ``date - 1`` for forecast rows; RT rows carry NaT.

Usage::

    python -m da_models.common.data.check_loaders.pjm_wind
    python modelling/da_models/common/data/check_loaders/pjm_wind.py
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.common.data import loader  # noqa: E402
from utils.logging_utils import init_logging, print_header, print_section  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ────────────────────────
CACHE_DIR: Path | None = None
LOOKBACK_DAYS: int | None = 60  # set to None to print all dates
LOG_DIR: Path = _MODELLING_ROOT / "logs"

HE_COLS: list[str] = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HE_COLS: list[str] = [f"HE{h}" for h in range(8, 24)]
OFFPEAK_HE_COLS: list[str] = [c for c in HE_COLS if c not in ONPEAK_HE_COLS]
ORDERED_COLS: list[str] = [
    "Source", "As of Date", "Date",
    "OnPeak", "OffPeak", "Flat", *HE_COLS,
]

_NUMERIC_COLS: list[str] = ["OnPeak", "OffPeak", "Flat", *HE_COLS]
_FORMATTERS: dict = {
    col: (lambda v: "" if pd.isna(v) else f"{v:>10,.0f}")
    for col in _NUMERIC_COLS
}
_FORMATTERS["As of Date"] = lambda v: "" if pd.isna(v) else str(v)


def _pjm_wind_wide(coalesced: pd.DataFrame) -> pd.DataFrame:
    """Pivot the coalesced PJM wind frame to wide (RTO-only, no region)."""
    if coalesced.empty:
        return pd.DataFrame(columns=ORDERED_COLS)

    pivot = (
        coalesced.pivot_table(
            index=["date", "source"],
            columns="hour_ending",
            values="wind_mw",
            aggfunc="mean",
        )
        .reindex(columns=range(1, 25))
    )
    pivot.columns = [f"HE{h}" for h in pivot.columns]
    pivot["OnPeak"] = pivot[ONPEAK_HE_COLS].mean(axis=1)
    pivot["OffPeak"] = pivot[OFFPEAK_HE_COLS].mean(axis=1)
    pivot["Flat"] = pivot[HE_COLS].mean(axis=1)
    pivot = pivot.reset_index()

    date_ts = pd.to_datetime(pivot["date"])
    pivot["As of Date"] = date_ts - pd.Timedelta(days=1)
    pivot.loc[pivot["source"] != "forecast", "As of Date"] = pd.NaT
    pivot["As of Date"] = pivot["As of Date"].dt.date

    pivot = pivot.rename(columns={"date": "Date", "source": "Source"})
    pivot["Source"] = pivot["Source"].map({"forecast": "Forecast", "rt": "RT"})

    return (
        pivot[ORDERED_COLS]
        .sort_values("Date", ascending=False)
        .reset_index(drop=True)
    )


def build_pjm_wind_table(
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide RTO PJM wind table, sorted Date desc.

    ``lookback_days`` trims to the N most recent dates (inclusive of the
    latest date in the data). ``None`` returns every date.

    Columns: Source | As of Date | Date | OnPeak | OffPeak | Flat | HE1..HE24.
    """
    coalesced = loader.load_wind_coalesced(cache_dir=cache_dir)
    if lookback_days is not None and not coalesced.empty:
        cutoff = coalesced["date"].max() - timedelta(days=lookback_days - 1)
        coalesced = coalesced[coalesced["date"] >= cutoff]
    return _pjm_wind_wide(coalesced)


def run(
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="check_loaders_pjm_wind", log_dir=LOG_DIR)
    try:
        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(f"load_wind_coalesced ({lookback_label})")

        with pl.timer("load coalesced PJM wind (RTO)"):
            coalesced = loader.load_wind_coalesced(cache_dir=cache_dir)

        if coalesced.empty:
            pl.warning("Coalesced wind frame is empty; nothing to print.")
            return

        if lookback_days is not None:
            cutoff = coalesced["date"].max() - timedelta(days=lookback_days - 1)
            coalesced = coalesced[coalesced["date"] >= cutoff]

        print_section("RTO wind")

        table = _pjm_wind_wide(coalesced)
        if table.empty:
            pl.warning("No wind data in window.")
            return

        source_counts = table["Source"].value_counts().to_dict()
        date_min = table["Date"].min()
        date_max = table["Date"].max()
        pl.info(f"RTO: rows={len(table):,} | date range: {date_min} -> {date_max}")
        pl.info(
            "RTO: source mix: "
            + ", ".join(f"{k}={v:,}" for k, v in source_counts.items())
        )

        rt_dates = table.loc[table["Source"] == "RT", "Date"].tolist()
        if rt_dates and lookback_days is not None:
            pl.warning(
                f"RTO: {len(rt_dates)} RT-fallback rows in window "
                f"(forecast missing or partial): "
                + ", ".join(str(d) for d in rt_dates[:10])
                + (" ..." if len(rt_dates) > 10 else "")
            )

        with pd.option_context(
            "display.max_rows", None,
            "display.max_columns", None,
            "display.width", None,
        ):
            print(table.to_string(index=False, formatters=_FORMATTERS))

        pl.success("Printed RTO wind section.")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
