"""Print the coalesced WSI temperature loader as a wide table.

One row per Date with Source, OnPeak / OffPeak / Flat summaries, and
HE1..HE24 temperature values (degF).

Source column flags whether the underlying row came from the WSI
observed parquet (preferred where 24-hour coverage exists) or the
forecast parquet (fallback for future dates and partial-coverage gaps).
Both parquets are RTO-wide so there's no region split.

Usage::

    python -m backend.modelling.da_models.common.data.check_loaders.wsi_temps
    python modelling/da_models/common/data/check_loaders/wsi_temps.py
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
CACHE_DIR: Path | None = None
LOOKBACK_DAYS: int | None = 60  # set to None to print all dates
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"

HE_COLS: list[str] = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HE_COLS: list[str] = [f"HE{h}" for h in range(8, 24)]
OFFPEAK_HE_COLS: list[str] = [c for c in HE_COLS if c not in ONPEAK_HE_COLS]
ORDERED_COLS: list[str] = [
    "Source",
    "Date",
    "OnPeak",
    "OffPeak",
    "Flat",
    *HE_COLS,
]

_NUMERIC_COLS: list[str] = ["OnPeak", "OffPeak", "Flat", *HE_COLS]
_FORMATTERS: dict = {
    col: (lambda v: "" if pd.isna(v) else f"{v:>6.1f}") for col in _NUMERIC_COLS
}


def _wide_temps(coalesced: pd.DataFrame) -> pd.DataFrame:
    """Pivot the coalesced WSI temp frame to wide HE1..HE24 layout."""
    if coalesced is None or coalesced.empty:
        return pd.DataFrame(columns=ORDERED_COLS)

    pivot = coalesced.pivot_table(
        index=["date", "source"],
        columns="hour_ending",
        values="temp",
        aggfunc="mean",
    ).reindex(columns=range(1, 25))
    pivot.columns = [f"HE{h}" for h in pivot.columns]
    pivot["OnPeak"] = pivot[ONPEAK_HE_COLS].mean(axis=1)
    pivot["OffPeak"] = pivot[OFFPEAK_HE_COLS].mean(axis=1)
    pivot["Flat"] = pivot[HE_COLS].mean(axis=1)
    pivot = pivot.reset_index().rename(columns={"date": "Date", "source": "Source"})
    pivot["Source"] = pivot["Source"].map(
        {"observed": "Observed", "forecast": "Forecast"}
    )

    return (
        pivot[ORDERED_COLS].sort_values("Date", ascending=False).reset_index(drop=True)
    )


def build_wsi_temps_table(
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide WSI temperature table, sorted Date desc.

    ``lookback_days`` trims the frame to the N most recent dates. ``None``
    returns every date.

    Columns: Source | Date | OnPeak | OffPeak | Flat | HE1..HE24.
    """
    coalesced = loader.load_weather_coalesced(cache_dir=cache_dir)
    if lookback_days is not None and not coalesced.empty:
        cutoff = coalesced["date"].max() - timedelta(days=lookback_days - 1)
        coalesced = coalesced[coalesced["date"] >= cutoff]
    return _wide_temps(coalesced)


def _print_wsi_temps_block(
    pl,
    coalesced: pd.DataFrame,
    lookback_days: int | None,
) -> None:
    print_section("RTO temps (observed-first coalesced)")

    table = _wide_temps(coalesced)
    if table.empty:
        pl.warning("No weather data; nothing to print.")
        return

    source_counts = table["Source"].value_counts().to_dict()
    date_min = table["Date"].min()
    date_max = table["Date"].max()
    pl.info(f"rows={len(table):,} | date range: {date_min} -> {date_max}")
    pl.info("source mix: " + ", ".join(f"{k}={v:,}" for k, v in source_counts.items()))

    forecast_dates = table.loc[table["Source"] == "Forecast", "Date"].tolist()
    if forecast_dates and lookback_days is not None:
        pl.warning(
            f"{len(forecast_dates)} forecast-fallback row(s) in window "
            "(observed missing or partial): "
            + ", ".join(str(d) for d in forecast_dates[:10])
            + (" ..." if len(forecast_dates) > 10 else "")
        )

    null_he_rows = int(table[HE_COLS].isna().any(axis=1).sum())
    if null_he_rows:
        pl.warning(f"{null_he_rows} row(s) have at least one missing HE value")

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
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="check_loaders_wsi_temps", log_dir=LOG_DIR)
    try:
        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(f"load_weather_coalesced ({lookback_label})")

        with pl.timer("load coalesced WSI temps"):
            coalesced = loader.load_weather_coalesced(cache_dir=cache_dir)

        if coalesced.empty:
            pl.warning("Coalesced weather frame is empty; nothing to print.")
            return

        if lookback_days is not None:
            cutoff = coalesced["date"].max() - timedelta(days=lookback_days - 1)
            coalesced = coalesced[coalesced["date"] >= cutoff]

        _print_wsi_temps_block(pl, coalesced, lookback_days)

        pl.success("Printed WSI temps coalesced view.")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
