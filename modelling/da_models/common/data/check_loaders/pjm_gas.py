"""Print the ICE next-day gas loader as a wide table, one section per hub.

Mirrors ``check_loaders/pjm_load.py`` for gas. Unlike load/wind/solar, gas
is a single-source series — ICE next-day pricing only, no forecast vs RT
distinction and no coalesce. One row per (Date, Hub) with OnPeak / OffPeak /
Flat summaries and HE1..HE24, in $/MMBtu.

Most days have intra-day price variation (cycles within the gas trading
day), so the hourly granularity is meaningful — not just an upsampled daily
value.

Prints one section per hub in ``HUBS`` order (M3 first, then TCO, TZ6,
Dom South — the loader's column order).

Usage::

    python -m da_models.common.data.check_loaders.pjm_gas
    python modelling/da_models/common/data/check_loaders/pjm_gas.py
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
# Hub keys map to the loader's column names. Labels are for printed headers.
HUBS: tuple[tuple[str, str], ...] = (
    ("gas_m3", "Tetco M3"),
    ("gas_tco", "Columbia TCO"),
    ("gas_tz6", "Transco Z6 NY"),
    ("gas_dom_south", "Dominion South"),
)
CACHE_DIR: Path | None = None
LOOKBACK_DAYS: int | None = 60  # set to None to print all dates
LOG_DIR: Path = _MODELLING_ROOT / "logs"

HE_COLS: list[str] = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HE_COLS: list[str] = [f"HE{h}" for h in range(8, 24)]
OFFPEAK_HE_COLS: list[str] = [c for c in HE_COLS if c not in ONPEAK_HE_COLS]
ORDERED_COLS: list[str] = [
    "Date", "OnPeak", "OffPeak", "Flat", *HE_COLS,
]

_NUMERIC_COLS: list[str] = ["OnPeak", "OffPeak", "Flat", *HE_COLS]
_FORMATTERS: dict = {
    col: (lambda v: "" if pd.isna(v) else f"{v:>8,.3f}")
    for col in _NUMERIC_COLS
}


def _pjm_gas_wide_for_hub(
    gas: pd.DataFrame, hub_col: str,
) -> pd.DataFrame:
    """Pivot the gas frame to wide for a single hub.

    Caller is responsible for any lookback windowing on ``gas``.
    """
    if gas.empty or hub_col not in gas.columns:
        return pd.DataFrame(columns=ORDERED_COLS)

    pivot = (
        gas.pivot_table(
            index="date",
            columns="hour_ending",
            values=hub_col,
            aggfunc="mean",
        )
        .reindex(columns=range(1, 25))
    )
    pivot.columns = [f"HE{h}" for h in pivot.columns]
    pivot["OnPeak"] = pivot[ONPEAK_HE_COLS].mean(axis=1)
    pivot["OffPeak"] = pivot[OFFPEAK_HE_COLS].mean(axis=1)
    pivot["Flat"] = pivot[HE_COLS].mean(axis=1)
    pivot = pivot.reset_index().rename(columns={"date": "Date"})

    return (
        pivot[ORDERED_COLS]
        .sort_values("Date", ascending=False)
        .reset_index(drop=True)
    )


def build_pjm_gas_table(
    hub_col: str = HUBS[0][0],
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide ICE next-day gas table for ``hub_col``, sorted Date desc.

    ``lookback_days`` trims to the N most recent dates (inclusive of the
    latest date in the data). ``None`` returns every date.

    Columns: Date | OnPeak | OffPeak | Flat | HE1..HE24 (in $/MMBtu).
    """
    gas = loader.load_gas_prices_hourly(cache_dir=cache_dir)
    if lookback_days is not None and not gas.empty:
        cutoff = gas["date"].max() - timedelta(days=lookback_days - 1)
        gas = gas[gas["date"] >= cutoff]
    return _pjm_gas_wide_for_hub(gas, hub_col)


def _print_pjm_gas_hub_block(
    pl,
    gas: pd.DataFrame,
    hub_col: str,
    hub_label: str,
) -> None:
    """Print one hub's gas section: header, metadata, table."""
    print_section(f"{hub_label} ({hub_col})")

    table = _pjm_gas_wide_for_hub(gas, hub_col)
    if table.empty:
        pl.warning(f"No gas data for hub={hub_col}.")
        return

    date_min = table["Date"].min()
    date_max = table["Date"].max()
    flat_min = table["Flat"].min()
    flat_max = table["Flat"].max()
    pl.info(f"{hub_label}: rows={len(table):,} | date range: {date_min} -> {date_max}")
    pl.info(
        f"{hub_label}: Flat range: ${flat_min:,.3f} -> ${flat_max:,.3f} /MMBtu"
    )

    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", None,
    ):
        print(table.to_string(index=False, formatters=_FORMATTERS))


def run(
    hubs: tuple[tuple[str, str], ...] = HUBS,
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="check_loaders_pjm_gas", log_dir=LOG_DIR)
    try:
        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(f"load_gas_prices_hourly ({lookback_label})")

        with pl.timer("load ICE next-day gas (all hubs)"):
            gas = loader.load_gas_prices_hourly(cache_dir=cache_dir)

        if gas.empty:
            pl.warning("Gas frame is empty; nothing to print.")
            return

        if lookback_days is not None:
            cutoff = gas["date"].max() - timedelta(days=lookback_days - 1)
            gas = gas[gas["date"] >= cutoff]

        for hub_col, hub_label in hubs:
            _print_pjm_gas_hub_block(pl, gas, hub_col, hub_label)

        pl.success(
            f"Printed {len(hubs)} hub(s): "
            + ", ".join(label for _, label in hubs)
            + "."
        )
    finally:
        pl.close()


if __name__ == "__main__":
    run()
