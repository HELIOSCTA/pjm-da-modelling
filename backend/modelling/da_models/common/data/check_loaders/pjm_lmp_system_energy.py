"""Print the PJM DA System Energy Price loader as a wide table.

One row per Date for the configured hub, with OnPeak / OffPeak / Flat
summaries and HE1..HE24, in $/MWh. ``lmp_system_energy_price`` (SEP) is
the system-wide energy component of LMP — it isolates the
fundamentals-driven piece of price from congestion and losses.

SEP is uniform across all nodes within a given hour, so the parquet
stores the same value for every hub. We filter to one hub
(``WESTERN HUB`` by default) to get a clean one-row-per-(date, HE) view;
selecting a different hub would yield an identical series.

Mirrors ``check_loaders/pjm_lmp_total.py``. Single-source actuals (DA
market), no forecast / RT distinction.

Usage::

    python -m backend.modelling.da_models.common.data.check_loaders.pjm_lmp_system_energy
    python modelling/da_models/common/data/check_loaders/pjm_lmp_system_energy.py
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
HUBS: tuple[str, ...] = ("WESTERN HUB",)
CACHE_DIR: Path | None = None
LOOKBACK_DAYS: int | None = 60  # set to None to print all dates
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"

HE_COLS: list[str] = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HE_COLS: list[str] = [f"HE{h}" for h in range(8, 24)]
OFFPEAK_HE_COLS: list[str] = [c for c in HE_COLS if c not in ONPEAK_HE_COLS]
ORDERED_COLS: list[str] = [
    "Date",
    "OnPeak",
    "OffPeak",
    "Flat",
    *HE_COLS,
]

_NUMERIC_COLS: list[str] = ["OnPeak", "OffPeak", "Flat", *HE_COLS]
_FORMATTERS: dict = {
    col: (lambda v: "" if pd.isna(v) else f"{v:>+8,.2f}") for col in _NUMERIC_COLS
}


def _pjm_lmp_sep_wide_for_hub(
    sep: pd.DataFrame,
    hub: str,
) -> pd.DataFrame:
    """Pivot the DA SEP frame to wide for a single hub.

    Caller is responsible for any lookback windowing on ``sep``.
    """
    df = sep[sep["region"].astype(str) == hub]
    if df.empty:
        return pd.DataFrame(columns=ORDERED_COLS)

    pivot = df.pivot_table(
        index="date",
        columns="hour_ending",
        values="lmp_system_energy_price",
        aggfunc="mean",
    ).reindex(columns=range(1, 25))
    pivot.columns = [f"HE{h}" for h in pivot.columns]
    pivot["OnPeak"] = pivot[ONPEAK_HE_COLS].mean(axis=1)
    pivot["OffPeak"] = pivot[OFFPEAK_HE_COLS].mean(axis=1)
    pivot["Flat"] = pivot[HE_COLS].mean(axis=1)
    pivot = pivot.reset_index().rename(columns={"date": "Date"})

    return (
        pivot[ORDERED_COLS].sort_values("Date", ascending=False).reset_index(drop=True)
    )


def build_pjm_lmp_system_energy_table(
    hub: str = HUBS[0],
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide PJM DA SEP table for ``hub``, sorted Date desc.

    ``lookback_days`` trims to the N most recent dates (inclusive of the
    latest date in the data). ``None`` returns every date.

    Columns: Date | OnPeak | OffPeak | Flat | HE1..HE24 (in $/MWh).
    """
    sep = loader.load_lmp_system_energy_da(cache_dir=cache_dir)
    if lookback_days is not None and not sep.empty:
        cutoff = sep["date"].max() - timedelta(days=lookback_days - 1)
        sep = sep[sep["date"] >= cutoff]
    return _pjm_lmp_sep_wide_for_hub(sep, hub)


def _print_pjm_lmp_sep_hub_block(
    pl,
    sep: pd.DataFrame,
    hub: str,
) -> None:
    """Print one hub's DA SEP section: header, metadata, table."""
    print_section(f"{hub} (lmp_system_energy_price)")

    table = _pjm_lmp_sep_wide_for_hub(sep, hub)
    if table.empty:
        pl.warning(f"No DA SEP data for hub={hub}.")
        return

    date_min = table["Date"].min()
    date_max = table["Date"].max()
    flat_min = table["Flat"].min()
    flat_max = table["Flat"].max()
    pl.info(f"{hub}: rows={len(table):,} | date range: {date_min} -> {date_max}")
    pl.info(f"{hub}: Flat range: ${flat_min:+,.2f} -> ${flat_max:+,.2f} /MWh")

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
    hubs: tuple[str, ...] = HUBS,
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="check_loaders_pjm_lmp_system_energy", log_dir=LOG_DIR)
    try:
        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(
            f"load_lmp_system_energy_da -- lmp_system_energy_price ({lookback_label})"
        )

        with pl.timer("load PJM DA SEP (all hubs)"):
            sep = loader.load_lmp_system_energy_da(cache_dir=cache_dir)

        if sep.empty:
            pl.warning("DA SEP frame is empty; nothing to print.")
            return

        if lookback_days is not None:
            cutoff = sep["date"].max() - timedelta(days=lookback_days - 1)
            sep = sep[sep["date"] >= cutoff]

        for hub in hubs:
            _print_pjm_lmp_sep_hub_block(pl, sep, hub)

        pl.success(f"Printed {len(hubs)} hub(s): {', '.join(hubs)}.")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
