"""Print the PJM DA LMP (total) loader as a wide table.

One row per Date for the configured hub, with OnPeak / OffPeak / Flat
summaries and HE1..HE24, in $/MWh. ``lmp_total`` is the full DA LMP —
energy + congestion + losses — at the hub.

Mirrors ``check_loaders/ice_python_gas.py`` but for DA LMPs. Single-source
actuals (DA market), so no forecast / RT distinction and no coalesce.

Scoped to WESTERN HUB by default (the canonical hub for the
``like_day_model_knn`` model family).

Usage::

    python -m backend.modelling.da_models.common.data.check_loaders.pjm_lmp_total
    python modelling/da_models/common/data/check_loaders/pjm_lmp_total.py
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


def _pjm_lmp_total_wide_for_hub(
    lmps: pd.DataFrame,
    hub: str,
) -> pd.DataFrame:
    """Pivot the DA LMP frame to wide for a single hub.

    Caller is responsible for any lookback windowing on ``lmps``.
    """
    df = lmps[lmps["region"].astype(str) == hub]
    if df.empty:
        return pd.DataFrame(columns=ORDERED_COLS)

    pivot = df.pivot_table(
        index="date",
        columns="hour_ending",
        values="lmp",
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


def build_pjm_lmp_total_table(
    hub: str = HUBS[0],
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide PJM DA LMP (total) table for ``hub``, sorted Date desc.

    ``lookback_days`` trims to the N most recent dates (inclusive of the
    latest date in the data). ``None`` returns every date.

    Columns: Date | OnPeak | OffPeak | Flat | HE1..HE24 (in $/MWh).
    """
    lmps = loader.load_lmps_da(cache_dir=cache_dir)
    if lookback_days is not None and not lmps.empty:
        cutoff = lmps["date"].max() - timedelta(days=lookback_days - 1)
        lmps = lmps[lmps["date"] >= cutoff]
    return _pjm_lmp_total_wide_for_hub(lmps, hub)


def _print_pjm_lmp_total_hub_block(
    pl,
    lmps: pd.DataFrame,
    hub: str,
) -> None:
    """Print one hub's DA LMP section: header, metadata, table."""
    print_section(f"{hub} (lmp_total)")

    table = _pjm_lmp_total_wide_for_hub(lmps, hub)
    if table.empty:
        pl.warning(f"No DA LMP data for hub={hub}.")
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

    pl = init_logging(name="check_loaders_pjm_lmp_total", log_dir=LOG_DIR)
    try:
        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(f"load_lmps_da -- lmp_total ({lookback_label})")

        with pl.timer("load PJM DA LMPs (all hubs)"):
            lmps = loader.load_lmps_da(cache_dir=cache_dir)

        if lmps.empty:
            pl.warning("DA LMP frame is empty; nothing to print.")
            return

        if lookback_days is not None:
            cutoff = lmps["date"].max() - timedelta(days=lookback_days - 1)
            lmps = lmps[lmps["date"] >= cutoff]

        for hub in hubs:
            _print_pjm_lmp_total_hub_block(pl, lmps, hub)

        pl.success(f"Printed {len(hubs)} hub(s): {', '.join(hubs)}.")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
