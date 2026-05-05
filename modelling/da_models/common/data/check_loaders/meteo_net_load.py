"""Print the unified Meteologica supply-demand bundle as a wide table.

For each (Date, Region) prints six rows: load, solar, wind, net_load
(reported), net_load (implied = load - solar - wind), and delta
(reported - implied). The delta row exists to surface any identity break;
under the unified loader it should be ~0 by construction.

Source flags ``Meteologica`` (DA-cutoff vintage with all 24 HEs present)
or ``RT`` (PJM RT actuals filling gaps and pre-Meteologica history;
Meteologica coverage starts ~2026-02-27). Identity holds within each
(region, date) row group because the unified loader makes a single
forecast-vs-RT decision for all four components simultaneously.

Unlike the PJM-native net-load forecast (RTO only), Meteologica publishes
all four regions (RTO + MIDATL/WEST/SOUTH), so this script prints one
section per region in ``REGIONS`` order.

Consumes ``loader.load_meteologica_supply_demand_coalesced`` — the
identity-safe primitive that exposes load + solar + wind + net_load
together. The Streamlit Data page still uses the older per-series
``load_meteologica_*_coalesced`` functions for display-comparison
purposes.

Usage::

    python -m da_models.common.data.check_loaders.meteo_net_load
    python modelling/da_models/common/data/check_loaders/meteo_net_load.py
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
REGIONS: tuple[str, ...] = ("RTO", "MIDATL", "WEST", "SOUTH")
CACHE_DIR: Path | None = None
# Smaller default than the per-series scripts since each date emits 6 rows
# (4 components + 2 derived). Set to None to print every date.
LOOKBACK_DAYS: int | None = 7
LOG_DIR: Path = _MODELLING_ROOT / "logs"

HE_COLS: list[str] = [f"HE{h}" for h in range(1, 25)]
ONPEAK_HE_COLS: list[str] = [f"HE{h}" for h in range(8, 24)]
OFFPEAK_HE_COLS: list[str] = [c for c in HE_COLS if c not in ONPEAK_HE_COLS]
ORDERED_COLS: list[str] = [
    "Source",
    "Forecast Executed",
    "As of Date",
    "Date",
    "Region",
    "Type",
    "OnPeak",
    "OffPeak",
    "Flat",
    *HE_COLS,
]

_NUMERIC_COLS: list[str] = ["OnPeak", "OffPeak", "Flat", *HE_COLS]
_FORMATTERS: dict = {
    col: (lambda v: "" if pd.isna(v) else f"{v:>+10,.0f}") for col in _NUMERIC_COLS
}
_FORMATTERS["As of Date"] = lambda v: "" if pd.isna(v) else str(v)
_FORMATTERS["Forecast Executed"] = lambda v: (
    "" if pd.isna(v) else pd.Timestamp(v).strftime("%Y-%m-%d %H:%M")
)
_FORMATTERS["Type"] = lambda v: f"{v:<16}"

_TYPE_ORDER: tuple[str, ...] = (
    "load",
    "solar",
    "wind",
    "net_load (rep)",
    "net_load (impl)",
    "delta (rep-impl)",
)

_ONPEAK_IDX: list[int] = [h - 1 for h in range(8, 24)]
_OFFPEAK_IDX: list[int] = [h - 1 for h in list(range(1, 8)) + [24]]


def _values_for(grp: pd.DataFrame, col: str) -> np.ndarray:
    """Return length-24 array of grp[col] indexed by hour_ending, NaN-padded."""
    arr = np.full(24, float("nan"))
    he = grp["hour_ending"].astype(int).to_numpy()
    vals = pd.to_numeric(grp[col], errors="coerce").to_numpy(dtype=float)
    for h, v in zip(he, vals):
        if 1 <= h <= 24:
            arr[h - 1] = v
    return arr


def _summary(values: np.ndarray) -> tuple[float, float, float]:
    """OnPeak/OffPeak/Flat means from a length-24 array (HE-1 indexed)."""
    on = values[_ONPEAK_IDX]
    off = values[_OFFPEAK_IDX]
    onpk = float(np.nanmean(on)) if np.isfinite(on).any() else float("nan")
    offpk = float(np.nanmean(off)) if np.isfinite(off).any() else float("nan")
    flat = float(np.nanmean(values)) if np.isfinite(values).any() else float("nan")
    return onpk, offpk, flat


def _meteo_supply_demand_wide_for_region(
    coalesced: pd.DataFrame,
    region: str,
) -> pd.DataFrame:
    """Wide layout: 6 rows per (date, region, source) — load / solar / wind /
    net_load (rep) / net_load (impl = load - solar - wind) / delta (rep - impl)."""
    df = coalesced[coalesced["region"].astype(str) == region]
    if df.empty:
        return pd.DataFrame(columns=ORDERED_COLS)

    rows: list[dict] = []
    for (date_, src), grp in df.groupby(["date", "source"], sort=False):
        load_v = _values_for(grp, "load_mw")
        solar_v = _values_for(grp, "solar_mw")
        wind_v = _values_for(grp, "wind_mw")
        net_v = _values_for(grp, "net_load_mw")

        # Implied = load - solar.fillna(0) - wind.fillna(0). NaN load
        # propagates; missing renewables treated as zero.
        solar_filled = np.where(np.isnan(solar_v), 0.0, solar_v)
        wind_filled = np.where(np.isnan(wind_v), 0.0, wind_v)
        impl_v = load_v - solar_filled - wind_filled
        delta_v = net_v - impl_v

        # Forecast metadata: any non-null component exec timestamp; NaT for RT.
        fc_exec_series = grp["load_forecast_execution_datetime_local"].dropna()
        fc_exec = fc_exec_series.iloc[0] if not fc_exec_series.empty else pd.NaT
        as_of = (date_ - timedelta(days=1)) if src == "meteologica" else None
        src_label = {"meteologica": "Meteologica", "rt": "RT"}.get(src, src)

        for label, values in (
            ("load", load_v),
            ("solar", solar_v),
            ("wind", wind_v),
            ("net_load (rep)", net_v),
            ("net_load (impl)", impl_v),
            ("delta (rep-impl)", delta_v),
        ):
            onpk, offpk, flat = _summary(values)
            rec = {
                "Source": src_label,
                "Forecast Executed": fc_exec,
                "As of Date": as_of,
                "Date": date_,
                "Region": region,
                "Type": label,
                "OnPeak": onpk,
                "OffPeak": offpk,
                "Flat": flat,
            }
            for h in range(1, 25):
                rec[f"HE{h}"] = float(values[h - 1])
            rows.append(rec)

    out = pd.DataFrame(rows, columns=ORDERED_COLS)
    type_order = {t: i for i, t in enumerate(_TYPE_ORDER)}
    out["_type_idx"] = out["Type"].map(type_order)
    out = (
        out.sort_values(["Date", "_type_idx"], ascending=[False, True])
        .drop(columns="_type_idx")
        .reset_index(drop=True)
    )
    return out


def build_meteo_net_load_table(
    region: str = REGIONS[0],
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> pd.DataFrame:
    """Return the wide Meteologica supply-demand bundle table for ``region``.

    Six rows per (Date, Region): load, solar, wind, net_load (rep),
    net_load (impl), delta (rep-impl).

    ``lookback_days`` trims to the N most recent dates in the data
    (inclusive of the latest date). ``None`` returns every date.

    Columns: Source | Forecast Executed | As of Date | Date | Region |
    Type | OnPeak | OffPeak | Flat | HE1..HE24.
    """
    coalesced = loader.load_meteologica_supply_demand_coalesced(cache_dir=cache_dir)
    if lookback_days is not None and not coalesced.empty:
        cutoff = coalesced["date"].max() - timedelta(days=lookback_days - 1)
        coalesced = coalesced[coalesced["date"] >= cutoff]
    return _meteo_supply_demand_wide_for_region(coalesced, region)


def _print_meteo_supply_demand_region_block(
    pl,
    coalesced: pd.DataFrame,
    region: str,
    lookback_days: int | None,
) -> None:
    """Print one region's section: header, metadata, identity check, table."""
    print_section(f"{region} supply-demand bundle (load + solar + wind + net_load)")

    table = _meteo_supply_demand_wide_for_region(coalesced, region)
    if table.empty:
        pl.warning(f"No supply-demand data for region={region}.")
        return

    date_source = table[["Date", "Source"]].drop_duplicates()
    source_counts = date_source["Source"].value_counts().to_dict()
    date_min = table["Date"].min()
    date_max = table["Date"].max()
    pl.info(
        f"{region}: dates={len(date_source):,} | date range: {date_min} -> {date_max}"
    )
    pl.info(
        f"{region}: source mix: "
        + ", ".join(f"{k}={v:,}" for k, v in source_counts.items())
    )

    # Identity check: max abs delta across all 24 HEs of all (Date) groups.
    delta_rows = table[table["Type"] == "delta (rep-impl)"]
    he_vals = delta_rows[HE_COLS].to_numpy(dtype=float)
    max_abs = float(np.nanmax(np.abs(he_vals))) if he_vals.size else 0.0
    pl.info(f"{region}: identity check max |delta| = {max_abs:,.2f} MW")

    rt_dates = (
        date_source[date_source["Source"] == "RT"]["Date"].tolist()
        if "RT" in source_counts
        else []
    )
    if rt_dates and lookback_days is not None:
        pl.warning(
            f"{region}: {len(rt_dates)} RT-fallback date(s) in window "
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


def run(
    regions: tuple[str, ...] = REGIONS,
    cache_dir: Path | None = CACHE_DIR,
    lookback_days: int | None = LOOKBACK_DAYS,
) -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="check_loaders_meteo_net_load", log_dir=LOG_DIR)
    try:
        lookback_label = (
            f"last {lookback_days}d" if lookback_days is not None else "all dates"
        )
        print_header(f"meteo_supply_demand_coalesced ({lookback_label})")

        with pl.timer("load coalesced Meteologica supply-demand bundle (all regions)"):
            coalesced = loader.load_meteologica_supply_demand_coalesced(
                cache_dir=cache_dir,
            )
            if set(regions) != set(REGIONS):
                coalesced = coalesced[
                    coalesced["region"].astype(str).isin(regions)
                ].copy()

        if coalesced.empty:
            pl.warning(
                "Coalesced Meteologica supply-demand frame is empty; nothing to print."
            )
            return

        if lookback_days is not None:
            cutoff = coalesced["date"].max() - timedelta(days=lookback_days - 1)
            coalesced = coalesced[coalesced["date"] >= cutoff]

        for region in regions:
            _print_meteo_supply_demand_region_block(
                pl, coalesced, region, lookback_days
            )

        pl.success(f"Printed {len(regions)} region(s): {', '.join(regions)}.")
    finally:
        pl.close()


if __name__ == "__main__":
    run()
