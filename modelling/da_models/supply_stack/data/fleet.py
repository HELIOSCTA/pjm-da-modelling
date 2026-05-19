"""Load the per-unit PJM fleet, cost-ready, for a target delivery date.

Reads ``pjm_fleet.parquet`` (run ``_extract_fleet_from_excel.py`` once to
build it), picks the summer/winter cap rating by season, fills missing
heat rates from technology defaults, classifies each unit's fuel hub
(gas / coal / oil / nuclear / none), precomputes the static carbon cost,
drops the categories that are netted off the load (solar, wind), and
flags must-run units (nuclear). The *fuel price* part of the marginal
cost is left to ``merit_order.py`` because it varies hour-to-hour (the
live gas feed).

Returns ``{"fleet": DataFrame, "meta": dict}``. ``fleet`` columns:
``plant, fuel_category, power_hub, zone, fuel_hub, fuel_kind, capacity_mw,
heat_rate, vom, carbon_cost, min_load_mw, must_run``.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from da_models.supply_stack import configs as C

logger = logging.getLogger(__name__)

_PARQUET_PATH = Path(__file__).resolve().parent / C.FLEET_PARQUET


def _fuel_kind(fuel_hub: str) -> str:
    if not fuel_hub:
        return "none"
    if fuel_hub in C.COAL_FUEL_HUBS:
        return "coal"
    if fuel_hub in C.OIL_FUEL_HUBS:
        return "oil"
    if fuel_hub in C.NUCLEAR_FUEL_HUBS:
        return "nuclear"
    return "gas"  # everything else with a named hub


def build_fleet(target_date: date, *, cache_dir: Path | None = None) -> dict:  # noqa: ARG001
    """Return ``{"fleet": DataFrame, "meta": dict}`` for ``target_date``.

    ``cache_dir`` is accepted for signature symmetry with the other loaders
    but the fleet parquet lives in this package, not the data cache.
    """
    if not _PARQUET_PATH.exists():
        raise RuntimeError(
            f"Fleet parquet not found at {_PARQUET_PATH}. Run: "
            "python -m da_models.supply_stack.data._extract_fleet_from_excel"
        )
    df = pd.read_parquet(_PARQUET_PATH).copy()

    use_summer = target_date.month in C.SUMMER_CAP_MONTHS
    cap_col = "summer_cap_mw" if use_summer else "winter_cap_mw"
    df["capacity_mw"] = (
        pd.to_numeric(df[cap_col], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    df["heat_rate"] = pd.to_numeric(df["heat_rate_mmbtu_mwh"], errors="coerce").fillna(
        0.0
    )
    df["vom"] = pd.to_numeric(df["vom_usd_mwh"], errors="coerce").fillna(0.0)
    df["min_load_mw"] = pd.to_numeric(
        df.get("min_load_mw", 0.0), errors="coerce"
    ).fillna(0.0)

    # Drop renewables (netted off the load) and zero-capacity rows.
    df = df[~df["fuel_category"].isin(C.EXCLUDE_FUEL_CATEGORIES)]
    df = df[df["capacity_mw"] > 0.0].copy()

    # Fill missing heat rates for thermal units.
    is_thermal = df["fuel_category"].isin(C.DEFAULT_HEAT_RATE_MMBTU_MWH.keys())
    needs_hr = is_thermal & (df["heat_rate"] <= 0.0)
    df.loc[needs_hr, "heat_rate"] = (
        df.loc[needs_hr, "fuel_category"]
        .map(C.DEFAULT_HEAT_RATE_MMBTU_MWH)
        .astype(float)
    )

    df["fuel_kind"] = df["fuel_hub"].fillna("").map(_fuel_kind)
    df["must_run"] = df["fuel_category"].isin(C.MUST_RUN_FUEL_CATEGORIES)

    # Static carbon cost ($/MWh) -- RGGI units only; CO2 = heat_rate x intensity.
    is_rggi = df["is_rggi"].fillna(False) if "is_rggi" in df.columns else False
    intensity = (
        df["fuel_category"].map(C.CO2_INTENSITY_TON_MMBTU).fillna(0.0).astype(float)
    )
    df["carbon_cost"] = (
        df["heat_rate"] * intensity * C.CARBON_PRICE_USD_TON_CO2
    ).where(is_rggi, 0.0)

    keep = [
        "plant",
        "fuel_category",
        "power_hub",
        "zone",
        "fuel_hub",
        "fuel_kind",
        "capacity_mw",
        "heat_rate",
        "vom",
        "carbon_cost",
        "min_load_mw",
        "must_run",
    ]
    fleet = df[keep].reset_index(drop=True)

    by_fuel = (
        fleet.groupby("fuel_category")["capacity_mw"].sum().sort_values(ascending=False)
    )
    meta = {
        "source": f"pjm_fleet.parquet ({len(fleet):,} units, {cap_col})",
        "rating": "summer" if use_summer else "winter",
        "total_stack_mw": float(fleet["capacity_mw"].sum()),
        "thermal_mw": float(
            fleet.loc[
                fleet["fuel_kind"].isin(["gas", "coal", "oil"]), "capacity_mw"
            ].sum()
        ),
        "must_run_mw": float(fleet.loc[fleet["must_run"], "capacity_mw"].sum()),
        "capacity_by_fuel_mw": {k: float(v) for k, v in by_fuel.items()},
    }
    return {"fleet": fleet, "meta": meta}
