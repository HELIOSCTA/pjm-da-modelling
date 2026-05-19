"""Build the per-unit PJM fleet from the legacy Excel stack model.

Reads ``.archive/.excel/PJM_Stack_Model_v1_2026_mar_10.xlsx`` -- the
``PJM Raw Data`` sheet (~4,594 generating units) and the ``Assumptions``
sheet (fuel prices, carbon/SO2, emissions factors) -- and writes a clean
``pjm_fleet.parquet`` to the parent ``data/`` package, where
``fleet.py`` reads it. This is the EIA-860/PUDL substitute the design
memo wanted; refresh annually by re-running against an updated workbook,
or use ``builders/build_from_pudl.py`` to build from PUDL directly.

What's kept per unit: plant, fuel_category, unit_type, power_hub, zone,
fuel_hub (which gas/coal/oil hub it buys from), summer/winter cap, cap
factor, heat_rate (MMBtu/MWh -- the Excel labels it "BTU/kWh" but the
numbers are MMBtu/MWh and its own fuel-cost formula has a stray /1000
bug, which we drop), vom, fom, min_load_factor, cold_start_hrs,
so2_factor, baseload_mw, carbon_mkt (RGGI flag), so2_mkt.

The Assumptions table is printed (not persisted) -- those constants live
in ``configs.py`` so they're editable without re-running this script.

Usage::

    python -m da_models.supply_stack.data.builders.build_from_excel
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[5]  # .../helioscta-pjm-da-data-scrapes
_MODELLING_ROOT = Path(__file__).resolve().parents[4]  # .../modelling
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

XLSX_PATH: Path = (
    _REPO_ROOT / ".archive" / ".excel" / "PJM_Stack_Model_v1_2026_mar_10.xlsx"
)
# Write canonical fleet parquet to the parent data/ package, not builders/.
OUT_PARQUET: Path = Path(__file__).resolve().parents[1] / "pjm_fleet.parquet"

_RAW_COLS: dict[str, str] = {
    "Plant Name": "plant",
    "Fuel Category": "fuel_category",
    "Unit Type": "unit_type",
    "Power Hub": "power_hub",
    "Zone": "zone",
    "Fuel Hub": "fuel_hub",
    "Summer Cap (MW)": "summer_cap_mw",
    "Winter Cap (MW)": "winter_cap_mw",
    "Cap Factor": "cap_factor",
    "Heat Rate\n(BTU/kWh)": "heat_rate_mmbtu_mwh",  # mislabeled in the sheet; values are MMBtu/MWh
    "VOM\n($/MWh)": "vom_usd_mwh",
    "FOM\n($/kW-yr)": "fom_usd_kwyr",
    "Min Load\nFactor": "min_load_factor",
    "Cold Start\n(hrs)": "cold_start_hrs",
    "SO2 Factor": "so2_factor",
    "Baseload\n(MW)": "baseload_mw",
    "Carbon Mkt": "carbon_mkt",
    "SO2 Mkt": "so2_mkt",
}


def run() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    if not XLSX_PATH.exists():
        raise FileNotFoundError(f"Workbook not found: {XLSX_PATH}")

    raw = pd.read_excel(
        XLSX_PATH, sheet_name="PJM Raw Data", header=1, engine="openpyxl"
    )
    raw = raw[[c for c in _RAW_COLS if c in raw.columns]].rename(columns=_RAW_COLS)
    raw = raw.dropna(subset=["plant", "fuel_category"]).copy()

    for c in (
        "summer_cap_mw",
        "winter_cap_mw",
        "cap_factor",
        "heat_rate_mmbtu_mwh",
        "vom_usd_mwh",
        "fom_usd_kwyr",
        "min_load_factor",
        "cold_start_hrs",
        "so2_factor",
        "baseload_mw",
    ):
        if c in raw.columns:
            raw[c] = pd.to_numeric(raw[c], errors="coerce").fillna(0.0)
    for c in (
        "plant",
        "fuel_category",
        "unit_type",
        "power_hub",
        "zone",
        "fuel_hub",
        "carbon_mkt",
        "so2_mkt",
    ):
        if c in raw.columns:
            raw[c] = raw[c].astype(str).str.strip()
    # "0" / "0.0" fuel-hub values mean "no fuel cost" (renewables/storage/biomass/hydro).
    raw["fuel_hub"] = raw["fuel_hub"].where(
        ~raw["fuel_hub"].isin({"0", "0.0", "nan", "None"}), other=""
    )
    raw["is_rggi"] = raw["carbon_mkt"].str.upper().str.startswith("RGGI")
    raw["min_load_mw"] = (raw["min_load_factor"] * raw["summer_cap_mw"]).clip(lower=0.0)

    raw = raw.reset_index(drop=True)
    raw.to_parquet(OUT_PARQUET, index=False)

    # Echo summary + the Assumptions table (the latter lives in configs.py).
    print(
        f"Wrote {OUT_PARQUET}  ({len(raw):,} units, {raw['summer_cap_mw'].sum():,.0f} MW summer cap)"
    )
    by_fuel = (
        raw.groupby("fuel_category")
        .agg(
            units=("plant", "size"),
            summer_mw=("summer_cap_mw", "sum"),
            mean_hr=("heat_rate_mmbtu_mwh", "mean"),
        )
        .sort_values("summer_mw", ascending=False)
    )
    print(by_fuel.to_string())
    print(
        f"\nRGGI-flagged units: {int(raw['is_rggi'].sum()):,}  |  fuel hubs: {sorted(h for h in raw['fuel_hub'].unique() if h)}"
    )

    assumptions = pd.read_excel(
        XLSX_PATH, sheet_name="Assumptions", header=None, engine="openpyxl"
    )
    print(
        "\n--- Assumptions sheet (for reference; the constants live in configs.py) ---"
    )
    for _, row in assumptions.iterrows():
        vals = [v for v in row.tolist() if pd.notna(v)]
        if vals:
            print(vals)


if __name__ == "__main__":
    run()
