"""Verify pjm_fleet.parquet against yes_energy.objects (identity layer).

Static / dimensional cross-checks of the per-unit fleet parquet against
the LPI plant dimension in AWS:

- Name-match coverage (parquet plants -> yes_energy.objects PLANTs)
- Capacity reconciliation (summer_cap_mw vs plant_capacity_mw / max_cap_mw)
- Fuel-category contingency (parquet.fuel_category x objects.primary_fuel)
- Zone contingency (parquet.zone x objects.source_zone)
- A sample of unmatched parquet plants for spot-checking

For the operational checks (observed cap factor, heat rate, min load)
see ``verify_vs_yes_energy_operations.py``. For broader-coverage
identity checks against EIA-860, see ``verify_vs_pudl_identity.py``.

Note: yes_energy is the LPI-licensed subset (~133 PJM PLANTs vs ~2,200
in the parquet). Unmatched parquet plants are not necessarily bad data
-- they just aren't in the LPI feed.

Usage::

    python -m da_models.supply_stack.data.validators.verify_vs_yes_energy_identity
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[5]
_MODELLING_ROOT = Path(__file__).resolve().parents[4]
for _p in (_MODELLING_ROOT, _REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import pandas as pd  # noqa: E402

from da_models.supply_stack.data.sources.yes_energy import (  # noqa: E402
    match_fleet_to_yes_energy,
)

PARQUET_PATH: Path = Path(__file__).resolve().parents[1] / "pjm_fleet.parquet"
TOP_N: int = 20


def run() -> dict:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)

    if not PARQUET_PATH.exists():
        raise FileNotFoundError(f"Fleet parquet not found: {PARQUET_PATH}")
    fleet = pd.read_parquet(PARQUET_PATH)
    print(f"Fleet parquet: {len(fleet):,} rows, {fleet['plant'].nunique():,} plants")

    merged = match_fleet_to_yes_energy(fleet)
    matched_mask = merged["ye_lpi_objectid"].notna()
    n_parq_plants = fleet["plant"].nunique()
    n_matched_plants = merged.loc[matched_mask, "plant"].nunique()
    print(
        f"yes_energy matches: {n_matched_plants:,} / {n_parq_plants:,} plants "
        f"({n_matched_plants / n_parq_plants * 100:.1f}%); "
        f"{matched_mask.sum():,} / {len(merged):,} parquet rows"
    )
    print()

    # --- Capacity reconciliation ------------------------------------
    print("=== Capacity reconciliation (matched plants, plant-level rollup) ===")
    plant_cap = (
        merged.loc[matched_mask]
        .groupby("plant", as_index=False)
        .agg(
            parquet_summer_mw=("summer_cap_mw", "sum"),
            parquet_winter_mw=("winter_cap_mw", "sum"),
            n_parquet_units=("plant", "size"),
            ye_plant_capacity_mw=("ye_plant_capacity_mw", "first"),
            ye_max_cap_mw=("ye_max_cap_mw", "first"),
        )
    )
    plant_cap["summer_minus_ye"] = (
        plant_cap["parquet_summer_mw"] - plant_cap["ye_plant_capacity_mw"]
    )
    plant_cap["pct_gap"] = (
        plant_cap["summer_minus_ye"] / plant_cap["ye_plant_capacity_mw"] * 100
    )

    top = (
        plant_cap.assign(_abs=plant_cap["summer_minus_ye"].abs())
        .sort_values("_abs", ascending=False)
        .drop(columns="_abs")
        .head(TOP_N)
    )
    print(
        top.to_string(
            index=False,
            formatters={
                "parquet_summer_mw": "{:>9,.0f}".format,
                "parquet_winter_mw": "{:>9,.0f}".format,
                "ye_plant_capacity_mw": "{:>9,.0f}".format,
                "ye_max_cap_mw": "{:>9,.0f}".format,
                "summer_minus_ye": "{:>9,.0f}".format,
                "pct_gap": "{:>7,.1f}".format,
            },
        )
    )
    print()
    print(
        f"  median |gap|: {plant_cap['summer_minus_ye'].abs().median():,.0f} MW   "
        f"median |pct_gap|: {plant_cap['pct_gap'].abs().median():.1f}%   "
        f"plants with |pct_gap| > 20%: "
        f"{(plant_cap['pct_gap'].abs() > 20).sum()} / {len(plant_cap)}"
    )
    print()

    # --- Fuel category contingency ----------------------------------
    print("=== Fuel category contingency (matched rows) ===")
    print(
        pd.crosstab(
            merged.loc[matched_mask, "fuel_category"],
            merged.loc[matched_mask, "ye_primary_fuel"],
        ).to_string()
    )
    print()

    # --- Zone contingency -------------------------------------------
    print("=== Zone contingency (matched plants) ===")
    plant_zone = merged.loc[matched_mask].drop_duplicates(subset=["plant"])
    print(pd.crosstab(plant_zone["zone"], plant_zone["ye_source_zone"]).to_string())
    print()

    # --- Unmatched sample -------------------------------------------
    unmatched = merged.loc[
        ~matched_mask, ["plant", "fuel_category", "zone", "summer_cap_mw"]
    ].drop_duplicates(subset=["plant"])
    sample_n = min(TOP_N, len(unmatched))
    print(
        f"=== Unmatched parquet plants (sample of {sample_n} / "
        f"{len(unmatched)} total) ==="
    )
    print(
        unmatched.sort_values("summer_cap_mw", ascending=False)
        .head(sample_n)
        .to_string(
            index=False,
            formatters={"summer_cap_mw": "{:>8,.0f}".format},
        )
    )

    return {"merged": merged, "plant_cap": plant_cap}


if __name__ == "__main__":
    run()
