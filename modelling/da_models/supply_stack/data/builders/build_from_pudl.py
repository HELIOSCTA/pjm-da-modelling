"""Build the per-unit PJM fleet from PUDL (EIA-860/923).

Pulls ``out_eia__monthly_generators`` + ``core_eia860__scd_plants`` from
the public PUDL S3 bucket, filters to PJM existing generators (excluding
solar/wind which are handled by the renewable forecast feeds), classifies
fuel type by ``prime_mover_code`` (CC vs CT for gas), and joins the
EIA-860 transmission/distribution owner to derive PJM zone.

Writes a per-unit parquet to ``artifacts/pjm_fleet_pudl.parquet`` --
parallel grain to the Excel-built ``pjm_fleet.parquet``, NOT the
aggregated-block CSV the other repo produces. This sits alongside the
Excel fleet so the verifier can join on EIA ``plant_id_eia`` and get
~100% coverage instead of yes_energy's ~1.4%.

Also writes ``artifacts/pudl_generators_audit.parquet`` with the
generator-level audit trail (used by CEMS / EIA-923 validators for
join keys).

Usage::

    python -m da_models.supply_stack.data.builders.build_from_pudl
    python -m da_models.supply_stack.data.builders.build_from_pudl --year 2024
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np

_REPO_ROOT = Path(__file__).resolve().parents[5]
_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import pandas as pd  # noqa: E402

from da_models.supply_stack.data.sources.pudl import (  # noqa: E402
    pull_generators,
    pull_plants,
)

logger = logging.getLogger(__name__)

_ARTIFACTS_DIR: Path = Path(__file__).resolve().parents[1] / "artifacts"
OUT_FLEET_PARQUET: Path = _ARTIFACTS_DIR / "pjm_fleet_pudl.parquet"
OUT_AUDIT_PARQUET: Path = _ARTIFACTS_DIR / "pudl_generators_audit.parquet"

# Renewables handled by forecast feeds, not the dispatch stack.
SKIP_FUELS: set[str] = {"solar", "wind"}

# Prime mover -> CC vs CT classification for gas.
_CC_PRIME_MOVERS: set[str] = {"CA", "CS"}

# EIA-860 transmission/distribution owner -> PJM pricing zone.
# Authoritative because PJM zones are defined by the TD owner; mirrors the
# mapping in the helioscta-pjm-da supply_stack_model.
TD_OWNER_TO_ZONE: dict[str, str] = {
    # Exelon / Constellation
    "Commonwealth Edison Co": "COMED",
    "PECO Energy Co": "PECO",
    "Baltimore Gas & Electric Co": "BGE",
    # Dominion
    "Virginia Electric & Power Co": "DOM",
    # FirstEnergy
    "American Transmission Systems Inc": "ATSI",
    "Ohio Edison Co": "ATSI",
    "Cleveland Electric Illum Co": "ATSI",
    "The Toledo Edison Co": "ATSI",
    "FirstEnergy Co": "ATSI",
    "Metropolitan Edison Co": "METED",
    "Pennsylvania Electric Co": "PENELEC",
    "West Penn Power Company": "AP",
    "West Penn Power Co": "AP",
    "Monongahela Power Co": "AP",
    "The Potomac Edison Company": "AP",
    # AEP
    "Ohio Power Co": "AEP",
    "Indiana Michigan Power Co": "AEP",
    "Appalachian Power Co": "AEP",
    "Wheeling Power Co": "AEP",
    "AEP Appalachian Transmission Co Inc": "AEP",
    "AEP Ohio Transmission Co Inc": "AEP",
    # PSEG
    "Public Service Elec & Gas Co": "PSEG",
    # PPL
    "PPL Electric Utilities Corp": "PPL",
    # Duquesne
    "Duquesne Light Co": "DUQ",
    # Duke Ohio/Kentucky
    "Duke Energy Ohio Inc": "DEOK",
    "Duke Energy Kentucky Inc": "DEOK",
    "Duke Energy Indiana LLC": "DEOK",
    # Pepco Holdings
    "Potomac Electric Power Co": "PEPCO",
    "Delmarva Power": "DPL",
    "Delmarva Power & Light Co": "DPL",
    "Atlantic City Electric Co": "AECO",
    # Jersey Central
    "Jersey Central Power & Lt Co": "JCPL",
    # Dayton
    "Dayton Power & Light Co": "DAY",
    "AES Ohio": "DAY",
    # East Kentucky
    "East Kentucky Power Coop, Inc": "EKPC",
    "East Kentucky Power Cooperative Inc": "EKPC",
    # Rockland
    "Rockland Electric Co": "RECO",
}


def _classify_fuel_type(row: pd.Series) -> str:
    """Map PUDL fuel_type_code_pudl + prime_mover_code to fleet fuel_type."""
    pudl_fuel = str(row.get("fuel_type_code_pudl", "") or "").lower().strip()
    pm = str(row.get("prime_mover_code", "") or "").upper().strip()
    if pudl_fuel == "nuclear":
        return "nuclear"
    if pudl_fuel == "coal":
        return "coal"
    if pudl_fuel == "oil":
        return "oil"
    if pudl_fuel == "hydro":
        return "hydro"
    if pudl_fuel == "gas":
        return "cc_gas" if pm in _CC_PRIME_MOVERS else "ct_gas"
    if pudl_fuel in ("waste", "other", "geothermal"):
        return "other"
    return "other"


def _build_plant_zone_map(
    plants_df: pd.DataFrame,
    capacity_year: int | None,
) -> pd.DataFrame:
    """Map plant_id_eia -> pjm_zone via EIA-860 transmission/distribution owner."""
    df = plants_df.dropna(subset=["transmission_distribution_owner_name"]).copy()
    if df.empty:
        return pd.DataFrame(columns=["plant_id_eia", "pjm_zone"])

    latest_td_year = int(df["report_date"].dt.year.max())
    use_year = min(capacity_year, latest_td_year) if capacity_year else latest_td_year
    df = df[df["report_date"].dt.year == use_year]
    df = df.sort_values("report_date").drop_duplicates(
        subset=["plant_id_eia"], keep="last"
    )
    df["pjm_zone"] = (
        df["transmission_distribution_owner_name"].map(TD_OWNER_TO_ZONE).fillna("")
    )
    return df[["plant_id_eia", "pjm_zone", "transmission_distribution_owner_name"]]


def _filter_and_classify(
    generators: pd.DataFrame,
    plant_zone_map: pd.DataFrame,
    capacity_year: int | None,
    heat_rate_year: int | None,
) -> pd.DataFrame:
    """Filter to PJM existing generators and classify."""
    df = generators.copy()
    df["year"] = df["report_date"].dt.year

    # Capacity year = latest available with data
    if capacity_year is None:
        capacity_year = int(df["year"].max())
    cap_df = df[df["year"] == capacity_year]
    cap_df = cap_df[cap_df["operational_status"] == "existing"]
    cap_df = cap_df[~cap_df["fuel_type_code_pudl"].isin(SKIP_FUELS)]
    cap_df = cap_df.sort_values("report_date").drop_duplicates(
        subset=["plant_id_eia", "generator_id"], keep="last"
    )

    # Heat rate year = latest year with non-null observations (often lags
    # capacity by ~1 year because EIA-923 closes after EIA-860).
    if heat_rate_year is None:
        hr_cov = df.groupby("year")["unit_heat_rate_mmbtu_per_mwh"].apply(
            lambda s: s.notna().sum()
        )
        valid = hr_cov[hr_cov > 0]
        heat_rate_year = int(valid.index.max()) if len(valid) > 0 else capacity_year

    hr_df = df[df["year"] == heat_rate_year].copy()
    hr_df = hr_df[hr_df["unit_heat_rate_mmbtu_per_mwh"].notna()]
    hr_df = hr_df[np.isfinite(hr_df["unit_heat_rate_mmbtu_per_mwh"])]
    hr_df = hr_df[
        (hr_df["unit_heat_rate_mmbtu_per_mwh"] > 0)
        & (hr_df["unit_heat_rate_mmbtu_per_mwh"] < 30)
    ]
    hr_avg = (
        hr_df.groupby(["plant_id_eia", "generator_id"])["unit_heat_rate_mmbtu_per_mwh"]
        .mean()
        .reset_index()
        .rename(columns={"unit_heat_rate_mmbtu_per_mwh": "avg_heat_rate"})
    )

    # Fuel-cost averaging from the same year (per-unit monthly $/MMBtu).
    fc_df = df[df["year"] == heat_rate_year].copy()
    fc_df = fc_df[fc_df["fuel_cost_per_mmbtu"].notna()]
    fc_df = fc_df[np.isfinite(fc_df["fuel_cost_per_mmbtu"])]
    fc_avg = (
        fc_df.groupby(["plant_id_eia", "generator_id"])["fuel_cost_per_mmbtu"]
        .mean()
        .reset_index()
        .rename(columns={"fuel_cost_per_mmbtu": "avg_fuel_cost_usd_mmbtu"})
    )

    out = cap_df.merge(hr_avg, on=["plant_id_eia", "generator_id"], how="left")
    out = out.merge(fc_avg, on=["plant_id_eia", "generator_id"], how="left")

    out["fleet_fuel_type"] = out.apply(_classify_fuel_type, axis=1)

    if not plant_zone_map.empty:
        zone_lookup = plant_zone_map[["plant_id_eia", "pjm_zone"]].drop_duplicates(
            "plant_id_eia"
        )
        out = out.merge(zone_lookup, on="plant_id_eia", how="left")
        out["pjm_zone"] = out["pjm_zone"].fillna("")
    else:
        out["pjm_zone"] = ""

    out["effective_capacity_mw"] = out["summer_capacity_mw"].fillna(out["capacity_mw"])
    out["_capacity_year"] = capacity_year
    out["_heat_rate_year"] = heat_rate_year
    return out


def _to_fleet_schema(gen: pd.DataFrame) -> pd.DataFrame:
    """Project the classified generator frame to the per-unit fleet schema.

    Mirrors the Excel-built ``pjm_fleet.parquet`` shape where possible so
    the two artifacts can be diffed directly.
    """
    out = pd.DataFrame(
        {
            "plant_id_eia": gen["plant_id_eia"].astype("int64"),
            "generator_id": gen["generator_id"].astype(str),
            "plant": gen["plant_name_eia"].astype(str),
            "utility_name_eia": gen["utility_name_eia"].astype(str),
            "state": gen["state"].astype(str),
            "fuel_category": gen["fleet_fuel_type"].astype(str),
            "primary_fuel_pudl": gen["fuel_type_code_pudl"].astype(str),
            "prime_mover_code": gen["prime_mover_code"].astype(str),
            "technology_description": gen["technology_description"].astype(str),
            "operational_status": gen["operational_status"].astype(str),
            "zone": gen["pjm_zone"].fillna("").astype(str),
            "summer_cap_mw": pd.to_numeric(
                gen["summer_capacity_mw"], errors="coerce"
            ).fillna(0.0),
            "winter_cap_mw": pd.to_numeric(
                gen["winter_capacity_mw"], errors="coerce"
            ).fillna(0.0),
            "nameplate_cap_mw": pd.to_numeric(
                gen["capacity_mw"], errors="coerce"
            ).fillna(0.0),
            "effective_cap_mw": pd.to_numeric(
                gen["effective_capacity_mw"], errors="coerce"
            ).fillna(0.0),
            "heat_rate_mmbtu_mwh": pd.to_numeric(gen["avg_heat_rate"], errors="coerce"),
            "min_load_mw": pd.to_numeric(gen["minimum_load_mw"], errors="coerce"),
            "avg_fuel_cost_usd_mmbtu": pd.to_numeric(
                gen["avg_fuel_cost_usd_mmbtu"], errors="coerce"
            ),
            "capacity_year": gen["_capacity_year"].astype("int32"),
            "heat_rate_year": gen["_heat_rate_year"].astype("int32"),
        }
    )
    return out.reset_index(drop=True)


def run(
    channel: str = "stable",
    capacity_year: int | None = None,
    heat_rate_year: int | None = None,
    dry_run: bool = False,
) -> dict:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    print(f"Pulling PUDL generators (channel={channel}) ...")
    generators = pull_generators(channel=channel)
    print(f"  {len(generators):,} PJM generator-month rows")
    print("Pulling PUDL plants ...")
    plants = pull_plants(channel=channel)
    print(f"  {len(plants):,} PJM plant-year rows")
    print()

    plant_zone_map = _build_plant_zone_map(plants, capacity_year=capacity_year)
    print(
        f"Zone map: {len(plant_zone_map):,} plants; "
        f"{(plant_zone_map['pjm_zone'] != '').sum():,} mapped to a PJM zone"
    )

    classified = _filter_and_classify(
        generators,
        plant_zone_map=plant_zone_map,
        capacity_year=capacity_year,
        heat_rate_year=heat_rate_year,
    )
    cap_year = int(classified["_capacity_year"].iloc[0])
    hr_year = int(classified["_heat_rate_year"].iloc[0])
    print(
        f"Classified: {len(classified):,} existing generators, "
        f"capacity_year={cap_year}, heat_rate_year={hr_year}"
    )

    fleet = _to_fleet_schema(classified)
    by_fuel = (
        fleet.groupby("fuel_category")
        .agg(
            units=("plant", "size"),
            cap_gw=("effective_cap_mw", lambda s: s.sum() / 1000),
        )
        .sort_values("cap_gw", ascending=False)
    )
    print()
    print("=== Fleet summary (per-unit, GW by fuel_category) ===")
    print(
        by_fuel.to_string(
            formatters={"cap_gw": "{:>6.1f}".format, "units": "{:>5,}".format}
        )
    )
    print()
    print(
        f"Total: {len(fleet):,} units, {fleet['effective_cap_mw'].sum() / 1000:,.1f} GW"
    )

    if dry_run:
        print("\n(dry-run: not writing artifacts)")
        return {"fleet": fleet, "classified": classified}

    _ARTIFACTS_DIR.mkdir(exist_ok=True)
    fleet.to_parquet(OUT_FLEET_PARQUET, index=False)
    print(f"\nWrote {OUT_FLEET_PARQUET}")

    audit_cols = [
        "plant_id_eia",
        "generator_id",
        "plant_name_eia",
        "utility_name_eia",
        "state",
        "pjm_zone",
        "fuel_type_code_pudl",
        "prime_mover_code",
        "technology_description",
        "operational_status",
        "capacity_mw",
        "summer_capacity_mw",
        "winter_capacity_mw",
        "effective_capacity_mw",
        "minimum_load_mw",
        "avg_heat_rate",
        "avg_fuel_cost_usd_mmbtu",
        "fleet_fuel_type",
    ]
    audit = classified[[c for c in audit_cols if c in classified.columns]]
    audit.to_parquet(OUT_AUDIT_PARQUET, index=False)
    print(f"Wrote {OUT_AUDIT_PARQUET}")

    return {"fleet": fleet, "classified": classified}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build PJM per-unit fleet from PUDL")
    p.add_argument("--channel", default="stable", choices=["stable", "nightly"])
    p.add_argument(
        "--year",
        dest="capacity_year",
        type=int,
        default=None,
        help="Capacity/status year (default: latest)",
    )
    p.add_argument(
        "--heat-rate-year",
        type=int,
        default=None,
        help="Heat rate year (default: latest with data)",
    )
    p.add_argument(
        "--dry-run", action="store_true", help="Print summary without writing artifacts"
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(
        channel=args.channel,
        capacity_year=args.capacity_year,
        heat_rate_year=args.heat_rate_year,
        dry_run=args.dry_run,
    )
