"""Verify Excel thermal-fleet operational fields against EPA CEMS observations.

For every Excel parquet plant that has a PUDL match (via name) and a
CEMS-monitored unit, compare:

- ``cap_factor`` (Excel, plant-level, capacity-weighted) vs empirical
  (mean ``gross_load_mw`` / Excel ``summer_cap_mw``)
- ``heat_rate_mmbtu_mwh`` (Excel, capacity-weighted) vs CEMS-implied
  heat rate (Σ ``heat_content_mmbtu`` / Σ ``gross_load_mw``)

Covers all CEMS-monitored fuels: Gas CC, Gas CT/ST, Coal, Oil, Biomass.
CEMS is the highest-quality observed dataset available -- every fossil
unit in PJM with an EPA monitor reports hourly. This validator finally
lets us ground-truth Excel heat rates and cap factors.

Prereq: ``artifacts/pjm_fleet_pudl.parquet`` and
``artifacts/pudl_generators_audit.parquet`` (run
``builders/build_from_pudl.py`` first). The audit parquet supplies the
``plant_id_eia`` -> Excel-plant join.

Usage::

    python -m da_models.supply_stack.data.validators.verify_thermal_vs_cems
    python -m da_models.supply_stack.data.validators.verify_thermal_vs_cems --year 2024
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np

_REPO_ROOT = Path(__file__).resolve().parents[5]
_MODELLING_ROOT = Path(__file__).resolve().parents[4]
for _p in (_MODELLING_ROOT, _REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import pandas as pd  # noqa: E402

from da_models.supply_stack.data.sources.cems import (  # noqa: E402
    pull_hourly_emissions,
)

EXCEL_PARQUET: Path = Path(__file__).resolve().parents[1] / "pjm_fleet.parquet"
PUDL_AUDIT_PARQUET: Path = (
    Path(__file__).resolve().parents[1] / "artifacts" / "pudl_generators_audit.parquet"
)
OUT_VALIDATION_PARQUET: Path = (
    Path(__file__).resolve().parents[1]
    / "artifacts"
    / "thermal_cems_validation.parquet"
)

# Excel fuel_category -> PUDL fleet_fuel_type for CEMS-covered thermal fuels.
EXCEL_THERMAL: list[str] = ["Gas CC", "Gas CT/ST", "Coal", "Oil", "Biomass"]
PUDL_THERMAL: list[str] = ["cc_gas", "ct_gas", "coal", "oil", "other"]

DEFAULT_YEAR: int = 2024
TOP_N: int = 20
MIN_OBS_HOURS: int = 100


def _normalize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = name.lower()
    n = re.sub(r"\([^)]*\)", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _plant_level_excel_thermal(fleet: pd.DataFrame) -> pd.DataFrame:
    """Roll Excel thermal units up to plant level (capacity-weighted hr + cf)."""
    df = fleet[fleet["fuel_category"].isin(EXCEL_THERMAL)].copy()
    df["norm_plant"] = df["plant"].map(_normalize)
    df["_hr_x_cap"] = df["heat_rate_mmbtu_mwh"] * df["summer_cap_mw"]
    df["_cf_x_cap"] = df["cap_factor"] * df["summer_cap_mw"]
    agg = df.groupby("norm_plant", as_index=False).agg(
        excel_plant=("plant", "first"),
        excel_summer_mw=("summer_cap_mw", "sum"),
        excel_units=("plant", "size"),
        excel_fuel=("fuel_category", lambda s: s.mode().iloc[0] if len(s) else ""),
        _hr_num=("_hr_x_cap", "sum"),
        _cf_num=("_cf_x_cap", "sum"),
    )
    agg["excel_heat_rate"] = agg["_hr_num"] / agg["excel_summer_mw"].where(
        agg["excel_summer_mw"] > 0
    )
    agg["excel_cap_factor"] = agg["_cf_num"] / agg["excel_summer_mw"].where(
        agg["excel_summer_mw"] > 0
    )
    return agg.drop(columns=["_hr_num", "_cf_num"])


def _plant_pudl_thermal(audit: pd.DataFrame) -> pd.DataFrame:
    """Roll PUDL audit (per generator) up to plant level for thermal fuels."""
    df = audit[audit["fleet_fuel_type"].isin(PUDL_THERMAL)].copy()
    df["norm_plant"] = df["plant_name_eia"].map(_normalize)
    agg = df.groupby(["norm_plant", "plant_id_eia"], as_index=False).agg(
        pudl_plant=("plant_name_eia", "first"),
        pudl_summer_mw=("summer_capacity_mw", "sum"),
        pudl_gens=("generator_id", "size"),
        pudl_fuel=("fleet_fuel_type", lambda s: s.mode().iloc[0] if len(s) else ""),
        pjm_zone=("pjm_zone", "first"),
    )
    return agg


def _per_plant_cems_metrics(cems: pd.DataFrame) -> pd.DataFrame:
    """Aggregate CEMS hourly rows to per-plant annual metrics."""
    df = cems.copy()
    df["gross_load_mw"] = df["gross_load_mw"].clip(lower=0)
    df["heat_content_mmbtu"] = df["heat_content_mmbtu"].clip(lower=0)
    plant = df.groupby("plant_id_eia", as_index=False).agg(
        n_obs_hours=("gross_load_mw", "size"),
        n_running_hours=("gross_load_mw", lambda s: (s > 0).sum()),
        cems_mean_mw=("gross_load_mw", "mean"),
        cems_peak_mw=("gross_load_mw", "max"),
        cems_p01_running_mw=(
            "gross_load_mw",
            lambda s: float(np.percentile(s[s > 0], 1)) if (s > 0).any() else np.nan,
        ),
        cems_total_gen_mwh=("gross_load_mw", "sum"),
        cems_total_heat_mmbtu=("heat_content_mmbtu", "sum"),
    )
    plant["cems_implied_heat_rate"] = plant["cems_total_heat_mmbtu"] / plant[
        "cems_total_gen_mwh"
    ].where(plant["cems_total_gen_mwh"] > 0)
    return plant


def run(
    year: int = DEFAULT_YEAR,
    channel: str = "stable",
    dry_run: bool = False,
) -> dict:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)

    if not EXCEL_PARQUET.exists():
        raise FileNotFoundError(EXCEL_PARQUET)
    if not PUDL_AUDIT_PARQUET.exists():
        raise FileNotFoundError(
            f"{PUDL_AUDIT_PARQUET} -- run builders/build_from_pudl.py first"
        )

    excel = pd.read_parquet(EXCEL_PARQUET)
    audit = pd.read_parquet(PUDL_AUDIT_PARQUET)

    excel_th = _plant_level_excel_thermal(excel)
    pudl_th = _plant_pudl_thermal(audit)
    print(
        f"Excel thermal plants: {len(excel_th):,}   PUDL thermal plants: {len(pudl_th):,}"
    )

    matched = excel_th.merge(pudl_th, on="norm_plant", how="inner")
    plant_ids = sorted(matched["plant_id_eia"].astype(int).unique().tolist())
    print(f"Matched (name): {len(matched):,} plants, {len(plant_ids):,} EIA plant_ids")
    print()

    print(f"Pulling CEMS hourly for {len(plant_ids):,} plants, year={year} ...")
    cems = pull_hourly_emissions(plant_ids, year=year, channel=channel)
    print(f"  {len(cems):,} hourly rows from {cems['plant_id_eia'].nunique():,} plants")
    print()

    cems_metrics = _per_plant_cems_metrics(cems)
    out = matched.merge(cems_metrics, on="plant_id_eia", how="left")

    out["emp_cap_factor"] = out["cems_mean_mw"] / out["excel_summer_mw"].where(
        out["excel_summer_mw"] > 0
    )
    out["cap_factor_gap"] = out["excel_cap_factor"] - out["emp_cap_factor"]
    out["heat_rate_gap"] = out["excel_heat_rate"] - out["cems_implied_heat_rate"]

    obs_mask = out["n_obs_hours"].fillna(0) > MIN_OBS_HOURS

    # --- Heat-rate gap (the one yes_energy couldn't reach) -----------
    print(
        f"=== Heat rate gap: Excel vs CEMS-implied (top {TOP_N} by |gap| MMBtu/MWh; "
        f"n_obs > {MIN_OBS_HOURS}) ==="
    )
    hr = (
        out.loc[obs_mask]
        .assign(_abs=out.loc[obs_mask, "heat_rate_gap"].abs())
        .sort_values("_abs", ascending=False)
        .drop(columns="_abs")
        .head(TOP_N)
    )
    print(
        hr[
            [
                "excel_plant",
                "excel_fuel",
                "pjm_zone",
                "excel_summer_mw",
                "excel_heat_rate",
                "cems_implied_heat_rate",
                "heat_rate_gap",
                "n_obs_hours",
            ]
        ].to_string(
            index=False,
            formatters={
                "excel_summer_mw": "{:>8,.0f}".format,
                "excel_heat_rate": "{:>6.2f}".format,
                "cems_implied_heat_rate": "{:>6.2f}".format,
                "heat_rate_gap": "{:>+6.2f}".format,
                "n_obs_hours": "{:>6,}".format,
            },
        )
    )
    print()
    print(
        "  median |heat_rate_gap|: "
        f"{out.loc[obs_mask, 'heat_rate_gap'].abs().median():.2f} MMBtu/MWh"
    )
    print()

    # --- Cap factor gap ----------------------------------------------
    print(
        f"=== Cap factor gap: Excel vs CEMS-empirical (top {TOP_N} by |gap|; "
        f"n_obs > {MIN_OBS_HOURS}) ==="
    )
    cf = (
        out.loc[obs_mask]
        .assign(_abs=out.loc[obs_mask, "cap_factor_gap"].abs())
        .sort_values("_abs", ascending=False)
        .drop(columns="_abs")
        .head(TOP_N)
    )
    print(
        cf[
            [
                "excel_plant",
                "excel_fuel",
                "pjm_zone",
                "excel_summer_mw",
                "excel_cap_factor",
                "emp_cap_factor",
                "cap_factor_gap",
                "n_obs_hours",
            ]
        ].to_string(
            index=False,
            formatters={
                "excel_summer_mw": "{:>8,.0f}".format,
                "excel_cap_factor": "{:>6.2f}".format,
                "emp_cap_factor": "{:>6.2f}".format,
                "cap_factor_gap": "{:>+6.2f}".format,
                "n_obs_hours": "{:>6,}".format,
            },
        )
    )
    print()
    print(
        "  median |cap_factor_gap|: "
        f"{out.loc[obs_mask, 'cap_factor_gap'].abs().median():.3f}"
    )

    if not dry_run:
        OUT_VALIDATION_PARQUET.parent.mkdir(exist_ok=True)
        out.to_parquet(OUT_VALIDATION_PARQUET, index=False)
        print(f"\nWrote {OUT_VALIDATION_PARQUET}")

    return {"out": out, "cems": cems}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate Excel gas fleet against EPA CEMS")
    p.add_argument("--year", type=int, default=DEFAULT_YEAR)
    p.add_argument("--channel", default="stable", choices=["stable", "nightly"])
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(year=args.year, channel=args.channel, dry_run=args.dry_run)
