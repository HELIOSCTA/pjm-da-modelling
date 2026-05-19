"""Verify pjm_fleet.parquet operational fields against yes_energy observations.

For parquet plants that match a yes_energy.objects PLANT, compare the
static operational assumptions in the parquet to observed values from
``yes_energy.hourly_generation``:

- ``cap_factor`` (parquet) vs empirical (mean_gen / ye_plant_capacity_mw)
- ``heat_rate_mmbtu_mwh`` (parquet, capacity-weighted to plant level)
  vs observed avg_heat_rate (5..25 band, removes obvious outliers)
- ``min_load_mw`` (parquet, summed across units) vs 1st percentile of
  nonzero hourly generation

Uses calendar year 2025 as the observation window. yes_energy covers
only ~133 PJM plants out of ~2,200 in the parquet, so this only
validates the major LPI-tracked units. For static identity checks
(capacity, fuel, zone) see ``verify_vs_yes_energy_identity.py``. For
broader gas heat-rate validation, see ``verify_gas_vs_cems.py``.

Usage::

    python -m da_models.supply_stack.data.validators.verify_vs_yes_energy_operations
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

from backend.utils.aws_postgresql_utils import pull_from_db  # noqa: E402
from da_models.supply_stack.data.sources.yes_energy import (  # noqa: E402
    match_fleet_to_yes_energy,
)

PARQUET_PATH: Path = Path(__file__).resolve().parents[1] / "pjm_fleet.parquet"
OBS_START: str = "2025-01-01"
OBS_END: str = "2026-01-01"  # exclusive
MIN_GEN_OBS: int = 100
MIN_HR_OBS: int = 100
TOP_N: int = 20

_OBS_QUERY = f"""
    SELECT
        h.lpi_objectid,
        COUNT(*) AS n_rows,
        COUNT(h.avg_generation_mw) AS n_gen_obs,
        AVG(h.avg_generation_mw) AS mean_gen_mw,
        MAX(h.max_generation_mw) AS observed_max_gen_mw,
        AVG(h.avg_heat_rate) FILTER (
            WHERE h.avg_heat_rate BETWEEN 5 AND 25
        ) AS mean_heat_rate,
        COUNT(*) FILTER (WHERE h.avg_heat_rate BETWEEN 5 AND 25) AS n_hr_obs,
        PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY h.avg_generation_mw)
            FILTER (WHERE h.avg_generation_mw > 0) AS p01_running_mw
    FROM yes_energy.hourly_generation h
    WHERE h.iso = 'PJMISO'
      AND h.record_type = 'LPI_GEN'
      AND h.market_date >= DATE '{OBS_START}'
      AND h.market_date < DATE '{OBS_END}'
    GROUP BY h.lpi_objectid
"""


def _plant_level_rollup(matched: pd.DataFrame) -> pd.DataFrame:
    """Capacity-weighted aggregation of parquet unit rows to plant level."""
    df = matched.copy()
    df["_hr_x_cap"] = df["heat_rate_mmbtu_mwh"] * df["summer_cap_mw"]
    df["_cf_x_cap"] = df["cap_factor"] * df["summer_cap_mw"]
    agg = df.groupby("plant", as_index=False).agg(
        parquet_summer_mw=("summer_cap_mw", "sum"),
        parquet_min_load_mw=("min_load_mw", "sum"),
        _hr_num=("_hr_x_cap", "sum"),
        _cf_num=("_cf_x_cap", "sum"),
        ye_lpi_objectid=("ye_lpi_objectid", "first"),
        ye_primary_fuel=("ye_primary_fuel", "first"),
        ye_plant_capacity_mw=("ye_plant_capacity_mw", "first"),
    )
    agg["parquet_heat_rate"] = agg["_hr_num"] / agg["parquet_summer_mw"].where(
        agg["parquet_summer_mw"] > 0
    )
    agg["parquet_cap_factor"] = agg["_cf_num"] / agg["parquet_summer_mw"].where(
        agg["parquet_summer_mw"] > 0
    )
    return agg.drop(columns=["_hr_num", "_cf_num"])


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
    matched = merged.dropna(subset=["ye_lpi_objectid"]).copy()
    matched["ye_lpi_objectid"] = matched["ye_lpi_objectid"].astype("int64")
    print(
        f"yes_energy matches: {matched['plant'].nunique():,} plants, "
        f"{len(matched):,} parquet rows"
    )
    print(f"Observation window: {OBS_START} .. {OBS_END} (exclusive)")
    print()

    print("Pulling hourly_generation aggregates from AWS ...")
    obs = pull_from_db(_OBS_QUERY)
    if obs is None:
        raise RuntimeError("hourly_generation pull failed")
    print(f"  rows returned: {len(obs):,} distinct lpi_objectids")
    print()

    plant_agg = _plant_level_rollup(matched)
    fleet_plant = plant_agg.merge(
        obs, left_on="ye_lpi_objectid", right_on="lpi_objectid", how="left"
    )
    fleet_plant["emp_cap_factor"] = fleet_plant["mean_gen_mw"] / fleet_plant[
        "ye_plant_capacity_mw"
    ].where(fleet_plant["ye_plant_capacity_mw"] > 0)
    fleet_plant["cap_factor_gap"] = (
        fleet_plant["parquet_cap_factor"] - fleet_plant["emp_cap_factor"]
    )
    fleet_plant["heat_rate_gap"] = (
        fleet_plant["parquet_heat_rate"] - fleet_plant["mean_heat_rate"]
    )
    fleet_plant["min_load_gap"] = (
        fleet_plant["parquet_min_load_mw"] - fleet_plant["p01_running_mw"]
    )

    obs_mask = fleet_plant["n_gen_obs"] > MIN_GEN_OBS

    # --- Cap factor ---------------------------------------------------
    print(
        f"=== Capacity factor gap (top {TOP_N} by |gap|; n_gen_obs > {MIN_GEN_OBS}) ==="
    )
    cf = (
        fleet_plant.loc[obs_mask]
        .assign(_abs=fleet_plant.loc[obs_mask, "cap_factor_gap"].abs())
        .sort_values("_abs", ascending=False)
        .drop(columns="_abs")
        .head(TOP_N)
    )
    print(
        cf[
            [
                "plant",
                "ye_primary_fuel",
                "parquet_summer_mw",
                "parquet_cap_factor",
                "emp_cap_factor",
                "cap_factor_gap",
                "n_gen_obs",
            ]
        ].to_string(
            index=False,
            formatters={
                "parquet_summer_mw": "{:>8,.0f}".format,
                "parquet_cap_factor": "{:>6.2f}".format,
                "emp_cap_factor": "{:>6.2f}".format,
                "cap_factor_gap": "{:>+6.2f}".format,
                "n_gen_obs": "{:>6,}".format,
            },
        )
    )
    print()
    print(
        "  median |cap_factor_gap|: "
        f"{fleet_plant.loc[obs_mask, 'cap_factor_gap'].abs().median():.3f}"
    )
    print()

    # --- Heat rate ----------------------------------------------------
    hr_mask = (
        obs_mask
        & fleet_plant["mean_heat_rate"].notna()
        & (fleet_plant["n_hr_obs"] > MIN_HR_OBS)
        & (fleet_plant["parquet_heat_rate"] > 0)
    )
    print(f"=== Heat rate gap (top {TOP_N} by |gap|; n_hr_obs > {MIN_HR_OBS}) ===")
    hr = (
        fleet_plant.loc[hr_mask]
        .assign(_abs=fleet_plant.loc[hr_mask, "heat_rate_gap"].abs())
        .sort_values("_abs", ascending=False)
        .drop(columns="_abs")
        .head(TOP_N)
    )
    print(
        hr[
            [
                "plant",
                "ye_primary_fuel",
                "parquet_summer_mw",
                "parquet_heat_rate",
                "mean_heat_rate",
                "heat_rate_gap",
                "n_hr_obs",
            ]
        ].to_string(
            index=False,
            formatters={
                "parquet_summer_mw": "{:>8,.0f}".format,
                "parquet_heat_rate": "{:>6.2f}".format,
                "mean_heat_rate": "{:>6.2f}".format,
                "heat_rate_gap": "{:>+6.2f}".format,
                "n_hr_obs": "{:>6,}".format,
            },
        )
    )
    print()
    print(
        "  median |heat_rate_gap|: "
        f"{fleet_plant.loc[hr_mask, 'heat_rate_gap'].abs().median():.2f} MMBtu/MWh"
    )
    print()

    # --- Min load -----------------------------------------------------
    ml_mask = obs_mask & fleet_plant["p01_running_mw"].notna()
    print(f"=== Min-load gap (top {TOP_N} by |gap|) ===")
    ml = (
        fleet_plant.loc[ml_mask]
        .assign(_abs=fleet_plant.loc[ml_mask, "min_load_gap"].abs())
        .sort_values("_abs", ascending=False)
        .drop(columns="_abs")
        .head(TOP_N)
    )
    print(
        ml[
            [
                "plant",
                "ye_primary_fuel",
                "parquet_summer_mw",
                "parquet_min_load_mw",
                "p01_running_mw",
                "min_load_gap",
            ]
        ].to_string(
            index=False,
            formatters={
                "parquet_summer_mw": "{:>8,.0f}".format,
                "parquet_min_load_mw": "{:>8,.0f}".format,
                "p01_running_mw": "{:>8,.0f}".format,
                "min_load_gap": "{:>+8,.0f}".format,
            },
        )
    )
    print()
    print(
        "  median |min_load_gap|: "
        f"{fleet_plant.loc[ml_mask, 'min_load_gap'].abs().median():,.0f} MW"
    )

    return {"fleet_plant": fleet_plant, "obs": obs}


if __name__ == "__main__":
    run()
