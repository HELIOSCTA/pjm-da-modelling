"""Verify Excel nuclear-fleet operational fields against NRC daily reactor power.

For every Excel parquet plant whose ``fuel_category == 'Nuclear'``, pull
NRC daily power-level status and compare:

- Empirical capacity factor (mean of Power% / 100 over the 365-day NRC
  window) vs Excel ``cap_factor``.
- Min-load reasonability: nuclear should run at full output or be down
  for refueling -- there's no economic min-load to speak of. We report
  the 1st-percentile of nonzero daily power and compare to Excel
  ``min_load_mw`` (capacity-weighted at plant level).

This validator is the one that catches the Limerick-style bug we saw
in ``verify_vs_yes_energy_operations.py``: Excel encodes min_load as
~10% of nameplate for nuclear, but the observed floor is ~95% of
nameplate.

NRC publishes a rolling 365-day window, so no ``year`` arg.

Usage::

    python -m da_models.supply_stack.data.validators.verify_nuclear_vs_nrc
"""

from __future__ import annotations

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

from da_models.supply_stack.data.sources.nrc import (  # noqa: E402
    PJM_NRC_CROSSWALK,
    filter_pjm,
    pull_reactor_status,
)

EXCEL_PARQUET: Path = Path(__file__).resolve().parents[1] / "pjm_fleet.parquet"
OUT_VALIDATION_PARQUET: Path = (
    Path(__file__).resolve().parents[1] / "artifacts" / "nuclear_nrc_validation.parquet"
)
TOP_N: int = 20


def _normalize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = name.lower()
    n = re.sub(r"\([^)]*\)", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _excel_nuclear_plants(fleet: pd.DataFrame) -> pd.DataFrame:
    df = fleet[fleet["fuel_category"] == "Nuclear"].copy()
    df["norm_plant"] = df["plant"].map(_normalize)
    df["_cf_x_cap"] = df["cap_factor"] * df["summer_cap_mw"]
    agg = df.groupby("norm_plant", as_index=False).agg(
        excel_plant=("plant", "first"),
        excel_zone=("zone", lambda s: s.mode().iloc[0] if len(s) else ""),
        excel_summer_mw=("summer_cap_mw", "sum"),
        excel_units=("plant", "size"),
        excel_min_load_mw=("min_load_mw", "sum"),
        _cf_num=("_cf_x_cap", "sum"),
    )
    agg["excel_cap_factor"] = agg["_cf_num"] / agg["excel_summer_mw"].where(
        agg["excel_summer_mw"] > 0
    )
    return agg.drop(columns=["_cf_num"])


def _nrc_plant_metrics(pjm_nrc: pd.DataFrame) -> pd.DataFrame:
    """Aggregate NRC daily rows to plant-level 365-day metrics."""
    # Plant capacity from the crosswalk (sum of units at the plant)
    plant_cap = pd.DataFrame(
        [
            {"plant_id_eia": v["plant_id_eia"], "unit_capacity_mw": v["capacity_mw"]}
            for v in PJM_NRC_CROSSWALK.values()
        ]
    )
    plant_cap = plant_cap.groupby("plant_id_eia", as_index=False)[
        "unit_capacity_mw"
    ].sum()
    plant_cap = plant_cap.rename(columns={"unit_capacity_mw": "nrc_capacity_mw"})

    # Daily plant-level: sum effective_mw across units, average Power%
    plant_daily = pjm_nrc.groupby(["date", "plant_id_eia"], as_index=False).agg(
        effective_mw=("effective_mw", "sum"),
        avg_power_pct=("Power", "mean"),
    )
    plant_metrics = plant_daily.groupby("plant_id_eia", as_index=False).agg(
        n_days=("date", "size"),
        mean_effective_mw=("effective_mw", "mean"),
        mean_power_pct=("avg_power_pct", "mean"),
        p01_running_effective_mw=(
            "effective_mw",
            lambda s: float(np.percentile(s[s > 0], 1)) if (s > 0).any() else np.nan,
        ),
        days_at_zero=("effective_mw", lambda s: int((s <= 0).sum())),
    )
    plant_metrics = plant_metrics.merge(plant_cap, on="plant_id_eia", how="left")
    plant_metrics["nrc_cap_factor"] = (
        plant_metrics["mean_effective_mw"] / plant_metrics["nrc_capacity_mw"]
    )
    return plant_metrics


def _join_excel_to_nrc(excel: pd.DataFrame, nrc_metrics: pd.DataFrame) -> pd.DataFrame:
    """Map NRC plant_id_eia rows to Excel plant rows by normalized name.

    The crosswalk gives us plant_id -> EIA plant_name_eia via the
    helioscta-pjm-da repo's lookups; here we use the Excel plant names
    and the NRC ``Unit`` -> plant_id mapping. Simplest reliable join:
    use the family of unit names that share a plant_id, derive a
    canonical plant base name (e.g. "Peach Bottom 2" -> "peach bottom"),
    then merge to Excel by normalized base name.
    """
    plant_basenames = {}
    for unit_name, info in PJM_NRC_CROSSWALK.items():
        base = re.sub(r"\s+[0-9]+$", "", unit_name).lower().strip()
        plant_basenames.setdefault(info["plant_id_eia"], base)
    base_df = pd.DataFrame(
        [{"plant_id_eia": k, "norm_plant": v} for k, v in plant_basenames.items()]
    )
    nrc_named = nrc_metrics.merge(base_df, on="plant_id_eia", how="left")

    # Excel "Peach Bottom (York, PA)" -> "peach bottom" by _normalize already
    out = excel.merge(nrc_named, on="norm_plant", how="outer", indicator=True)
    return out


def run() -> dict:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)

    if not EXCEL_PARQUET.exists():
        raise FileNotFoundError(EXCEL_PARQUET)

    fleet = pd.read_parquet(EXCEL_PARQUET)
    excel_n = _excel_nuclear_plants(fleet)
    print(
        f"Excel nuclear plants: {len(excel_n):,} ({excel_n['excel_summer_mw'].sum() / 1000:.1f} GW summer cap)"
    )

    print("Pulling NRC daily reactor status (365-day rolling window) ...")
    nrc = pull_reactor_status()
    pjm_nrc = filter_pjm(nrc)
    print(
        f"  NRC PJM rows: {len(pjm_nrc):,}, "
        f"{pjm_nrc['Unit'].nunique()}/{len(PJM_NRC_CROSSWALK)} crosswalk units, "
        f"window {pjm_nrc['date'].min()} -> {pjm_nrc['date'].max()}"
    )
    print()

    nrc_metrics = _nrc_plant_metrics(pjm_nrc)
    merged = _join_excel_to_nrc(excel_n, nrc_metrics)

    matched = merged["_merge"] == "both"
    only_excel = merged["_merge"] == "left_only"
    only_nrc = merged["_merge"] == "right_only"

    print("=== Coverage ===")
    print(f"  matched plants: {matched.sum()} / {len(excel_n)} Excel nuclear")
    print(f"  Excel nuclear without NRC match: {only_excel.sum()}")
    print(f"  NRC plants without Excel match:  {only_nrc.sum()}")
    print()

    mm = merged.loc[matched].copy()
    mm["cap_factor_gap"] = mm["excel_cap_factor"] - mm["nrc_cap_factor"]
    mm["min_load_gap"] = mm["excel_min_load_mw"] - mm["p01_running_effective_mw"]

    # --- Cap factor ---------------------------------------------------
    print("=== Cap factor: Excel vs NRC 365-day ===")
    cf = mm.sort_values("cap_factor_gap", key=lambda s: s.abs(), ascending=False)
    print(
        cf[
            [
                "excel_plant",
                "excel_zone",
                "excel_summer_mw",
                "nrc_capacity_mw",
                "excel_cap_factor",
                "nrc_cap_factor",
                "cap_factor_gap",
                "n_days",
            ]
        ].to_string(
            index=False,
            formatters={
                "excel_summer_mw": "{:>8,.0f}".format,
                "nrc_capacity_mw": "{:>8,.0f}".format,
                "excel_cap_factor": "{:>6.2f}".format,
                "nrc_cap_factor": "{:>6.2f}".format,
                "cap_factor_gap": "{:>+6.2f}".format,
                "n_days": "{:>4,}".format,
            },
        )
    )
    print()
    print(f"  median |cap_factor_gap|: {mm['cap_factor_gap'].abs().median():.3f}")
    print()

    # --- Min load (the bug-finder) ------------------------------------
    print("=== Min-load gap: Excel vs NRC p01-running ===")
    print(
        "  ('p01_running' = 1st percentile of nonzero daily plant-effective MW;\n"
        "   for nuclear this should be ~nameplate -- when up, units are at 100%)"
    )
    ml = mm.sort_values("min_load_gap", key=lambda s: s.abs(), ascending=False)
    print(
        ml[
            [
                "excel_plant",
                "excel_zone",
                "excel_summer_mw",
                "excel_min_load_mw",
                "p01_running_effective_mw",
                "min_load_gap",
                "days_at_zero",
            ]
        ].to_string(
            index=False,
            formatters={
                "excel_summer_mw": "{:>8,.0f}".format,
                "excel_min_load_mw": "{:>9,.0f}".format,
                "p01_running_effective_mw": "{:>9,.0f}".format,
                "min_load_gap": "{:>+9,.0f}".format,
                "days_at_zero": "{:>4,}".format,
            },
        )
    )
    print()
    print(f"  median |min_load_gap|: {mm['min_load_gap'].abs().median():,.0f} MW")
    print(
        "  -> Negative gap means Excel under-states min_load; for nuclear, min_load_mw\n"
        "     should be near nameplate (no economic min-down), not 10-15%."
    )

    if not only_excel.empty:
        print()
        print("=== Excel nuclear plants without NRC match (sample) ===")
        print(
            merged.loc[only_excel, ["excel_plant", "excel_zone", "excel_summer_mw"]]
            .head(TOP_N)
            .to_string(index=False, formatters={"excel_summer_mw": "{:>8,.0f}".format})
        )

    OUT_VALIDATION_PARQUET.parent.mkdir(exist_ok=True)
    mm.to_parquet(OUT_VALIDATION_PARQUET, index=False)
    print(f"\nWrote {OUT_VALIDATION_PARQUET}")

    return {"merged": merged, "nrc_metrics": nrc_metrics}


if __name__ == "__main__":
    run()
