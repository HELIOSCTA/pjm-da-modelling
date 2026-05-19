"""Verify pjm_fleet.parquet against PUDL EIA-860 (identity layer).

Compares the canonical Excel-built ``pjm_fleet.parquet`` to the PUDL-
built ``artifacts/pjm_fleet_pudl.parquet``. PUDL is the authoritative
fleet dimension (EIA-860 / EIA-923), so this is the high-coverage
counterpart to ``verify_vs_yes_energy_identity.py`` -- expect ~90%+
plant matches instead of yes_energy's ~1.4%.

What this reports:

- Name-match coverage (Excel ``plant`` -> PUDL ``plant_name_eia``)
- Plant-level capacity reconciliation (summer_cap_mw)
- Fuel category contingency (Excel ``fuel_category`` x PUDL ``fuel_category``)
- Zone contingency (Excel ``zone`` x PUDL ``zone``)
- Unmatched-on-each-side samples for spot-checking

Prereq: run ``builders/build_from_pudl.py`` first to produce
``artifacts/pjm_fleet_pudl.parquet``.

Usage::

    python -m da_models.supply_stack.data.validators.verify_vs_pudl_identity
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[5]
_MODELLING_ROOT = Path(__file__).resolve().parents[4]
for _p in (_MODELLING_ROOT, _REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import pandas as pd  # noqa: E402

EXCEL_PARQUET: Path = Path(__file__).resolve().parents[1] / "pjm_fleet.parquet"
PUDL_PARQUET: Path = (
    Path(__file__).resolve().parents[1] / "artifacts" / "pjm_fleet_pudl.parquet"
)
TOP_N: int = 20

# Substrings stripped before name matching. Vendor prefixes and
# unit-class suffixes that don't appear in EIA naming.
_PREFIX_NOISE: tuple[str, ...] = (
    "talenenergy ",
    "pseg ",
    "constellation ",
    "exelon ",
    "nrg ",
    "lsp ",
    "aes ",
    "calpine ",
)
_SUFFIX_NOISE: tuple[str, ...] = (
    " nuclear power plant",
    " generating station",
    " generation station",
    " power station",
    " power plant",
    " energy center",
    " generating plant",
    " power llc",
    " llc",
    " lp",
    " station",
)


def _normalize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = name.lower()
    n = re.sub(r"\([^)]*\)", "", n)  # drop "(County, ST)" qualifiers
    n = re.sub(r"\s+", " ", n).strip()
    for pref in _PREFIX_NOISE:
        if n.startswith(pref):
            n = n[len(pref) :].strip()
    for suf in _SUFFIX_NOISE:
        if n.endswith(suf):
            n = n[: -len(suf)].strip()
    return n


def _plant_level_excel(fleet: pd.DataFrame) -> pd.DataFrame:
    """Roll Excel per-unit rows up to plant level (capacity-weighted)."""
    df = fleet.copy()
    df["norm_plant"] = df["plant"].map(_normalize)
    df["_hr_x_cap"] = df["heat_rate_mmbtu_mwh"] * df["summer_cap_mw"]
    agg = df.groupby("norm_plant", as_index=False).agg(
        excel_plant=("plant", "first"),
        excel_summer_mw=("summer_cap_mw", "sum"),
        excel_units=("plant", "size"),
        excel_fuel_categories=("fuel_category", lambda s: ",".join(sorted(set(s)))),
        excel_zones=("zone", lambda s: ",".join(sorted(set(s)))),
        excel_dominant_fuel=(
            "fuel_category",
            lambda s: s.mode().iloc[0] if len(s) else "",
        ),
        excel_dominant_zone=("zone", lambda s: s.mode().iloc[0] if len(s) else ""),
        _hr_num=("_hr_x_cap", "sum"),
    )
    agg["excel_heat_rate"] = agg["_hr_num"] / agg["excel_summer_mw"].where(
        agg["excel_summer_mw"] > 0
    )
    return agg.drop(columns=["_hr_num"])


def _plant_level_pudl(pudl: pd.DataFrame) -> pd.DataFrame:
    """Roll PUDL per-generator rows up to plant level."""
    df = pudl.copy()
    df["norm_plant"] = df["plant"].map(_normalize)
    df["_hr_x_cap"] = df["heat_rate_mmbtu_mwh"] * df["summer_cap_mw"]
    agg = df.groupby("norm_plant", as_index=False).agg(
        pudl_plant=("plant", "first"),
        plant_id_eia=("plant_id_eia", "first"),
        pudl_summer_mw=("summer_cap_mw", "sum"),
        pudl_units=("plant", "size"),
        pudl_dominant_fuel=(
            "fuel_category",
            lambda s: s.mode().iloc[0] if len(s) else "",
        ),
        pudl_zone=("zone", lambda s: s.mode().iloc[0] if len(s) else ""),
        _hr_num=("_hr_x_cap", "sum"),
    )
    agg["pudl_heat_rate"] = agg["_hr_num"] / agg["pudl_summer_mw"].where(
        agg["pudl_summer_mw"] > 0
    )
    return agg.drop(columns=["_hr_num"])


def run() -> dict:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)

    if not EXCEL_PARQUET.exists():
        raise FileNotFoundError(f"Excel fleet parquet not found: {EXCEL_PARQUET}")
    if not PUDL_PARQUET.exists():
        raise FileNotFoundError(
            f"PUDL fleet parquet not found: {PUDL_PARQUET}\n"
            "Run: python -m da_models.supply_stack.data.builders.build_from_pudl"
        )

    excel = pd.read_parquet(EXCEL_PARQUET)
    pudl = pd.read_parquet(PUDL_PARQUET)
    print(f"Excel fleet:  {len(excel):,} rows, {excel['plant'].nunique():,} plants")
    print(f"PUDL fleet:   {len(pudl):,} rows, {pudl['plant'].nunique():,} plants")
    print()

    e_plant = _plant_level_excel(excel)
    p_plant = _plant_level_pudl(pudl)

    merged = e_plant.merge(p_plant, on="norm_plant", how="outer", indicator=True)
    matched = merged["_merge"] == "both"
    only_excel = merged["_merge"] == "left_only"
    only_pudl = merged["_merge"] == "right_only"

    print("=== Coverage ===")
    print(f"  plants in both:        {matched.sum():>5,}")
    print(f"  Excel-only:            {only_excel.sum():>5,}")
    print(f"  PUDL-only:             {only_pudl.sum():>5,}")
    print(
        f"  Excel match rate:      {matched.sum() / (matched.sum() + only_excel.sum()) * 100:.1f}%"
    )
    print(
        f"  PUDL match rate:       {matched.sum() / (matched.sum() + only_pudl.sum()) * 100:.1f}%"
    )
    print()

    # --- Capacity reconciliation -------------------------------------
    mm = merged.loc[matched].copy()
    mm["summer_minus_pudl"] = mm["excel_summer_mw"] - mm["pudl_summer_mw"]
    mm["pct_gap"] = (
        mm["summer_minus_pudl"]
        / mm["pudl_summer_mw"].where(mm["pudl_summer_mw"] > 0)
        * 100
    )

    print(f"=== Capacity gap (top {TOP_N} by |gap| MW) ===")
    top = (
        mm.assign(_abs=mm["summer_minus_pudl"].abs())
        .sort_values("_abs", ascending=False)
        .drop(columns="_abs")
        .head(TOP_N)
    )
    print(
        top[
            [
                "excel_plant",
                "pudl_plant",
                "plant_id_eia",
                "excel_summer_mw",
                "pudl_summer_mw",
                "summer_minus_pudl",
                "pct_gap",
                "excel_units",
                "pudl_units",
            ]
        ].to_string(
            index=False,
            formatters={
                "excel_summer_mw": "{:>8,.0f}".format,
                "pudl_summer_mw": "{:>8,.0f}".format,
                "summer_minus_pudl": "{:>8,.0f}".format,
                "pct_gap": "{:>6,.1f}".format,
                "plant_id_eia": "{:>6.0f}".format,
                "excel_units": "{:>4,}".format,
                "pudl_units": "{:>4,}".format,
            },
        )
    )
    print()
    print(
        f"  median |gap|: {mm['summer_minus_pudl'].abs().median():,.0f} MW   "
        f"median |pct_gap|: {mm['pct_gap'].abs().median():.1f}%   "
        f"plants with |pct_gap| > 20%: {(mm['pct_gap'].abs() > 20).sum()} / {len(mm)}"
    )
    print()

    # --- Fuel category contingency -----------------------------------
    print("=== Fuel category contingency (matched plants) ===")
    fc = pd.crosstab(mm["excel_dominant_fuel"], mm["pudl_dominant_fuel"])
    print(fc.to_string())
    print()

    # --- Zone contingency --------------------------------------------
    print("=== Zone contingency (matched plants, blanks = unmapped TD owner) ===")
    zc = pd.crosstab(
        mm["excel_dominant_zone"], mm["pudl_zone"].replace("", "(unmapped)")
    )
    print(zc.to_string())
    print()

    # --- Unmatched samples -------------------------------------------
    print(f"=== Excel-only plants (top {TOP_N} by capacity) ===")
    ex_only = (
        merged.loc[only_excel]
        .sort_values("excel_summer_mw", ascending=False)
        .head(TOP_N)
    )
    print(
        ex_only[
            [
                "excel_plant",
                "excel_summer_mw",
                "excel_dominant_fuel",
                "excel_dominant_zone",
                "excel_units",
            ]
        ].to_string(
            index=False,
            formatters={
                "excel_summer_mw": "{:>8,.0f}".format,
                "excel_units": "{:>4,}".format,
            },
        )
    )
    print()

    print(f"=== PUDL-only plants (top {TOP_N} by capacity) ===")
    pu_only = (
        merged.loc[only_pudl].sort_values("pudl_summer_mw", ascending=False).head(TOP_N)
    )
    print(
        pu_only[
            [
                "pudl_plant",
                "plant_id_eia",
                "pudl_summer_mw",
                "pudl_dominant_fuel",
                "pudl_zone",
                "pudl_units",
            ]
        ].to_string(
            index=False,
            formatters={
                "pudl_summer_mw": "{:>8,.0f}".format,
                "plant_id_eia": "{:>6.0f}".format,
                "pudl_units": "{:>4,}".format,
            },
        )
    )

    return {"merged": merged, "matched": mm}


if __name__ == "__main__":
    run()
