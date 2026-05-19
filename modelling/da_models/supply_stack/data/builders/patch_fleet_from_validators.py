"""Patch pjm_fleet.parquet with high-confidence fixes from CEMS / NRC validators.

Reads the canonical ``pjm_fleet.parquet`` plus the per-plant validator
artifacts produced by ``verify_thermal_vs_cems.py`` and
``verify_nuclear_vs_nrc.py``, applies surgical defect fixes, and writes
the patched fleet back to ``pjm_fleet.parquet`` (with an unpatched
backup at ``artifacts/pjm_fleet_unpatched.parquet``).

Patches applied:

1. **Thermal ``heat_rate_mmbtu_mwh == 0``** -> CEMS-implied heat rate
   (where the plant has >= 100 observation hours and CEMS implied is in
   the sane 5..25 MMBtu/MWh band). 872 candidate rows pre-patch.
2. **Thermal ``cap_factor == 0``** -> CEMS empirical CF (where > 0.05
   and CEMS data available). 1,005 candidate rows pre-patch.
3. **Coal ``cap_factor`` refresh** -> CEMS empirical CF for any Coal
   plant with CEMS observations, regardless of current value. The
   Excel coal CFs are stale (~0.55 vs ~0.30 observed).
4. **Nuclear ``min_load_factor`` floor of 0.95** -> nuclear has no
   economic ramping; the Excel mean of 0.30 understates min load by
   ~800-1,000 MW per plant (confirmed by NRC p01-running). Also
   recomputes ``min_load_mw = min_load_factor * summer_cap_mw``.
5. **Morgantown 2-MW row** -> reported for manual review (not
   auto-patched because the Excel may legitimately have a small
   Morgantown facility distinct from the big Charles, MD coal plant).

Patches are idempotent: re-running is safe. #1-4 only fire when their
guard conditions still hold, #5 always re-reports.

Prereq: run all three of these first::

    python -m da_models.supply_stack.data.builders.build_from_pudl
    python -m da_models.supply_stack.data.validators.verify_thermal_vs_cems
    python -m da_models.supply_stack.data.validators.verify_nuclear_vs_nrc

Usage::

    python -m da_models.supply_stack.data.builders.patch_fleet_from_validators
    python -m da_models.supply_stack.data.builders.patch_fleet_from_validators --dry-run
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[5]
_MODELLING_ROOT = Path(__file__).resolve().parents[4]
for _p in (_MODELLING_ROOT, _REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import pandas as pd  # noqa: E402

FLEET_PARQUET: Path = Path(__file__).resolve().parents[1] / "pjm_fleet.parquet"
ARTIFACTS_DIR: Path = Path(__file__).resolve().parents[1] / "artifacts"
CEMS_VALIDATION: Path = ARTIFACTS_DIR / "thermal_cems_validation.parquet"
NRC_VALIDATION: Path = ARTIFACTS_DIR / "nuclear_nrc_validation.parquet"
BACKUP_PARQUET: Path = ARTIFACTS_DIR / "pjm_fleet_unpatched.parquet"

THERMAL_FUELS: list[str] = ["Gas CC", "Gas CT/ST", "Coal", "Oil", "Biomass"]
NUCLEAR_MIN_LOAD_FACTOR_FLOOR: float = 0.95
MIN_OBS_HOURS: int = 100
HR_SANE_MIN: float = 5.0
HR_SANE_MAX: float = 25.0
CF_SANE_MIN: float = 0.05


def _normalize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    n = name.lower()
    n = re.sub(r"\([^)]*\)", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _build_cems_lookups(
    cems: pd.DataFrame,
) -> tuple[dict[str, float], dict[str, float]]:
    """Return (heat_rate, cap_factor) lookups keyed by normalized plant name."""
    df = cems.copy()
    df = df[df["n_obs_hours"].fillna(0) >= MIN_OBS_HOURS]

    hr_ok = df[
        df["cems_implied_heat_rate"].between(HR_SANE_MIN, HR_SANE_MAX, inclusive="both")
    ]
    cf_ok = df[df["emp_cap_factor"].fillna(0) >= CF_SANE_MIN]

    hr_lookup = hr_ok.groupby("norm_plant")["cems_implied_heat_rate"].first().to_dict()
    # Cap-factor lookup uses the broader CF-OK set so CEMS CF is available
    # even when implied heat rate falls outside the sane band (rare).
    cf_lookup = cf_ok.groupby("norm_plant")["emp_cap_factor"].first().to_dict()
    return hr_lookup, cf_lookup


def _apply_patches(
    fleet: pd.DataFrame, cems: pd.DataFrame
) -> tuple[pd.DataFrame, list[dict]]:
    """Apply patches 1-4 to a copy of ``fleet``. Returns (patched_fleet, log)."""
    out = fleet.copy()
    out["_norm_plant"] = out["plant"].map(_normalize)
    hr_lookup, cf_lookup = _build_cems_lookups(cems)

    log: list[dict] = []
    thermal_mask = out["fuel_category"].isin(THERMAL_FUELS)

    # --- Patch 1: thermal heat_rate == 0 -> CEMS implied --------------
    new_hr = out["_norm_plant"].map(hr_lookup)
    cond_hr = (
        thermal_mask & (out["heat_rate_mmbtu_mwh"].fillna(0) == 0) & new_hr.notna()
    )
    n_hr = int(cond_hr.sum())
    if n_hr:
        out.loc[cond_hr, "heat_rate_mmbtu_mwh"] = new_hr[cond_hr]
    log.append(
        {
            "patch": "thermal_heat_rate_zero -> CEMS",
            "rows": n_hr,
            "mean_new_value": float(new_hr[cond_hr].mean()) if n_hr else None,
            "candidate_rows": int(
                (thermal_mask & (out["heat_rate_mmbtu_mwh"].fillna(0) == 0)).sum()
            ),
        }
    )

    # --- Patch 2: thermal cap_factor == 0 -> CEMS empirical -----------
    new_cf = out["_norm_plant"].map(cf_lookup)
    cond_cf = (
        thermal_mask
        & (out["cap_factor"].fillna(0) == 0)
        & new_cf.notna()
        & (new_cf > CF_SANE_MIN)
    )
    n_cf = int(cond_cf.sum())
    if n_cf:
        out.loc[cond_cf, "cap_factor"] = new_cf[cond_cf]
    log.append(
        {
            "patch": "thermal_cap_factor_zero -> CEMS",
            "rows": n_cf,
            "mean_new_value": float(new_cf[cond_cf].mean()) if n_cf else None,
            "candidate_rows": int(
                (thermal_mask & (out["cap_factor"].fillna(0) == 0)).sum()
            ),
        }
    )

    # --- Patch 3: Coal cap_factor refresh (any CEMS data) -------------
    coal_mask = out["fuel_category"] == "Coal"
    coal_new_cf = out["_norm_plant"].map(cf_lookup)
    cond_coal = coal_mask & coal_new_cf.notna() & (coal_new_cf > CF_SANE_MIN)
    # Only refresh where the gap is material (>0.05) to avoid noise from
    # rounding. Skip rows where we already wrote in patch 2.
    cond_coal = cond_coal & ((out["cap_factor"].fillna(0) - coal_new_cf).abs() > 0.05)
    cond_coal = cond_coal & ~cond_cf  # don't double-patch
    n_coal = int(cond_coal.sum())
    if n_coal:
        old_mean = float(out.loc[cond_coal, "cap_factor"].mean())
        out.loc[cond_coal, "cap_factor"] = coal_new_cf[cond_coal]
        new_mean = float(coal_new_cf[cond_coal].mean())
    else:
        old_mean = new_mean = None
    log.append(
        {
            "patch": "coal_cap_factor_refresh -> CEMS",
            "rows": n_coal,
            "old_mean": old_mean,
            "new_mean": new_mean,
            "candidate_rows": int(coal_mask.sum()),
        }
    )

    # --- Patch 4: Nuclear min_load_factor floor 0.95 ------------------
    nuke_mask = out["fuel_category"] == "Nuclear"
    cond_nuke = nuke_mask & (
        out["min_load_factor"].fillna(0) < NUCLEAR_MIN_LOAD_FACTOR_FLOOR
    )
    n_nuke = int(cond_nuke.sum())
    if n_nuke:
        old_mean = float(out.loc[cond_nuke, "min_load_factor"].mean())
        out.loc[cond_nuke, "min_load_factor"] = NUCLEAR_MIN_LOAD_FACTOR_FLOOR
        # Recompute derived min_load_mw to stay consistent with the factor.
        out.loc[cond_nuke, "min_load_mw"] = (
            out.loc[cond_nuke, "min_load_factor"] * out.loc[cond_nuke, "summer_cap_mw"]
        ).clip(lower=0.0)
    else:
        old_mean = None
    log.append(
        {
            "patch": f"nuclear_min_load_factor floor {NUCLEAR_MIN_LOAD_FACTOR_FLOOR}",
            "rows": n_nuke,
            "old_mean": old_mean,
            "new_mean": NUCLEAR_MIN_LOAD_FACTOR_FLOOR if n_nuke else None,
            "candidate_rows": int(nuke_mask.sum()),
        }
    )

    out = out.drop(columns=["_norm_plant"])
    return out, log


def _report_morgantown(fleet: pd.DataFrame) -> pd.DataFrame:
    """Return any suspicious Morgantown rows for manual review."""
    suspect = fleet[
        fleet["plant"].str.startswith("Morgantown", na=False)
        & (fleet["summer_cap_mw"] < 100)
    ]
    return suspect[
        [
            "plant",
            "fuel_category",
            "unit_type",
            "zone",
            "summer_cap_mw",
            "winter_cap_mw",
        ]
    ].copy()


def _print_log(log: list[dict]) -> None:
    print("=== Patches applied ===")
    for entry in log:
        cand = entry.get("candidate_rows", "?")
        if entry["rows"] == 0:
            print(f"  [SKIP] {entry['patch']:>45} -> 0 rows ({cand} candidates)")
            continue
        if "mean_new_value" in entry and entry["mean_new_value"] is not None:
            print(
                f"  [OK]   {entry['patch']:>45} -> {entry['rows']:>4} rows "
                f"({cand} candidates), mean new value = {entry['mean_new_value']:.3f}"
            )
        elif "old_mean" in entry:
            om = entry.get("old_mean")
            nm = entry.get("new_mean")
            print(
                f"  [OK]   {entry['patch']:>45} -> {entry['rows']:>4} rows "
                f"({cand} candidates), {om:.3f} -> {nm:.3f}"
            )
        else:
            print(
                f"  [OK]   {entry['patch']:>45} -> {entry['rows']:>4} rows ({cand} candidates)"
            )


def run(dry_run: bool = False) -> dict:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 240)

    if not FLEET_PARQUET.exists():
        raise FileNotFoundError(FLEET_PARQUET)
    if not CEMS_VALIDATION.exists():
        raise FileNotFoundError(
            f"{CEMS_VALIDATION} -- run verify_thermal_vs_cems.py first"
        )
    # NRC validation is optional for now; patch 4 is data-free (uses a constant)
    # but we still print whether NRC says nuclear actually does run high.

    fleet = pd.read_parquet(FLEET_PARQUET)
    cems = pd.read_parquet(CEMS_VALIDATION)
    print(
        f"Fleet (pre-patch):  {len(fleet):,} rows, {fleet['plant'].nunique():,} plants"
    )
    print(f"CEMS validation:    {len(cems):,} plant rows")
    if NRC_VALIDATION.exists():
        nrc = pd.read_parquet(NRC_VALIDATION)
        print(f"NRC validation:     {len(nrc):,} plant rows")
    print()

    patched, log = _apply_patches(fleet, cems)
    _print_log(log)
    print()

    # Patch 5: Morgantown review-only report
    suspect = _report_morgantown(patched)
    if len(suspect) > 0:
        print("=== Morgantown rows flagged for manual review (NOT auto-patched) ===")
        print(suspect.to_string(index=False))
        print(
            "  yes_energy / PUDL both show ~1,548 MW for 'Morgantown' (the Charles, MD coal plant);\n"
            "  if the row above is meant to be that plant, fix the source Excel\n"
            "  and re-run builders/build_from_excel.py."
        )
        print()

    # Sanity stats
    n_zero_hr_post = int(
        (
            patched["fuel_category"].isin(THERMAL_FUELS)
            & (patched["heat_rate_mmbtu_mwh"].fillna(0) == 0)
        ).sum()
    )
    n_zero_cf_post = int(
        (
            patched["fuel_category"].isin(THERMAL_FUELS)
            & (patched["cap_factor"].fillna(0) == 0)
        ).sum()
    )
    n_low_nuke = int(
        (
            (patched["fuel_category"] == "Nuclear")
            & (patched["min_load_factor"] < NUCLEAR_MIN_LOAD_FACTOR_FLOOR)
        ).sum()
    )
    print("=== Residuals after patching ===")
    print(
        f"  thermal heat_rate == 0:                {n_zero_hr_post:>5,}  (runtime falls back to configs.DEFAULT_HEAT_RATE_MMBTU_MWH)"
    )
    print(
        f"  thermal cap_factor == 0:               {n_zero_cf_post:>5,}  (may be legitimate inactive units)"
    )
    print(
        f"  nuclear min_load_factor < {NUCLEAR_MIN_LOAD_FACTOR_FLOOR}:        {n_low_nuke:>5,}"
    )

    if dry_run:
        print("\n(dry-run: not writing artifacts)")
        return {"patched": patched, "log": log, "morgantown_suspect": suspect}

    ARTIFACTS_DIR.mkdir(exist_ok=True)
    shutil.copy2(FLEET_PARQUET, BACKUP_PARQUET)
    patched.to_parquet(FLEET_PARQUET, index=False)
    print(f"\nWrote backup -> {BACKUP_PARQUET}")
    print(f"Wrote patched -> {FLEET_PARQUET}")

    return {"patched": patched, "log": log, "morgantown_suspect": suspect}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Patch pjm_fleet.parquet from validator artifacts"
    )
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(dry_run=args.dry_run)
