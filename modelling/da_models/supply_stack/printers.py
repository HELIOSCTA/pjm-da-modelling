"""Terminal report for the supply-stack forecast.

ASCII-only (``=`` / ``-`` / ``|`` separators, ``to_string`` tables) per
the python-scripts skill. Sections: FORECAST CONFIGURATION (fleet summary
+ every structural assumption, echoed for auditability) with the merit
order collapsed to one row per fuel category at day-mean gas; Hourly
Dispatch (per-HE clearing price / marginal fuel / heat rate / reserve
headroom / utilization / scarcity adder / P10 / P90 [/ Actual / Err]);
and Sanity Checks (CC implied-price anchor, marginal-fuel mix, scarcity
hours, MAE vs settled DA LMP).
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from da_models.supply_stack import configs as C
from da_models.supply_stack.stack.merit_order import (
    build_merit_order,
    merit_order_by_fuel,
)
from utils.logging_utils import print_divider, print_header, print_section

_ONPEAK_HOURS = list(range(8, 24))
_OFFPEAK_HOURS = list(range(1, 8)) + [24]


def _block_mean(by_he: dict[int, float], hours: list[int]) -> float:
    vals = [by_he[h] for h in hours if h in by_he and pd.notna(by_he[h])]
    return float(np.mean(vals)) if vals else float("nan")


def print_config(
    target_date: date, hub: str, fleet: pd.DataFrame, fleet_meta: dict, inputs: dict
) -> None:
    print_header("FORECAST CONFIGURATION", "=", 120)
    dow = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")[target_date.weekday()]
    print(f"\n  Target        {target_date} ({dow})")
    print(f"  Hub           {hub}   (gas sanity hub: {C.GAS_SANITY_HUB})")
    print(f"  Model         {C.MODEL_NAME}  (family: {C.MODEL_FAMILY})")
    print(
        "  Description   Structural per-unit merit-order: price = marg var cost x bid-markup + congestion + ramp + scarcity"
    )

    print_section("Fleet (per-unit)")
    print(
        f"  Source             {fleet_meta.get('source')}  ({fleet_meta.get('rating')} ratings)"
    )
    print(
        f"  Total stack        {fleet_meta.get('total_stack_mw'):,.0f} MW   "
        f"(thermal {fleet_meta.get('thermal_mw'):,.0f} MW, must-run {fleet_meta.get('must_run_mw'):,.0f} MW; renewables netted off load)"
    )
    by_fuel = fleet_meta.get("capacity_by_fuel_mw", {})
    print(
        "  Capacity by fuel   "
        + "  ".join(f"{k}={v / 1000:.1f}GW" for k, v in by_fuel.items())
    )
    print(
        f"  Outage derate      {inputs.get('outage_mw'):,.0f} MW  ->  available stack {inputs.get('available_stack_mw'):,.0f} MW"
    )
    by_he = inputs.get("operating_reserve_mw_by_he", {})
    spread = (
        f"  per-HE range {min(by_he.values()):,.0f}-{max(by_he.values()):,.0f} MW"
        if by_he
        else ""
    )
    print(
        f"  Operating reserves {inputs.get('operating_reserve_mw_mean', 0):,.0f} MW co-optimised away  ->  "
        f"energy-offered {inputs.get('energy_offered_mw'):,.0f} MW (utilization denominator){spread}"
    )

    print_section("Assumptions")
    print(
        f"  Gas (day mean @{C.GAS_SANITY_HUB})  ${inputs.get('gas_mean_usd_mmbtu'):.2f}/MMBtu   (vintage: {inputs.get('vintage')}; "
        f"other gas hubs + coal/oil/uranium from configs.FUEL_PRICES)"
    )
    print(
        f"  Carbon (RGGI)      ${C.CARBON_PRICE_USD_TON_CO2:.0f}/ton CO2 on RGGI-flagged units"
    )
    print(
        f"  Congestion adder   ${C.CONGESTION_ADDER_USD_MWH:.2f}/MWh   Price cap ${C.PRICE_CAP_USD_MWH:,.0f}/MWh"
    )
    print(
        "  Bid-stack markup   util-> "
        + "  ".join(f"{int(u * 100)}%:x{m:.2f}" for u, m in C.BID_MARKUP_BANDS)
    )
    print(
        "  Scarcity adder     util-> "
        + "  ".join(f"{int(u * 100)}%:+${a:,.0f}" for u, a in C.SCARCITY_BANDS)
    )
    ramp_nz = ", ".join(
        f"HE{i + 1}:+${v:.0f}" for i, v in enumerate(C.HOURLY_RAMP_ADDER_USD_MWH) if v
    )
    print(f"  Hour-of-day ramp   {ramp_nz}")
    print(
        f"  MC bands           {C.MC_DRAWS} draws  (load +/-{C.MC_LOAD_SIGMA_PCT:.0%}, "
        f"outage +/-{C.MC_FORCED_OUTAGE_SIGMA_MW:,.0f} MW, gas +/-{C.MC_GAS_SIGMA_PCT:.0%})"
    )

    print_section("Merit Order by fuel (at day-mean gas)")
    gas_mean = inputs.get("gas_mean_usd_mmbtu", 2.8)
    merit = build_merit_order(
        fleet,
        gas_prices_by_hub={C.GAS_SANITY_HUB: gas_mean},
        outage_mw=inputs.get("outage_mw", 0.0),
    )
    summ = merit_order_by_fuel(merit).copy()
    summ["mean_var_cost"] = summ["mean_var_cost"].map(lambda v: f"{v:>8.2f}")
    summ["min_var_cost"] = summ["min_var_cost"].map(lambda v: f"{v:>8.2f}")
    summ["max_var_cost"] = summ["max_var_cost"].map(lambda v: f"{v:>8.2f}")
    for c in ("derated_mw", "cum_mw"):
        summ[c] = summ[c].map(lambda v: f"{v:>12,.0f}")
    print(
        summ.rename(
            columns={
                "fuel_category": "Fuel",
                "units": "Units",
                "mean_var_cost": "MeanMC",
                "min_var_cost": "MinMC",
                "max_var_cost": "MaxMC",
                "derated_mw": "DeratedMW",
                "cum_mw": "CumMW",
                "must_run": "MustRun",
            }
        ).to_string(index=False)
    )
    print()
    print_divider("=", 120, dim=False)


def build_hourly_table(
    target_date: date, table: pd.DataFrame, actuals_hourly: dict[int, float] | None
) -> pd.DataFrame:  # noqa: ARG001
    """Tidy per-HE display frame: HE | NetLoad | $/MWh | MargUnit/Fuel | HR | Headroom | Util | Scarcity | P10 | P90 [| Actual | Err]."""
    df = table.copy()
    out = pd.DataFrame({"HE": df["hour_ending"].astype(int)})
    out["NetLoad_GW"] = (df.get("net_load_mw") / 1000.0).round(1)
    out["Clear_$"] = df.get("clearing_price").round(2)
    out["MargFuel"] = df.get("marginal_fuel")
    out["HR"] = df.get("marginal_heat_rate")
    out["Headroom_GW"] = (df.get("reserve_headroom_mw") / 1000.0).round(1)
    out["Util_%"] = (df.get("utilization") * 100.0).round(1)
    out["Scarcity_$"] = df.get("scarcity_adder").round(1)
    if "q_0.10" in df.columns:
        out["P10"] = df["q_0.10"].round(2)
    if "q_0.90" in df.columns:
        out["P90"] = df["q_0.90"].round(2)
    if actuals_hourly is not None:
        out["Actual"] = out["HE"].map(actuals_hourly)
        out["Err"] = (out["Clear_$"] - out["Actual"]).round(2)
    return out


def print_hourly(hourly: pd.DataFrame, table: pd.DataFrame) -> None:
    print_section("Hourly Dispatch ($/MWh)")
    fmt: dict = {
        c: (lambda v: "" if pd.isna(v) else f"{v:>9.2f}")
        for c in ("Clear_$", "P10", "P90", "Actual", "Err", "Scarcity_$")
    }
    fmt["HR"] = lambda v: "" if pd.isna(v) else f"{v:>5.1f}"
    fmt["NetLoad_GW"] = fmt["Headroom_GW"] = lambda v: (
        "" if pd.isna(v) else f"{v:>6.1f}"
    )
    fmt["Util_%"] = lambda v: "" if pd.isna(v) else f"{v:>6.1f}"
    print(hourly.to_string(index=False, formatters=fmt))
    by_he = dict(
        zip(
            table["hour_ending"].astype(int),
            table.get("clearing_price", pd.Series(dtype=float)),
        )
    )
    on, off, flat = (
        _block_mean(by_he, _ONPEAK_HOURS),
        _block_mean(by_he, _OFFPEAK_HOURS),
        _block_mean(by_he, list(range(1, 25))),
    )
    print(f"\n  OnPeak (HE8-23): ${on:.2f}    OffPeak: ${off:.2f}    Flat: ${flat:.2f}")
    print()


def print_sanity(
    table: pd.DataFrame, inputs: dict, actuals_hourly: dict[int, float] | None
) -> None:
    print_section("Sanity Checks")
    gas = inputs.get("gas_mean_usd_mmbtu")
    if gas:
        print(
            f"  CC implied-price anchor: {C.GAS_REF_HEAT_RATE} x ${gas:.2f} = ${C.GAS_REF_HEAT_RATE * gas:.2f}/MWh "
            "(an efficient CC on margin clears near here pre-markup/scarcity)"
        )
    if "marginal_fuel" in table.columns:
        print(
            f"  Marginal-fuel mix (24 HE): {table['marginal_fuel'].value_counts().to_dict()}"
        )
    if "marginal_block" in table.columns:
        print(f"  Marginal units seen: {sorted(set(table['marginal_block'].dropna()))}")
    print(
        f"  Hours with a scarcity adder: {int((table.get('scarcity_adder', pd.Series(dtype=float)) > 0).sum())}/24"
    )
    if actuals_hourly is not None and "clearing_price" in table.columns:
        merged = table[table["hour_ending"].isin(actuals_hourly)].copy()
        merged["actual"] = merged["hour_ending"].map(actuals_hourly)
        merged = merged.dropna(subset=["actual", "clearing_price"])
        if not merged.empty:
            err = merged["clearing_price"] - merged["actual"]
            print(
                f"  vs settled DA LMP: MAE ${err.abs().mean():.2f}  bias ${err.mean():+.2f}  (n={len(merged)} HE)"
            )
    print()
