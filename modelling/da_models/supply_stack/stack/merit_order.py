"""Build the hourly cost-ordered, outage-derated supply stack (per-unit).

Variable cost per unit = ``heat_rate * fuel_price + VOM + carbon_cost``,
where ``fuel_price`` is the hour's live gas price for gas units (the 4
scraped hubs; other gas hubs and all coal/oil/uranium use the
``configs.FUEL_PRICES`` constants) and ``carbon_cost`` is the static
RGGI term precomputed in ``fleet.py``. The aggregate outage MW is
allocated pro-rata across the dispatchable (non-must-run) units by
capacity. Must-run units (nuclear) sit at the bottom regardless of cost;
the rest sort ascending by variable cost. Returns the stack with
cumulative derated capacity, ready for ``dispatch``.

Vectorised (numpy) because the Monte-Carlo bands call it ~MC_DRAWS x 24
times on a ~4k-unit fleet.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from da_models.supply_stack import configs as C


def _resolve_fuel_prices(
    fleet: pd.DataFrame, gas_prices_by_hub: dict[str, float]
) -> np.ndarray:
    """$/MMBtu per unit, given the hour's live gas prices."""
    # Excel-hub name -> live price, for the hubs we scrape.
    live_by_hubname = {
        C.LIVE_GAS_HUB_MAP[col]: float(p)
        for col, p in gas_prices_by_hub.items()
        if col in C.LIVE_GAS_HUB_MAP and p is not None
    }
    default_gas = live_by_hubname.get("Tetco M3") or (
        float(np.mean(list(gas_prices_by_hub.values())))
        if gas_prices_by_hub
        else C.FUEL_PRICES_USD_MMBTU["Tetco M3"]
    )
    out = np.zeros(len(fleet), dtype=float)
    for i, (kind, hub) in enumerate(
        zip(fleet["fuel_kind"].to_numpy(), fleet["fuel_hub"].fillna("").to_numpy())
    ):
        if kind == "none":
            out[i] = 0.0
        elif kind == "gas":
            out[i] = (
                live_by_hubname.get(hub)
                or C.FUEL_PRICES_USD_MMBTU.get(hub)
                or default_gas
            )
        else:  # coal / oil / nuclear -- static constants
            out[i] = C.FUEL_PRICES_USD_MMBTU.get(
                hub, default_gas if kind == "gas" else 0.0
            )
    return out


def build_merit_order(
    fleet: pd.DataFrame,
    *,
    gas_prices_by_hub: dict[str, float],
    outage_mw: float,
) -> pd.DataFrame:
    """Cost-ordered, derated stack for one hour.

    Columns: ``plant, fuel_category, fuel_kind, var_cost, heat_rate,
    nameplate_mw, outage_mw, derated_capacity_mw, cum_capacity_mw, must_run``.
    """
    fp = _resolve_fuel_prices(fleet, gas_prices_by_hub)
    hr = fleet["heat_rate"].to_numpy(dtype=float)
    vom = fleet["vom"].to_numpy(dtype=float)
    carbon = fleet["carbon_cost"].to_numpy(dtype=float)
    var_cost = hr * fp + vom + carbon
    nameplate = fleet["capacity_mw"].to_numpy(dtype=float)
    must_run = fleet["must_run"].to_numpy(dtype=bool)

    # Pro-rata outage allocation across dispatchable (non-must-run) units.
    outage_alloc = np.zeros(len(fleet), dtype=float)
    disp_total = float(nameplate[~must_run].sum())
    if disp_total > 0.0 and outage_mw > 0.0:
        outage_alloc[~must_run] = np.minimum(
            nameplate[~must_run] / disp_total * float(outage_mw), nameplate[~must_run]
        )
    derated = np.clip(nameplate - outage_alloc, 0.0, None)

    # Order: must-run first (own cost order), then dispatchable ascending by cost.
    mr_idx = np.where(must_run)[0]
    dp_idx = np.where(~must_run)[0]
    mr_idx = mr_idx[np.argsort(var_cost[mr_idx], kind="stable")]
    dp_idx = dp_idx[np.argsort(var_cost[dp_idx], kind="stable")]
    order = np.concatenate([mr_idx, dp_idx])

    out = pd.DataFrame(
        {
            "plant": fleet["plant"].to_numpy()[order],
            "fuel_category": fleet["fuel_category"].to_numpy()[order],
            "fuel_kind": fleet["fuel_kind"].to_numpy()[order],
            "var_cost": var_cost[order],
            "heat_rate": hr[order],
            "nameplate_mw": nameplate[order],
            "outage_mw": outage_alloc[order],
            "derated_capacity_mw": derated[order],
            "must_run": must_run[order],
        }
    )
    out["cum_capacity_mw"] = out["derated_capacity_mw"].cumsum()
    return out


def total_derated_capacity(merit: pd.DataFrame) -> float:
    return float(merit["derated_capacity_mw"].sum())


def assert_valid_stack(merit: pd.DataFrame) -> None:
    """Layer-2 stack-mechanics invariants (cheap run-time guard)."""
    disp = merit[~merit["must_run"]]
    assert disp["var_cost"].is_monotonic_increasing, (
        "dispatchable units not cost-ordered"
    )
    assert merit["cum_capacity_mw"].is_monotonic_increasing, (
        "cumulative capacity not monotonic"
    )
    assert (merit["derated_capacity_mw"] >= -1e-6).all(), "negative derated capacity"
    assert (merit["derated_capacity_mw"] <= merit["nameplate_mw"] + 1e-6).all(), (
        "derated exceeds nameplate"
    )
    assert np.isfinite(merit["var_cost"]).all(), "non-finite variable cost"


def merit_order_by_fuel(merit: pd.DataFrame) -> pd.DataFrame:
    """Collapse the unit-level stack to one row per fuel category (for display):
    capacity-weighted mean var cost, total derated MW, cumulative MW at the
    block's top edge."""
    rows: list[dict] = []
    cum = 0.0
    # Preserve the stack's order: group by fuel_category in first-appearance order.
    for fc in merit["fuel_category"].drop_duplicates():
        # Note: a fuel category can appear in more than one stack segment; this
        # groups all its units (good enough for a summary).
        g = merit[merit["fuel_category"] == fc]
        cap = float(g["derated_capacity_mw"].sum())
        w = g["derated_capacity_mw"].to_numpy()
        vc = g["var_cost"].to_numpy()
        wmean = float(np.average(vc, weights=w)) if w.sum() > 0 else float(vc.mean())
        cum += cap
        rows.append(
            {
                "fuel_category": fc,
                "units": len(g),
                "mean_var_cost": wmean,
                "min_var_cost": float(vc.min()),
                "max_var_cost": float(vc.max()),
                "derated_mw": cap,
                "cum_mw": cum,
                "must_run": bool(g["must_run"].any()),
            }
        )
    return pd.DataFrame(rows)
