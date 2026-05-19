"""Dispatch net load against a merit-order stack -> clearing price.

Walk the cost-ordered stack until cumulative derated capacity covers net
load; the block you stop on is marginal. Clearing price =
``marginal_var_cost * bid_markup(utilization) + congestion_adder +
scarcity_adder(utilization)``. The bid-markup curve lifts the price above
pure marginal cost (generators bid up; the bid stack is convex --
Coulon-Howison) and grows with how tight the stack is. The scarcity adder
escalates near/above full utilization (the last ``SCARCITY_BANDS`` knot,
>=1.0, is the shortage/VOLL proxy, so net load above the stack still
produces a finite large price). Clamped at ``PRICE_CAP_USD_MWH``.
``utilization`` = net_load / total derated capacity.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from da_models.supply_stack import configs as C
from da_models.supply_stack.stack.merit_order import total_derated_capacity


@dataclass
class DispatchResult:
    clearing_price: float
    marginal_block: str
    marginal_fuel: str
    marginal_var_cost: float
    marginal_heat_rate: float | None
    energy_price: float  # marginal var cost * bid_markup (no congestion/scarcity)
    bid_markup: float
    congestion_adder: float
    scarcity_adder: float
    utilization: float
    reserve_headroom_mw: float
    stack_position_pct: float  # clipped 0-100


def _interp_bands(x: float, knots: tuple[tuple[float, float], ...]) -> float:
    """Piecewise-linear over (x, y) knots; flat below the first / at-or-above the last."""
    if x <= knots[0][0]:
        return knots[0][1]
    if x >= knots[-1][0]:
        return knots[-1][1]
    for (x0, y0), (x1, y1) in zip(knots[:-1], knots[1:]):
        if x0 <= x <= x1:
            frac = (x - x0) / (x1 - x0) if x1 > x0 else 0.0
            return y0 + frac * (y1 - y0)
    return knots[-1][1]


def dispatch(
    merit: pd.DataFrame,
    net_load_mw: float,
    *,
    operating_reserve_mw: float = C.OPERATING_RESERVE_MW_FALLBACK,
    congestion_adder: float = C.CONGESTION_ADDER_USD_MWH,
) -> DispatchResult:
    total_cap = total_derated_capacity(merit)
    # Energy-offered capacity = total derated minus the operating-reserve
    # requirement co-optimised away from energy. Drives utilization (which
    # feeds bid-markup and scarcity); the marginal-unit lookup still walks
    # the full stack -- physically those units exist, the haircut is about
    # what's *offered* into the energy market. ``operating_reserve_mw`` is
    # per-hour: historical from the dbt mart, forward from a rolling
    # profile, fallback constant via ``configs.OPERATING_RESERVE_MW_FALLBACK``.
    energy_cap = max(total_cap - operating_reserve_mw, 1.0)
    net_load_mw = max(float(net_load_mw), 0.0)

    # Marginal unit: first whose cumulative derated capacity covers net load
    # (numpy searchsorted on the cumsum -- fast for the ~4k-unit stack).
    cum = merit["cum_capacity_mw"].to_numpy()
    pos = int(np.searchsorted(cum, net_load_mw, side="left"))
    pos = min(pos, len(merit) - 1)
    marg = merit.iloc[pos]

    var_cost = float(marg["var_cost"])
    utilization = net_load_mw / energy_cap
    markup = _interp_bands(utilization, C.BID_MARKUP_BANDS)
    scarcity = _interp_bands(utilization, C.SCARCITY_BANDS)
    energy_price = var_cost * markup
    price = min(energy_price + congestion_adder + scarcity, C.PRICE_CAP_USD_MWH)

    hr = marg.get("heat_rate")
    hr = float(hr) if hr is not None and pd.notna(hr) else None

    return DispatchResult(
        clearing_price=float(price),
        marginal_block=str(marg["plant"]),
        marginal_fuel=str(marg["fuel_category"]),
        marginal_var_cost=var_cost,
        marginal_heat_rate=hr,
        energy_price=float(energy_price),
        bid_markup=float(markup),
        congestion_adder=float(congestion_adder),
        scarcity_adder=float(scarcity),
        utilization=float(utilization),
        reserve_headroom_mw=float(energy_cap - net_load_mw),
        stack_position_pct=float(np.clip(utilization * 100.0, 0.0, 100.0)),
    )
