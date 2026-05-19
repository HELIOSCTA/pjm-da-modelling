"""Configuration for the supply-stack DA-LMP forecaster.

The fleet is now per-unit (``data/pjm_fleet.parquet``, extracted from the
legacy Excel model -- see ``data/_extract_fleet_from_excel.py``). This
file holds the *non-fleet* assumptions a structural model rests on: fuel
prices by hub (coal/oil/uranium constants; the 4 scraped gas hubs are
overridden hour-by-hour by the live feed), carbon pricing + CO2
intensities, default heat rates for units with a missing one, which fuel
categories are netted-off-load vs in the thermal stack, the seasonal
cap-rating switch, the bid-stack markup curve, the reserve-utilization
scarcity curve, the hour-of-day ramp adder, and the Monte-Carlo band
config. All of it is echoed in the run report so every forecast is
auditable.

CALIBRATION IS PROVISIONAL. The per-unit heat-rate / VOM data is real
(from the workbook); the *curves* below (bid markup, scarcity bands, ramp
adder) and the coal/oil price constants are hand-set, not fitted -- the
supply_stack_model.md Phase-5 backtest (price-duration-curve /
marginal-fuel-match / DM-test) is what tunes them.
"""

from __future__ import annotations

MODEL_NAME: str = "supply_stack_pjm_western_hub"
MODEL_FAMILY: str = "supply_stack"

# ── Target ─────────────────────────────────────────────────────────────────
HUB: str = "WESTERN HUB"
HOURS: tuple[int, ...] = tuple(range(1, 25))
QUANTILES: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)
GAS_REF_HEAT_RATE: float = 7.0  # most-efficient-CC implied-heat-rate sanity anchor

# ── Fleet (per-unit parquet) ───────────────────────────────────────────────
FLEET_PARQUET: str = "pjm_fleet.parquet"
# Categories netted OFF the load (we consume their forecast generation) rather
# than placed in the thermal merit order:
EXCLUDE_FUEL_CATEGORIES: tuple[str, ...] = ("Solar", "Wind")
# Categories dispatched first regardless of economics (run flat near max):
MUST_RUN_FUEL_CATEGORIES: tuple[str, ...] = ("Nuclear",)
# Months that use the *summer* cap rating (else the winter rating):
SUMMER_CAP_MONTHS: tuple[int, ...] = (5, 6, 7, 8, 9, 10)
# Default heat rate (MMBtu/MWh) for a thermal unit whose parquet heat rate is
# 0 / missing (mostly old oil peakers and a few gas OC steam units):
DEFAULT_HEAT_RATE_MMBTU_MWH: dict[str, float] = {
    "Coal": 11.0,
    "Gas CC": 7.5,
    "Gas CT/ST": 11.0,
    "Oil": 13.5,
    "Nuclear": 10.0,
    "Biomass": 14.0,
    "Other": 10.0,
}

# ── Fuel prices ($/MMBtu) -- from the Excel Assumptions sheet ──────────────
# The 4 gas hubs we scrape (LIVE_GAS_HUB_MAP) are overridden per-hour by the
# live feed; every other hub uses the constant here.
FUEL_PRICES_USD_MMBTU: dict[str, float] = {
    # gas hubs (in the Assumptions sheet)
    "Columbia TCO Pool": 2.60,
    "Dominion South Pt": 2.45,
    "Northern Ventura": 2.65,
    "Tetco M2 (deliveries)": 2.70,
    "Tetco M3": 2.85,
    "Transco Leidy": 2.55,
    "Transco Z5 non-WGL": 2.75,
    "Transco Z6": 2.90,
    # gas hubs that appear in the fleet but not the Assumptions sheet -> proxies
    "Tennessee Z4 (Marcellus)": 2.55,
    "Tennessee Z5": 2.85,
    "Chicago CityGate": 2.70,
    "ANR Southwest": 2.50,
    "Henry Hub": 2.60,
    "Algonquin CityGate": 3.20,
    "MichCon CityGate": 2.65,
    "Panhandle Oklahoma": 2.40,
    "REX East-Midwest": 2.40,
    # coal
    "Big Sandy (Barge)": 2.50,
    "Central App (CSX)": 2.65,
    "Northern App (Penn Railcar)": 2.80,
    "PRB 8800": 1.20,
    "Uinta Basin": 1.90,
    "Illinois Basin": 2.20,
    # oil
    "Gulf Coast No 6 Distillate": 9.50,
    "NY No 2 Distillate": 16.00,
    "NY No 54 Jet Fuel": 17.00,
    # nuclear
    "Uranium 308": 0.70,
}
# loader.load_gas_prices_hourly column -> Excel fuel-hub name it prices.
LIVE_GAS_HUB_MAP: dict[str, str] = {
    "gas_m3": "Tetco M3",
    "gas_tco": "Columbia TCO Pool",
    "gas_tz6": "Transco Z6",
    "gas_dom_south": "Dominion South Pt",
}
GAS_SANITY_HUB: str = "gas_m3"  # Western Hub's marginal gas hub, for the CC anchor
# Non-gas fuel-hub names (everything else with a hub is treated as gas).
COAL_FUEL_HUBS: frozenset[str] = frozenset(
    {
        "Big Sandy (Barge)",
        "Central App (CSX)",
        "Northern App (Penn Railcar)",
        "PRB 8800",
        "Uinta Basin",
        "Illinois Basin",
    }
)
OIL_FUEL_HUBS: frozenset[str] = frozenset(
    {"Gulf Coast No 6 Distillate", "NY No 2 Distillate", "NY No 54 Jet Fuel"}
)
NUCLEAR_FUEL_HUBS: frozenset[str] = frozenset({"Uranium 308"})

# ── Carbon ─────────────────────────────────────────────────────────────────
CARBON_PRICE_USD_TON_CO2: float = 15.0  # RGGI (applies to RGGI-flagged units only)
CO2_INTENSITY_TON_MMBTU: dict[str, float] = {
    "Gas CC": 0.053,
    "Gas CT/ST": 0.053,
    "Coal": 0.097,
    "Oil": 0.075,
    "Other": 0.060,
}

# ── Pricing: bid-stack markup, congestion, ramp, scarcity ──────────────────
CONGESTION_ADDER_USD_MWH: float = 3.0  # flat losses+congestion premium at Western Hub
# PJM co-optimises energy and reserves: ~3.3 GW primary + ~3.3 GW synchronized
# + ~3-5 GW 30-min supplemental are held *out* of the energy market. The
# utilization denominator that drives bid-markup and scarcity is therefore
# (total_derated_capacity - operating_reserve_mw), not raw total capacity --
# without this, a 108 GW net-load day on a 131 GW stack sits at "82%" and
# never trips scarcity. The *per-HE* value is pulled from the dbt mart
# ``pjm_reserve_market_results_hourly`` via
# ``supply_stack.data.reserves.get_operating_reserve_mw_by_he`` (uses the
# requirement, not the cleared MW -- PJM overclears 30-MIN with units that
# are also bidding into energy, so cleared overcounts). This constant is
# the safety-net fallback when the mart / parquet is unavailable; the
# empirical median in May 2026 was ~7,590 MW (sd 28).
OPERATING_RESERVE_MW_FALLBACK: float = 7_500.0

# Bid-stack markup: clearing price = marginal_var_cost * markup(utilization)
# + congestion + ramp + scarcity. Generators bid above marginal cost and the
# bid stack is convex (Coulon-Howison); the markup grows with how tight the
# stack is. ``utilization`` = net_load / (total_derated - operating_reserve_mw).
BID_MARKUP_BANDS: tuple[tuple[float, float], ...] = (
    (0.40, 1.15),
    (0.55, 1.30),
    (0.70, 1.55),
    (0.82, 1.95),
    (0.90, 2.50),
)
# Hour-of-day ramp/commitment premium ($/MWh, added per HE). Index 0 == HE1.
HOURLY_RAMP_ADDER_USD_MWH: tuple[float, ...] = (
    0,
    0,
    0,
    0,
    0,
    1,
    4,
    6,
    4,
    3,
    2,
    2,
    2,
    3,
    4,
    6,
    10,
    14,
    18,
    20,
    16,
    9,
    4,
    1,
)
# Reserve-utilization scarcity adder ($/MWh). Knots lowered to kick in at
# 85-90% utilization (the energy-stack is reserve-haircut'd above) so a heat
# event with ~90-95% util gets a real scarcity premium. Last knot (>=1.0) is
# the shortage/VOLL proxy -- net load above the stack -> finite (large) price.
SCARCITY_BANDS: tuple[tuple[float, float], ...] = (
    (0.85, 0.0),
    (0.90, 40.0),
    (0.94, 150.0),
    (0.98, 600.0),
    (1.00, 1500.0),
    (1.05, 2800.0),
)
PRICE_CAP_USD_MWH: float = 3000.0

# ── Uncertainty bands (Monte Carlo) ────────────────────────────────────────
MC_DRAWS: int = 200
MC_LOAD_SIGMA_PCT: float = 0.03
MC_FORCED_OUTAGE_SIGMA_MW: float = 4_000.0
MC_GAS_SIGMA_PCT: float = 0.05
MC_SEED: int = 12345

# ── Vintage ────────────────────────────────────────────────────────────────
LEAD_DAYS: int = 1  # outage-forecast vintage; load uses latest_only for the horizon
