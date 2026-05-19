"""Assemble inputs, dispatch all 24 hours, and produce the day's forecast.

Inputs: RTO load/solar/wind/net-load forecast (prefers the Meteologica
regional bundle -- horizon ~D+13, so a next-week date is covered), the
4 scraped ICE next-day gas hubs (Tetco M3 / Columbia TCO / Transco Z6 /
Dominion South -- forward-filled past their ~D+1 horizon), and the
aggregate outage forecast for the delivery date (forward-filled past the
7-day horizon). For each hour: build the outage-derated *per-unit* merit
order at that hour's gas prices, dispatch the hour's net load, read off
clearing price + marginal fuel + heat rate + reserve headroom.

Uncertainty bands: a Monte Carlo that perturbs the load forecast, the
forced-outage MW, and the gas price; re-dispatch every hour for each
draw; take per-hour quantiles. The returned per-HE frame mirrors the
other families' shape (``hour_ending, point_forecast, p50, q_0.10 ...
q_0.90``) plus structural metadata (``marginal_fuel``,
``marginal_heat_rate``, ``reserve_headroom_mw``, ``stack_position_pct``,
``utilization``, ``energy_price``, ``bid_markup``, ``congestion_adder``,
``ramp_adder``, ``scarcity_adder``, ``gas_price``).
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from da_models.common.data import loader
from da_models.supply_stack import configs as C
from da_models.supply_stack.data.fleet import build_fleet
from da_models.supply_stack.stack.dispatch import dispatch
from da_models.supply_stack.stack.merit_order import (
    assert_valid_stack,
    build_merit_order,
)

logger = logging.getLogger(__name__)

_GAS_COLS = tuple(C.LIVE_GAS_HUB_MAP.keys())  # gas_m3, gas_tco, gas_tz6, gas_dom_south


# ── Input assembly ─────────────────────────────────────────────────────────
def _slice_day(sd: pd.DataFrame, target_date: date) -> pd.DataFrame | None:
    sd = sd.copy()
    sd["date"] = pd.to_datetime(sd["date"], errors="coerce").dt.date
    sd["hour_ending"] = pd.to_numeric(sd["hour_ending"], errors="coerce")
    if "region" in sd.columns:
        sd = sd[sd["region"].astype(str) == "RTO"]
    day = sd[(sd["date"] == target_date) & sd["hour_ending"].notna()].copy()
    if day.empty:
        return None
    day["hour_ending"] = day["hour_ending"].astype(int)
    cols = [
        c for c in ("load_mw", "solar_mw", "wind_mw", "net_load_mw") if c in day.columns
    ]
    return (
        day[["hour_ending", *cols]]
        .groupby("hour_ending", as_index=False)
        .mean(numeric_only=True)
        .sort_values("hour_ending")
    )


def _net_load_24(
    target_date: date, cache_dir: Path | None, *, latest_only: bool
) -> tuple[pd.DataFrame, str] | None:
    """RTO load/solar/wind/net-load for the target date. Prefers the
    Meteologica regional bundle (~D+13 horizon); falls back to the PJM
    bundle (RTO-only, ~D+1) then Meteologica lead-1. Returns (24-HE frame,
    source-label) or None."""
    attempts: list[tuple[str, callable]] = []
    if latest_only:
        attempts.append(
            (
                "meteologica:latest",
                lambda: loader.load_meteologica_supply_demand_coalesced(
                    cache_dir=cache_dir, latest_only=True
                ),
            )
        )
        attempts.append(
            (
                "pjm:latest",
                lambda: loader.load_pjm_supply_demand_coalesced(
                    cache_dir=cache_dir, region="RTO", latest_only=True
                ),
            )
        )
    attempts.append(
        (
            "pjm:lead1",
            lambda: loader.load_pjm_supply_demand_coalesced(
                cache_dir=cache_dir, region="RTO", lead_days=C.LEAD_DAYS
            ),
        )
    )
    attempts.append(
        (
            "meteologica:lead1",
            lambda: loader.load_meteologica_supply_demand_coalesced(
                cache_dir=cache_dir, lead_days=C.LEAD_DAYS
            ),
        )
    )
    for label, fn in attempts:
        try:
            sd = fn()
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s supply-demand failed: %s", label, exc)
            continue
        out = _slice_day(sd, target_date)
        if out is not None and not out.empty and "net_load_mw" in out.columns:
            if len(out) < len(C.HOURS):
                logger.warning(
                    "%s has only %d/24 HEs for %s", label, len(out), target_date
                )
            return out, label
    return None


def _gas_24(
    target_date: date, cache_dir: Path | None
) -> dict[int, dict[str, float]] | None:
    """HE -> {gas_m3, gas_tco, gas_tz6, gas_dom_south} $/MMBtu for the day.
    Forward-fills from the most recent gas_day if the target isn't priced yet."""
    try:
        g = loader.load_gas_prices_hourly(cache_dir=cache_dir)
    except Exception as exc:  # noqa: BLE001
        logger.error("gas prices unavailable: %s", exc)
        return None
    cols = [c for c in _GAS_COLS if c in g.columns]
    if not cols:
        logger.error("no expected gas hub columns in feed (%s)", _GAS_COLS)
        return None
    g = g.copy()
    g["date"] = pd.to_datetime(g["date"], errors="coerce").dt.date
    g["hour_ending"] = pd.to_numeric(g["hour_ending"], errors="coerce")
    g = g.dropna(subset=["date", "hour_ending"])
    g["hour_ending"] = g["hour_ending"].astype(int)
    day = g[g["date"] == target_date]
    if day.empty:
        prior = g[g["date"] <= target_date]
        if prior.empty:
            return None
        last_day = prior["date"].max()
        day = g[g["date"] == last_day]
        logger.warning("no gas for %s; forward-filling from %s", target_date, last_day)
    he_means = day.groupby("hour_ending")[cols].mean()
    day_mean = day[cols].mean()
    out: dict[int, dict[str, float]] = {}
    for h in C.HOURS:
        row = he_means.loc[h] if h in he_means.index else day_mean
        out[h] = {
            c: float(row[c]) if pd.notna(row[c]) else float(day_mean[c]) for c in cols
        }
    return out


def _outage_mw(target_date: date, cache_dir: Path | None) -> float:
    for fn, date_col in (
        (lambda: loader.load_outages_forecast(cache_dir=cache_dir), "date"),
        (
            lambda: loader.load_outages_forecast_history(
                cache_dir=cache_dir, lead_days=C.LEAD_DAYS
            ),
            "forecast_date",
        ),
    ):
        try:
            df = fn()
        except Exception:  # noqa: BLE001
            continue
        if df is None or df.empty or "total_outages_mw" not in df.columns:
            continue
        if "region" in df.columns:
            df = df[df["region"].astype(str) == "RTO"]
        dc = (
            date_col
            if date_col in df.columns
            else ("date" if "date" in df.columns else None)
        )
        if dc is None:
            continue
        df = df.copy()
        df[dc] = pd.to_datetime(df[dc], errors="coerce").dt.date
        df = df.dropna(subset=[dc])
        exact = df[df[dc] == target_date]
        if not exact.empty:
            return float(exact["total_outages_mw"].iloc[-1])
        prior = df[df[dc] <= target_date]
        if not prior.empty:
            return float(prior.sort_values(dc)["total_outages_mw"].iloc[-1])
    logger.warning("no outage forecast for %s; assuming 0 MW derate", target_date)
    return 0.0


# ── Dispatch the day ───────────────────────────────────────────────────────
def _dispatch_one(
    fleet: pd.DataFrame,
    net_load_mw: float,
    gas_by_hub: dict[str, float],
    outage_mw: float,
    ramp_adder: float,
    operating_reserve_mw: float,
):
    merit = build_merit_order(fleet, gas_prices_by_hub=gas_by_hub, outage_mw=outage_mw)
    r = dispatch(merit, net_load_mw, operating_reserve_mw=operating_reserve_mw)
    clearing = min(r.clearing_price + ramp_adder, C.PRICE_CAP_USD_MWH)
    return merit, r, clearing


def _dispatch_day(
    fleet: pd.DataFrame,
    net_load: pd.DataFrame,
    gas_by_he: dict[int, dict[str, float]],
    outage_mw: float,
    reserve_by_he: dict[int, float],
) -> pd.DataFrame:
    nl_by_he = dict(
        zip(net_load["hour_ending"].astype(int), net_load["net_load_mw"].astype(float))
    )
    rows: list[dict] = []
    for h in C.HOURS:
        if h not in nl_by_he:
            rows.append({"hour_ending": h})
            continue
        ramp = float(C.HOURLY_RAMP_ADDER_USD_MWH[h - 1])
        merit, r, clearing = _dispatch_one(
            fleet, nl_by_he[h], gas_by_he[h], outage_mw, ramp, reserve_by_he[h]
        )
        if h == 1:
            assert_valid_stack(merit)  # invariant guard once per day is enough
        rows.append(
            {
                "hour_ending": h,
                "net_load_mw": nl_by_he[h],
                "clearing_price": clearing,
                "marginal_fuel": r.marginal_fuel,
                "marginal_block": r.marginal_block,
                "marginal_heat_rate": r.marginal_heat_rate,
                "energy_price": r.energy_price,
                "bid_markup": r.bid_markup,
                "congestion_adder": r.congestion_adder,
                "ramp_adder": ramp,
                "scarcity_adder": r.scarcity_adder,
                "utilization": r.utilization,
                "reserve_headroom_mw": r.reserve_headroom_mw,
                "stack_position_pct": r.stack_position_pct,
                "gas_price": gas_by_he[h].get(C.GAS_SANITY_HUB),
            }
        )
    return pd.DataFrame(rows)


def _monte_carlo_bands(
    fleet: pd.DataFrame,
    net_load: pd.DataFrame,
    gas_by_he: dict[int, dict[str, float]],
    outage_mw: float,
    reserve_by_he: dict[int, float],
) -> dict[int, dict[float, float]]:
    rng = np.random.default_rng(C.MC_SEED)
    nl_by_he = dict(
        zip(net_load["hour_ending"].astype(int), net_load["net_load_mw"].astype(float))
    )
    draws: dict[int, list[float]] = {h: [] for h in nl_by_he}
    for _ in range(C.MC_DRAWS):
        load_mult = 1.0 + rng.normal(0.0, C.MC_LOAD_SIGMA_PCT)
        gas_mult = max(0.1, 1.0 + rng.normal(0.0, C.MC_GAS_SIGMA_PCT))
        out_draw = max(0.0, outage_mw + rng.normal(0.0, C.MC_FORCED_OUTAGE_SIGMA_MW))
        for h, nl in nl_by_he.items():
            gas_h = {k: v * gas_mult for k, v in gas_by_he[h].items()}
            _, _, clearing = _dispatch_one(
                fleet,
                nl * load_mult,
                gas_h,
                out_draw,
                float(C.HOURLY_RAMP_ADDER_USD_MWH[h - 1]),
                reserve_by_he[h],
            )
            draws[h].append(clearing)
    return {
        h: {q: float(np.quantile(np.asarray(v), q)) for q in C.QUANTILES}
        for h, v in draws.items()
    }


def forecast_day(
    target_date: date,
    *,
    cache_dir: Path | None = None,
    hub: str = C.HUB,
    latest_only: bool = True,
    with_bands: bool = True,
) -> dict:
    """Dispatch all 24 hours for ``target_date``. Returns a dict:
    ``forecast_table``, ``fleet``, ``fleet_meta``, ``inputs``, ``hub``,
    ``target_date``, ``has_inputs``. When ``has_inputs`` is False the table
    is empty (a required feed was missing)."""
    # Local import to keep ``reserves`` co-located with its single use --
    # ``supply_stack.data.reserves`` reads the dbt-mart parquet via
    # ``loader.load_reserve_market_results_hourly``, with a rolling-profile
    # fallback for forward dates and a config-constant fallback when the
    # parquet is missing entirely.
    from da_models.supply_stack.data import reserves

    fb = build_fleet(target_date, cache_dir=cache_dir)
    fleet, fleet_meta = fb["fleet"], fb["meta"]

    nl_result = _net_load_24(target_date, cache_dir, latest_only=latest_only)
    gas_by_he = _gas_24(target_date, cache_dir)
    if nl_result is None or gas_by_he is None:
        return {
            "forecast_table": pd.DataFrame({"hour_ending": list(C.HOURS)}),
            "fleet": fleet,
            "fleet_meta": fleet_meta,
            "inputs": {},
            "hub": hub,
            "target_date": target_date,
            "has_inputs": False,
        }
    net_load, demand_source = nl_result
    outage_mw = _outage_mw(target_date, cache_dir)
    reserve_by_he = reserves.get_operating_reserve_mw_by_he(
        target_date, cache_dir=cache_dir
    )

    table = _dispatch_day(fleet, net_load, gas_by_he, outage_mw, reserve_by_he)
    table["point_forecast"] = table["clearing_price"]
    if with_bands:
        bands = _monte_carlo_bands(fleet, net_load, gas_by_he, outage_mw, reserve_by_he)
        for q in C.QUANTILES:
            table[f"q_{q:.2f}"] = table["hour_ending"].map(
                lambda h, _q=q: bands.get(h, {}).get(_q, np.nan)
            )
        table["p50"] = table["hour_ending"].map(
            lambda h: bands.get(h, {}).get(0.50, np.nan)
        )
    else:
        for q in C.QUANTILES:
            table[f"q_{q:.2f}"] = np.nan
        table["p50"] = table["point_forecast"]

    gas_mean = float(
        np.mean([gh.get(C.GAS_SANITY_HUB, np.nan) for gh in gas_by_he.values()])
    )
    available = fleet_meta["total_stack_mw"] - outage_mw
    reserve_mean = float(np.mean(list(reserve_by_he.values())))
    energy_offered = max(available - reserve_mean, 0.0)
    inputs = {
        "outage_mw": float(outage_mw),
        "available_stack_mw": float(available),
        "operating_reserve_mw_mean": reserve_mean,
        "operating_reserve_mw_by_he": {
            int(h): float(v) for h, v in reserve_by_he.items()
        },
        "energy_offered_mw": float(energy_offered),
        "gas_sanity_hub": C.GAS_SANITY_HUB,
        "gas_mean_usd_mmbtu": gas_mean,
        "net_load_peak_mw": float(net_load["net_load_mw"].max()),
        "net_load_avg_mw": float(net_load["net_load_mw"].mean()),
        "load_peak_mw": float(net_load["load_mw"].max())
        if "load_mw" in net_load
        else None,
        "carbon_price_usd_ton": C.CARBON_PRICE_USD_TON_CO2,
        "congestion_adder_usd_mwh": C.CONGESTION_ADDER_USD_MWH,
        "demand_source": demand_source,
        "n_he": int(len(net_load)),
        "vintage": "latest" if latest_only else f"lead_{C.LEAD_DAYS}",
    }
    return {
        "forecast_table": table,
        "fleet": fleet,
        "fleet_meta": fleet_meta,
        "inputs": inputs,
        "hub": hub,
        "target_date": target_date,
        "has_inputs": True,
    }
