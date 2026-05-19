"""``meteo_hourly`` demand block + panel assembly.

Fetches the Meteologica regional supply-demand bundle (one row per
``(region, date, hour_ending)`` with load/solar/wind/net-load forecast)
and pivots it to per-HE columns ``meteo_<series>_<region>`` for every
configured region/series, then hands that to
``da_models.linear_arx_da_price.features.common.assemble_panel`` which
adds the feed-agnostic groups (DA-LMP labels, weather, gas, outages,
calendar, interactions, optional backward-LMP anchors).

``build_panel`` -- single-day (DA-cutoff ``lead_days=1`` vintage).
``build_panel_horizon`` -- multi-day forward strip: training rows still
use the lead-1 vintage, but the future delivery dates beyond D+1 are
pulled from the *latest published* Meteologica vintage (which spans
~7-14 forward days), and the feeds that don't reach the horizon end
(outages, ICE next-day gas) are carried forward from the last known
value.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from da_models.common.data import loader
from da_models.linear_arx_da_price import configs as C
from da_models.linear_arx_da_price.features import common as fc
from da_models.linear_arx_da_price.meteo_hourly import config as V

logger = logging.getLogger(__name__)

_SERIES_TO_COL: dict[str, str] = {
    "load": "load_mw",
    "solar": "solar_mw",
    "wind": "wind_mw",
    "net_load": "net_load_mw",
}
# Daily-grain feeds whose coverage runs out before a 14-day horizon -- the
# horizon pipeline carries the last known value forward onto the late days.
HORIZON_FORWARD_FILL_COLS: tuple[str, ...] = (
    "outages_total_mw",
    "gas_m3",
    "gas_tz6",
    "gas_dom_south",
)


def _pivot_regional(df: pd.DataFrame) -> pd.DataFrame | None:
    """Pivot a Meteologica supply-demand long frame to per-HE
    ``meteo_<series>_<region>`` columns over the configured regions/series."""
    df = fc.coerce_date_col(fc.coerce_hour_col(df))
    df["region"] = df["region"].astype(str)
    out: pd.DataFrame | None = None
    for region in V.METEO_REGIONS:
        sub = df[df["region"] == region]
        if sub.empty:
            logger.warning("Meteologica region %s has no rows; skipping", region)
            continue
        rename = {
            _SERIES_TO_COL[s]: f"meteo_{s}_{region.lower()}"
            for s in V.METEO_SERIES
            if _SERIES_TO_COL[s] in sub.columns
        }
        piece = sub[["date", "hour_ending", *rename.keys()]].rename(columns=rename)
        out = piece if out is None else out.merge(piece, on=fc.KEY_COLS, how="outer")
    return out


def _meteo_supply_demand(cache_dir: Path | None) -> pd.DataFrame:
    """DA-cutoff (``lead_days=1``) Meteologica demand block."""
    df = loader.load_meteologica_supply_demand_coalesced(
        cache_dir=cache_dir, lead_days=C.LEAD_DAYS
    )
    out = _pivot_regional(df)
    if out is None:
        raise RuntimeError(
            "No Meteologica supply-demand rows for any configured region."
        )
    return out


def _meteo_supply_demand_with_horizon(cache_dir: Path | None) -> pd.DataFrame:
    """Lead-1 demand for the training span + D+1, extended with the latest
    published vintage for the further-out delivery dates it doesn't cover."""
    base = _meteo_supply_demand(cache_dir)
    latest_df = loader.load_meteologica_supply_demand_coalesced(
        cache_dir=cache_dir, latest_only=True
    )
    latest = _pivot_regional(latest_df)
    if latest is None:
        logger.warning(
            "latest Meteologica vintage empty; horizon limited to lead-1 coverage"
        )
        return base
    base_dates = set(base["date"])
    extra = latest[~latest["date"].isin(base_dates)]
    return pd.concat([base, extra], ignore_index=True)


def _assemble(
    demand: pd.DataFrame,
    *,
    target_date,
    cache_dir,
    hub,
    extra_target_dates=(),
    forward_fill_target_cols=(),
):
    return fc.assemble_panel(
        target_date,
        cache_dir=cache_dir,
        hub=hub,
        per_he_demand=demand,
        primary_load_col=V.PRIMARY_LOAD_COL,
        primary_net_load_col=V.PRIMARY_NET_LOAD_COL,
        primary_gas_col=V.PRIMARY_GAS_COL,
        target_required_cols=list(V.TARGET_REQUIRED_COLS),
        extra_target_dates=tuple(extra_target_dates),
        forward_fill_target_cols=tuple(forward_fill_target_cols),
        include_backward_lmp=V.INCLUDE_BACKWARD_LMP,
        backward_default_lag=V.BACKWARD_LMP_DEFAULT_LAG_DAYS,
        backward_monday_lag=V.BACKWARD_LMP_MONDAY_LAG_DAYS,
    )


def build_panel(
    *, target_date: date, cache_dir: Path | None = None, hub: str = C.HUB
) -> dict:
    """Single-day ``meteo_hourly`` panel. Returns the ``assemble_panel`` dict."""
    return _assemble(
        _meteo_supply_demand(cache_dir),
        target_date=target_date,
        cache_dir=cache_dir,
        hub=hub,
    )


def build_panel_horizon(
    *,
    run_date: date,
    horizon_days: int,
    cache_dir: Path | None = None,
    hub: str = C.HUB,
    forward_fill_cols: tuple[str, ...] = HORIZON_FORWARD_FILL_COLS,
) -> dict:
    """Multi-day-horizon ``meteo_hourly`` panel: target rows D+1..D+``horizon_days``.

    Demand comes from the latest published Meteologica vintage for the
    further-out days; ``forward_fill_cols`` (default: outages + ICE gas hubs)
    are carried forward from the last known value onto the late-horizon rows.
    Returns the ``assemble_panel`` dict (with ``target_dates`` /
    ``has_target_features_by_date``).
    """
    target_dates = [
        run_date + timedelta(days=k) for k in range(1, int(horizon_days) + 1)
    ]
    demand = _meteo_supply_demand_with_horizon(cache_dir)
    return _assemble(
        demand,
        target_date=target_dates[0],
        cache_dir=cache_dir,
        hub=hub,
        extra_target_dates=tuple(target_dates[1:]),
        forward_fill_target_cols=tuple(forward_fill_cols),
    )
