"""``pjm_hourly`` demand block + panel assembly.

Fetches the PJM per-HE demand features (RTO supply-demand bundle +
sub-zonal load forecasts) and hands them to
``da_models.linear_arx_da_price.features.common.assemble_panel``, which
adds the feed-agnostic groups (DA-LMP labels, weather, gas, outages,
calendar, interactions, optional backward-LMP anchors).
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from da_models.common.data import loader
from da_models.linear_arx_da_price import configs as C
from da_models.linear_arx_da_price.features import common as fc
from da_models.linear_arx_da_price.pjm_hourly import config as V

logger = logging.getLogger(__name__)


def _rto_supply_demand(cache_dir: Path | None) -> pd.DataFrame:
    df = loader.load_pjm_supply_demand_coalesced(
        cache_dir=cache_dir, region=V.LOAD_RTO_REGION, lead_days=C.LEAD_DAYS
    )
    df = fc.coerce_date_col(fc.coerce_hour_col(df))
    return df[
        ["date", "hour_ending", "load_mw", "solar_mw", "wind_mw", "net_load_mw"]
    ].rename(
        columns={
            "load_mw": "load_rto",
            "solar_mw": "solar_rto",
            "wind_mw": "wind_rto",
            "net_load_mw": "net_load_rto",
        }
    )


def _subzonal_load(cache_dir: Path | None) -> pd.DataFrame | None:
    try:
        df = loader.load_load_coalesced(cache_dir=cache_dir)
    except Exception as exc:  # noqa: BLE001
        logger.warning("sub-zonal load unavailable (%s); skipping", exc)
        return None
    df = fc.coerce_date_col(fc.coerce_hour_col(df))
    frames: list[pd.DataFrame] = []
    for region in V.SUBZONE_LOAD_REGIONS:
        sub = df[df["region"].astype(str) == region][["date", "hour_ending", "load_mw"]]
        if sub.empty:
            logger.warning("sub-zone %s has no load rows; skipping", region)
            continue
        frames.append(sub.rename(columns={"load_mw": f"load_{region.lower()}"}))
    if not frames:
        return None
    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on=fc.KEY_COLS, how="outer")
    return out


def build_panel(
    *, target_date: date, cache_dir: Path | None = None, hub: str = C.HUB
) -> dict:
    """Assemble the ``pjm_hourly`` training + target feature panel.

    Returns the dict described in ``features.common.assemble_panel``.
    """
    demand = _rto_supply_demand(cache_dir)
    sub = _subzonal_load(cache_dir)
    if sub is not None:
        demand = demand.merge(sub, on=fc.KEY_COLS, how="outer")
    return fc.assemble_panel(
        target_date,
        cache_dir=cache_dir,
        hub=hub,
        per_he_demand=demand,
        primary_load_col=V.PRIMARY_LOAD_COL,
        primary_net_load_col=V.PRIMARY_NET_LOAD_COL,
        primary_gas_col=V.PRIMARY_GAS_COL,
        target_required_cols=list(V.TARGET_REQUIRED_COLS),
        include_backward_lmp=V.INCLUDE_BACKWARD_LMP,
        backward_default_lag=V.BACKWARD_LMP_DEFAULT_LAG_DAYS,
        backward_monday_lag=V.BACKWARD_LMP_MONDAY_LAG_DAYS,
    )
