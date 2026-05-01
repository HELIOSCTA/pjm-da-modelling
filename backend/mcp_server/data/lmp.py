"""Data-access layer for the DA-LMP MCP views.

Pulls from ``pjm_da_modelling_cleaned.pjm_lmps_hourly`` — the unified
DA / RT / DART hourly mart at hub grain. The DA scrape (``da_hrl_lmps``)
filters ``type=hub`` upstream, so this mart only carries the ~15-20 PJM
aggregate hubs (no zonal or bus-level LMP).
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from backend.settings import DBT_SCHEMA
from backend.utils.azure_postgresql_utils import pull_from_db

logger = logging.getLogger(__name__)

_LMPS_TABLE = f"{DBT_SCHEMA}.pjm_lmps_hourly"


def pull_lmp_da_hourly(
    target_date: date, hubs: list[str] | None = None,
) -> pd.DataFrame:
    """DA hourly LMPs at hub grain for a single target date.

    Returns one row per (hub, hour_ending) with:
      ``hub``, ``date``, ``hour_ending``,
      ``lmp_total``, ``lmp_system_energy_price``,
      ``lmp_congestion_price``, ``lmp_marginal_loss_price``.

    Forward-looking: defaults to tomorrow's DA results in the endpoint
    (use after the DA market clears, ~13:30 EPT).

    When ``hubs`` is provided, filters to that subset (Tier 2 funnel
    drilldown). Hub names are case-sensitive matches against the mart.
    """
    hub_filter = ""
    if hubs:
        # SQL-escape via simple quoting — hub names come from a controlled
        # upstream pivot, no user-supplied strings reach this.
        quoted = ", ".join(f"'{h}'" for h in hubs)
        hub_filter = f"AND hub IN ({quoted})"
    query = f"""
        SELECT
            date,
            hour_ending,
            hub,
            lmp_total,
            lmp_system_energy_price,
            lmp_congestion_price,
            lmp_marginal_loss_price
        FROM {_LMPS_TABLE}
        WHERE date = '{target_date.isoformat()}'
          AND market = 'da'
          {hub_filter}
        ORDER BY hub, hour_ending
    """
    logger.info(f"Pulling DA LMPs for {target_date} (hubs={hubs or 'all'})")
    df = pull_from_db(query=query)
    logger.info(f"Pulled {len(df):,} DA LMP rows ({df['hub'].nunique() if len(df) else 0} hubs)")
    return df
