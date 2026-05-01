"""Data-access layer for the transmission-outage MCP views.

Each function pulls one dbt mart from `pjm_da_modelling_cleaned.*`. Filter
logic (active / 7-day window / 24h delta) lives in dbt — this layer is a
thin SQL passthrough.

Marts produced by:
  - models/power/pjm/marts/views/pjm_transmission_outages_active.sql
  - models/power/pjm/marts/views/pjm_transmission_outages_window_7d.sql
  - models/power/pjm/marts/views/pjm_transmission_outages_changes_24h_simple.sql
  - models/power/pjm/marts/views/pjm_transmission_outages_changes_24h_snapshot.sql

Refresh: see backend/schedulers/prefect/power/pjm/pjm_transmission_outages_daily_flows.py
"""
from __future__ import annotations

import logging

import pandas as pd

from backend.settings import DBT_SCHEMA
from backend.utils.azure_postgresql_utils import pull_from_db

logger = logging.getLogger(__name__)


def _pull_mart(table: str) -> pd.DataFrame:
    """Run ``select * from <DBT_SCHEMA>.<table>`` and return a DataFrame."""
    query = f"SELECT * FROM {DBT_SCHEMA}.{table}"
    logger.info(f"Pulling {DBT_SCHEMA}.{table}")
    df = pull_from_db(query=query)
    logger.info(f"Pulled {len(df):,} rows from {DBT_SCHEMA}.{table}")
    return df


def pull_active() -> pd.DataFrame:
    """Currently active or scheduled-and-locked-in outages.

    Filter (applied in dbt staging):
      outage_state in ('Active','Approved'), equipment_type in ('LINE','XFMR','PS'),
      voltage_kv >= 230.
    """
    return _pull_mart("pjm_transmission_outages_active")


def pull_window_7d() -> pd.DataFrame:
    """7-day forward outlook: outages overlapping [now, now+7d].

    Includes Received (planned-but-unapproved) tickets alongside Active/Approved.
    Each row carries a ``state_class`` column ∈ {locked, planned}.
    """
    return _pull_mart("pjm_transmission_outages_window_7d")


def pull_changes_24h_simple() -> pd.DataFrame:
    """Last-24h delta (simple variant).

    Driven by source-table ``created_at`` and ``last_revised`` only — no
    snapshot needed. Each row has ``change_type`` ∈ {NEW, REVISED}. No
    diff columns and no CLEARED detection. Useful day 1.
    """
    return _pull_mart("pjm_transmission_outages_changes_24h_simple")


def pull_changes_24h_snapshot() -> pd.DataFrame:
    """Last-24h delta (snapshot variant).

    Driven by the SCD2 snapshot (``pjm_transmission_outages_snapshot``).
    Each row has ``change_type`` ∈ {NEW, REVISED, CLEARED} and REVISED rows
    carry ``prev_outage_state`` / ``prev_start_datetime`` /
    ``prev_end_datetime`` / ``prev_risk`` for diff display.

    Returns an empty frame for the first 24h after the snapshot is first
    initialized — there's no history yet to diff against.
    """
    return _pull_mart("pjm_transmission_outages_changes_24h_snapshot")
