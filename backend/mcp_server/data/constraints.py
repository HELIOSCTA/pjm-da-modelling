"""Data-access layer for the binding-constraint MCP views.

Pulls from ``pjm_da_modelling_cleaned.pjm_constraints_hourly_pivot`` — the
unified DA / RT / DART hourly pivot mart. Filtering by date and market is
done in SQL since the constraint count is small (<50 / day) but we keep
unrelated rows out of the matcher.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from backend.settings import DBT_SCHEMA
from backend.utils.azure_postgresql_utils import pull_from_db

logger = logging.getLogger(__name__)

_PIVOT_TABLE = f"{DBT_SCHEMA}.pjm_constraints_hourly_pivot"


def pull_constraints_da(target_date: date) -> pd.DataFrame:
    """DA constraints for a single date — forward-looking view."""
    query = f"""
        SELECT *
        FROM {_PIVOT_TABLE}
        WHERE date = '{target_date.isoformat()}'
          AND market = 'DA'
        ORDER BY total_price DESC NULLS LAST
    """
    logger.info(f"Pulling DA constraints for {target_date}")
    df = pull_from_db(query=query)
    logger.info(f"Pulled {len(df):,} DA rows")
    return df


def pull_constraints_rt_dart(start_date: date, end_date: date) -> pd.DataFrame:
    """RT and DART constraints in a date range — backward-looking view.

    Returns the long-form pivot rows (one per market). The view-model layer
    pivots them into a wide RT+DART layout per (date, constraint, contingency).
    """
    query = f"""
        SELECT *
        FROM {_PIVOT_TABLE}
        WHERE date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'
          AND market IN ('RT', 'DART')
        ORDER BY date DESC, market, total_price DESC NULLS LAST
    """
    logger.info(f"Pulling RT+DART constraints for {start_date}..{end_date}")
    df = pull_from_db(query=query)
    logger.info(f"Pulled {len(df):,} RT+DART rows")
    return df
