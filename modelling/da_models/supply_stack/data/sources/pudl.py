"""PUDL (Catalyst Cooperative) S3 reader for EIA-860/923 generator and plant data.

Reads anonymously from ``s3://pudl.catalyst.coop/{channel}/<table>.parquet``.
Two tables are exposed:

- ``out_eia__monthly_generators`` -- per-(plant, generator, month) EIA-860
  capacity + EIA-923 operations: ``capacity_mw``, ``summer_capacity_mw``,
  ``minimum_load_mw``, ``unit_heat_rate_mmbtu_per_mwh``,
  ``fuel_cost_per_mmbtu``, ``fuel_type_code_pudl``, ``prime_mover_code``,
  ``technology_description``, ``operational_status``.
- ``core_eia860__scd_plants`` -- per-(plant, year) EIA-860 plant dim with
  ``transmission_distribution_owner_name`` (the authoritative source for
  PJM zone assignment, see ``TD_OWNER_TO_ZONE`` in
  ``builders/build_from_pudl.py``).

Filters to PJM balancing authority (``balancing_authority_code_eia ==
'PJM'``) on read. Not a runnable script.
"""

from __future__ import annotations

from typing import Literal

import pandas as pd
import pyarrow.parquet as pq
from pyarrow import fs as pafs

PUDL_S3_BUCKET = "pudl.catalyst.coop"
PUDL_S3_REGION = "us-west-2"
DEFAULT_CHANNEL = "stable"

GENERATORS_TABLE = "out_eia__monthly_generators"
PLANTS_TABLE = "core_eia860__scd_plants"

_PJM_BA_CODE = "PJM"

GENERATORS_COLUMNS: list[str] = [
    "plant_id_eia",
    "generator_id",
    "report_date",
    "plant_name_eia",
    "utility_name_eia",
    "balancing_authority_code_eia",
    "fuel_type_code_pudl",
    "technology_description",
    "prime_mover_code",
    "operational_status",
    "capacity_mw",
    "summer_capacity_mw",
    "winter_capacity_mw",
    "minimum_load_mw",
    "unit_heat_rate_mmbtu_per_mwh",
    "fuel_cost_per_mmbtu",
    "state",
]

PLANTS_COLUMNS: list[str] = [
    "plant_id_eia",
    "report_date",
    "balancing_authority_code_eia",
    "transmission_distribution_owner_id",
    "transmission_distribution_owner_name",
]


def _s3_filesystem() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(region=PUDL_S3_REGION, anonymous=True)


def pull_generators(
    channel: Literal["stable", "nightly"] = DEFAULT_CHANNEL,
    pjm_only: bool = True,
) -> pd.DataFrame:
    """Pull EIA-860/923 monthly generator rows from PUDL.

    Filters to PJM balancing authority on read when ``pjm_only=True``.
    The full table is ~10M rows nationally; PJM is ~120k rows.
    """
    fs = _s3_filesystem()
    path = f"{PUDL_S3_BUCKET}/{channel}/{GENERATORS_TABLE}.parquet"
    filters = (
        [("balancing_authority_code_eia", "==", _PJM_BA_CODE)] if pjm_only else None
    )
    table = pq.read_table(
        path, filesystem=fs, columns=GENERATORS_COLUMNS, filters=filters
    )
    df = table.to_pandas()
    df["report_date"] = pd.to_datetime(df["report_date"])
    return df


def pull_plants(
    channel: Literal["stable", "nightly"] = DEFAULT_CHANNEL,
    pjm_only: bool = True,
) -> pd.DataFrame:
    """Pull EIA-860 SCD plant rows from PUDL.

    Filters to PJM BA on read when ``pjm_only=True``. ~7k rows nationally.
    """
    fs = _s3_filesystem()
    path = f"{PUDL_S3_BUCKET}/{channel}/{PLANTS_TABLE}.parquet"
    filters = (
        [("balancing_authority_code_eia", "==", _PJM_BA_CODE)] if pjm_only else None
    )
    table = pq.read_table(path, filesystem=fs, columns=PLANTS_COLUMNS, filters=filters)
    df = table.to_pandas()
    df["report_date"] = pd.to_datetime(df["report_date"])
    return df
