"""
Energy Aspects - PJM installed capacity by fuel type (MW).

Monthly Energy Aspects forecast datasets for PJM installed nameplate capacity.
"""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
if sys.path[0] != str(PROJECT_ROOT):
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from backend.scrapes.energy_aspects import energy_aspects_api_utils as ea_api
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

API_SCRAPE_NAME = "ea_pjm_installed_capacity_monthly"
RAW_TABLE_NAME = "us_installed_capacity_by_iso_and_fuel_type"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=False,
)

DATASET_IDS = [
    24359,  # natural gas
    24360,  # coal
    24361,  # nuclear
    24362,  # oil products
    24363,  # solar
    24364,  # onshore wind
    24365,  # hydro
    582722,  # offshore wind
    582727,  # battery
]

COLUMN_MAP = {
    "24359": "fcst_ng_installed_capacity_in_pjm_in_mw",
    "24360": "fcst_coal_installed_capacity_in_pjm_in_mw",
    "24361": "fcst_nuclear_installed_capacity_in_pjm_in_mw",
    "24362": "fcst_oil_products_installed_capacity_in_pjm_in_mw",
    "24363": "fcst_solar_installed_capacity_in_pjm_in_mw",
    "24364": "fcst_onshore_wind_installed_capacity_in_pjm_in_mw",
    "24365": "fcst_hydro_installed_capacity_in_pjm_in_mw",
    "582722": "fcst_offshore_wind_installed_capacity_in_pjm_in_mw",
    "582727": "fcst_battery_installed_capacity_in_pjm_in_mw",
}


def _pull(
    date_from: str = "2012-01-01",
    date_to: str = "2079-01-01",
) -> pd.DataFrame:
    logger.info(f"Pulling {len(DATASET_IDS)} PJM installed capacity datasets from EA API...")
    return ea_api.pull_timeseries(DATASET_IDS, date_from=date_from, date_to=date_to)


def _format(df: pd.DataFrame) -> pd.DataFrame:
    missing_columns = sorted(set(COLUMN_MAP).difference(df.columns))
    if missing_columns:
        raise ValueError(f"Missing expected EA dataset columns: {missing_columns}")

    result = df[["date", *COLUMN_MAP.keys()]].rename(columns=COLUMN_MAP).copy()
    result["date"] = pd.to_datetime(result["date"])

    for column in COLUMN_MAP.values():
        result[column] = pd.to_numeric(result[column], errors="coerce")

    return ea_api.make_postgres_safe_columns(result)


def _upsert(
    df: pd.DataFrame,
    schema: str = "energy_aspects",
    table_name: str = RAW_TABLE_NAME,
) -> None:
    data_types = azure_postgresql.infer_sql_data_types(df=df)
    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=["date"],
    )


def main() -> pd.DataFrame:
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="energy_aspects",
        target_table=f"energy_aspects.{RAW_TABLE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        df = _pull()

        if df.empty:
            logger.section("No data returned, skipping upsert.")
        else:
            df = _format(df)
            logger.section(f"Upserting {len(df)} rows, {len(df.columns)} columns...")
            _upsert(df)
            logger.success(f"Upserted {len(df)} rows.")

        run.success(rows_processed=len(df))

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()

    return df


if __name__ == "__main__":
    df = main()
