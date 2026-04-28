import requests
from io import StringIO
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta

import pandas as pd

from backend import credentials
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# SCRAPE
API_SCRAPE_NAME = "five_min_solar_generation_v1_2026_APR_28"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
Five-Minute Solar Generation
https://dataminer2.pjm.com/feed/five_min_solar_generation/definition

    Posting Frequency:    Every 5 minutes

This is *realized* RTO-level solar generation at 5-min grain. Unlike the
hourly ``solar_generation_by_area`` feed, this one is system-wide only — there
is no ``area`` column. Used as an intra-day, lower-priority companion to the
hourly feed, which pads forward-dated and unreported hours with 0 (~2-day
publication gap). Only fills the RTO row in pjm_solar_gen_rt_hourly; sub-region
rows (MIDATL/WEST/SOUTH) remain HOURLY-only.

Schema (verified 2026-04-28):
    datetime_beginning_utc    datetime  5-min interval (UTC)
    datetime_beginning_ept    datetime  5-min interval (EPT)
    solar_generation_mw       float     realized 5-min RTO solar generation (MW)
"""


def _pull(
        start_date: str = (datetime.now() - relativedelta(days=5)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now() + relativedelta(days=1)).strftime("%Y-%m-%d"),
    ) -> pd.DataFrame:
    """Pull one window of 5-min realized solar generation by area."""

    url = (
        "https://api.pjm.com/api/v1/five_min_solar_generation"
        f"?rowCount=50000&startRow=1"
        f"&datetime_beginning_ept={start_date}%2000:00%20to%20{end_date}%2023:55"
        f"&format=csv&subscription-key=0e3e44aa6bde4d5da1699fda4511235e"
    )
    response = requests.get(url)
    response.raise_for_status()

    if not response.text.strip():
        return pd.DataFrame()

    df = pd.read_csv(StringIO(response.text))

    # Remove non-ascii / BOM characters from column names
    df.columns = df.columns.str.encode('ascii', errors='ignore').str.decode('ascii')

    if df.empty:
        return df

    # data types
    for col in ['datetime_beginning_utc', 'datetime_beginning_ept']:
        df[col] = pd.to_datetime(df[col])
    for col in ['solar_generation_mw']:
        df[col] = df[col].astype(float)

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "pjm",
        table_name: str = API_SCRAPE_NAME,
    ):

    primary_keys = ['datetime_beginning_utc']

    data_types = azure_postgresql.get_table_dtypes(
        database = database,
        schema = schema,
        table_name = table_name,
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database = database,
        schema = schema,
        table_name = table_name,
        df = df,
        columns = df.columns.tolist(),
        data_types = data_types,
        primary_key = primary_keys,
    )


def main(
        start_date: str = (datetime.now() - relativedelta(days=5)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now() + relativedelta(days=1)).strftime("%Y-%m-%d"),
    ):

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=f"pjm.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(f"{API_SCRAPE_NAME}")

        logger.section(f"Pulling data for {start_date} to {end_date}...")
        df = _pull(start_date=start_date, end_date=end_date)

        if df.empty:
            logger.section(f"No data returned for {start_date} to {end_date}, skipping upsert.")
        else:
            logger.section(f"Upserting {len(df)} rows...")
            _upsert(df)
            logger.success(f"Successfully pulled and upserted data for {start_date} to {end_date}!")

        run.success(rows_processed=len(df) if 'df' in locals() else 0)

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()

    if 'df' in locals() and df is not None:
        return df


"""
"""

if __name__ == "__main__":
    df = main()
