import requests
from io import StringIO
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from backend import credentials
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# SCRAPE
API_SCRAPE_NAME = "seven_day_load_forecast_v1_2025_08_13"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
"""

def _pull(
        start_date: datetime = datetime.now(),
    ) -> pd.DataFrame:
    """
    Seven-Day Load Forecast
    https://dataminer2.pjm.com/feed/load_frcstd_7_day/definition

        Posting Frequency: Hourly
        Update Availability: Every half hour on the quarter E.g. 1:15 and 1:45
        Retention Time: None
        Last Updated: 10/20/2024 11:48
        First Available: N/A
    """

    base_url = "https://api.pjm.com/api/v1/load_frcstd_7_day"

    start_date_str: str = (start_date).strftime("%Y-%m-%d")
    end_date_str: str = (start_date + timedelta(days=7)).strftime("%Y-%m-%d")

    params = {
        "rowCount": 50000,
        "startRow": 1,
        "forecast_datetime_beginning_ept": f"{start_date_str} to {end_date_str}",
        "format": "csv",
        "subscription-key": "0e3e44aa6bde4d5da1699fda4511235e"
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()
    df = pd.read_csv(StringIO(response.text))

    # Remove non-ascii characters
    df.columns = df.columns.str.encode('ascii', errors='ignore').str.decode('ascii')

    # data types
    # datetime
    for col in ['evaluated_at_datetime_utc', 'evaluated_at_datetime_ept', 'forecast_datetime_beginning_utc', 'forecast_datetime_beginning_ept', 'forecast_datetime_ending_utc', 'forecast_datetime_ending_ept']:
        df[col] = pd.to_datetime(df[col])
    # string
    for col in ['forecast_area']:
        df[col] = df[col].astype(str)
    # float
    for col in ['forecast_load_mw']:
        df[col] = df[col].astype(float)

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "pjm",
        table_name: str = API_SCRAPE_NAME,
    ):

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
        primary_key = ['evaluated_at_datetime_utc', 'evaluated_at_datetime_ept', 'forecast_datetime_beginning_utc', 'forecast_datetime_beginning_ept', 'forecast_datetime_ending_utc', 'forecast_datetime_ending_ept', 'forecast_area'],
    )


def main(
        start_date: datetime = datetime.now(),
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

        # pull
        logger.section(f"Pulling data for {start_date}...")
        df = _pull(
            start_date=start_date,
        )

        logger.info(f"Forecast Execution Date: {df['evaluated_at_datetime_ept'].max()}")

        # upsert
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df)
        logger.success(f"Successfully pulled and upserted data for {start_date}!")


        run.success(rows_processed=len(df) if 'df' in locals() else 0)

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")

        run.failure(error=e)

        # raise exception
        raise

    finally:
        logging_utils.close_logging()

    if 'df' in locals() and df is not None:
        return df

"""
"""

if __name__ == "__main__":
    df = main()


