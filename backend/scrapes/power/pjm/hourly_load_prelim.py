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
API_SCRAPE_NAME = "hourly_load_prelim"

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
        start_date: str = (datetime.now() - relativedelta(days=30)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now() + relativedelta(days=1)).strftime("%Y-%m-%d"),
    ) -> pd.DataFrame:
    """
    Hourly Load: Preliminary
    https://dataminer2.pjm.com/feed/hrl_load_prelim/definition

        Posting Frequency: Daily
        Update Availability: Daily 04:55 a.m.
        Retention Time: Indefinitely
        Last Updated: 10/17/2024 02:56
        First Available: 8/23/2011 00:00
    """

    url = f"https://api.pjm.com/api/v1/hrl_load_prelim?rowCount=50000&startRow=1&datetime_beginning_ept={start_date}%2000:00%20to%20{end_date}%2023:00&format=csv&subscription-key=0e3e44aa6bde4d5da1699fda4511235e"
    response = requests.get(url)

    df = pd.read_csv(StringIO(response.text))

    # Remove unwanted characters from column names
    df.columns = df.columns.str.replace('ï»¿', '')

    # Convert to datetime
    for col in ['datetime_beginning_utc', 'datetime_beginning_ept', 'datetime_ending_utc', 'datetime_ending_ept']:
        df[col] = pd.to_datetime(df[col])

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "pjm",
        table_name: str = API_SCRAPE_NAME,
        primary_key = ['datetime_beginning_utc', 'load_area'],
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
        primary_key = primary_key,
    )


def main(
        start_date: str = (datetime.now() - relativedelta(days=30)).strftime("%Y-%m-%d"),
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

        # pull
        logger.section(f"Pulling data for {start_date} to {end_date}...")
        df = _pull(
            start_date=start_date,
            end_date=end_date,
        )

        # upsert
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df)
        logger.success(f"Successfully pulled and upserted data for {start_date} to {end_date}!")


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




