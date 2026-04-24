from datetime import datetime, timedelta
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
API_SCRAPE_NAME = "seven_day_outage_forecast"

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
        start_date: str = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
    ) -> pd.DataFrame:
    """
    Generation Outage for Seven Days by Type
    https://dataminer2.pjm.com/feed/gen_outages_by_type/definition

        Posting Frequency: Daily
        Update Availability: Daily 06:00 a.m. EPT (04:00 a.m. MST)
        Retention Time: Indefinitely
        Last Updated: 10/20/2024 04:02
        First Available: 5/26/2015 00:00
    """

    url = f"https://api.pjm.com/api/v1/gen_outages_by_type?rowCount=50000&startRow=1&forecast_execution_date_ept={start_date}%20to%20{end_date}&format=csv&subscription-key=0e3e44aa6bde4d5da1699fda4511235e"

    df = pd.read_csv(url)

    # Remove non-ascii characters
    df.columns = df.columns.str.encode('ascii', errors='ignore').str.decode('ascii')

    # Convert first two columns to datetime
    df[df.columns[0]] = pd.to_datetime(df[df.columns[0]])
    df[df.columns[1]] = pd.to_datetime(df[df.columns[1]])

    # Rename 'forecast_execution_date_ept' to 'forecast_execution_date'
    df.rename(columns={'forecast_execution_date_ept': 'forecast_execution_date'}, inplace=True)

    # Convert to date
    for col in ['forecast_execution_date', 'forecast_date']:
        df[col] = pd.to_datetime(df[col]).dt.date

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
        primary_key = ['forecast_execution_date', 'forecast_date', 'region'],
    )


def main(
        start_date: str = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d"),
        end_date: str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
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


