import requests
from io import StringIO
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
API_SCRAPE_NAME = "ancillary_services"

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
    Ancillary Services
    https://dataminer2.pjm.com/feed/ancillary_services/definition

        Services: MAD Non-Synchronized Reserve, MAD Secondary Reserve,
                  MAD Synchronized Reserve, RTO Mileage Ratio, RTO Regulation Capability
        Units: Price ($/MWh), Ratio

        Key signal: When MAD Synchronized Reserve price > $0, a reserve scarcity
        adder is active. This is the leading indicator for RT price spikes driven
        by reserve shortage (e.g., 3/24 HE22 Dom $220 event).
    """

    url = f"https://api.pjm.com/api/v1/ancillary_services?rowCount=50000&startRow=1&datetime_beginning_ept={start_date}%2000:00%20to%20{end_date}%2023:00&format=csv&subscription-key=0e3e44aa6bde4d5da1699fda4511235e"
    response = requests.get(url)

    df = pd.read_csv(StringIO(response.text))

    # Remove unwanted characters from column names
    df.columns = df.columns.str.replace('ï»¿', '')

    # Convert to datetime
    for col in ['datetime_beginning_utc', 'datetime_beginning_ept']:
        df[col] = pd.to_datetime(df[col])

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
        primary_key = ['datetime_beginning_utc', 'datetime_beginning_ept', 'ancillary_service'],
    )


def main(
        start_date: datetime = (datetime.now() - relativedelta(days=5)),
        end_date: datetime = (datetime.now() + relativedelta(days=1)),
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

        # dates
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        # pull
        logger.section(f"Pulling data for {params['start_date']} to {params['end_date']}...")
        df = _pull(
            start_date=params['start_date'],
            end_date=params['end_date'],
        )

        # upsert
        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df)
        logger.success(f"Successfully pulled and upserted data for {params['start_date']} to {params['end_date']}!")


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
