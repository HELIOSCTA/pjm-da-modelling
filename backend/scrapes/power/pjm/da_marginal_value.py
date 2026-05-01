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
API_SCRAPE_NAME = "da_marginal_value"

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
        start_date: str = datetime.now().strftime("%Y-%m-%d 00:00"),
        end_date: str = datetime.now().strftime("%Y-%m-%d 23:00"),
    ) -> pd.DataFrame:
    """
        Day-Ahead Marginal Value (binding-constraint shadow prices)
        https://dataminer2.pjm.com/feed/da_marginal_value/definition

        Posting Frequency: Daily
        First Available: 12/31/2010
    """

    url: str = f"https://api.pjm.com/api/v1/da_marginal_value?rowCount=50000&startRow=1&datetime_beginning_ept={start_date}%20to%20{end_date}&format=csv&subscription-key=0e3e44aa6bde4d5da1699fda4511235e"
    response = requests.get(url)

    # return empty DataFrame if API returns no data
    if not response.text.strip():
        return pd.DataFrame()

    # read data
    df = pd.read_csv(StringIO(response.text))

    # Remove unwanted characters from column names
    df.columns = df.columns.str.replace('ï»¿', '')

    # Convert to datetime
    for col in [
        'datetime_beginning_utc',
        'datetime_beginning_ept',
        'datetime_ending_utc',
        'datetime_ending_ept',
    ]:
        df[col] = pd.to_datetime(df[col])

    return df


def _upsert(
        df: pd.DataFrame,
        schema: str = "pjm",
        table_name: str = API_SCRAPE_NAME,
        primary_key: list = [
            'datetime_beginning_utc',
            'monitored_facility',
            'contingency_facility',
        ],
    ) -> None:

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        schema = schema,
        table_name = table_name,
        df = df,
        columns = df.columns.tolist(),
        data_types = data_types,
        primary_key = primary_key,
    )


def main(
        start_date: datetime = (datetime.now() - relativedelta(days=7)),
        end_date: datetime = (datetime.now() + relativedelta(days=2)),
        delta: relativedelta = relativedelta(days=1),
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

        current_date = start_date
        while current_date <= end_date:

            # dates
            params = {
                "start_date": current_date.strftime("%Y-%m-%d 00:00"),
                "end_date": current_date.strftime("%Y-%m-%d 23:00"),
            }

            # pull
            logger.section(f"Pulling data for {params['start_date']} to {params['end_date']}...")
            df = _pull(
                start_date=params['start_date'],
                end_date=params['end_date'],
            )

            # upsert
            if df.empty:
                logger.section(f"No data returned for {params['start_date']} to {params['end_date']}, skipping upsert.")
            else:
                logger.section(f"Upserting {len(df)} rows...")
                _upsert(df)
                logger.success(f"Successfully pulled and upserted data for {params['start_date']} to {params['end_date']}!")

            # increment
            current_date += delta

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
