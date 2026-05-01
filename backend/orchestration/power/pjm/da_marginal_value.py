import requests
from io import StringIO
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

import pandas as pd

from backend import credentials
from backend.orchestration.power.pjm._policies import (
    DataNotYetAvailable,
    api_poll_policy,
)
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

API_SCRAPE_NAME: str = "da_marginal_value"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
"""


POLL_CEILING_SECONDS = 2 * 60 * 60  # 2 hours


def _build_url(
    start_date: str,
    end_date: str,
    base_url: str = "https://api.pjm.com/api/v1/da_marginal_value",
) -> str:
    """Build the PJM API URL for DA Marginal Value (binding-constraint shadow prices)."""

    url = (
        f"{base_url}"
        f"?rowCount=50000"
        f"&startRow=1"
        f"&datetime_beginning_ept={start_date}%20to%20{end_date}"
        f"&format=csv"
        f"&subscription-key={credentials.PJM_API_KEY}"
    )
    logger.info(f"Built URL: {url}")
    return url


@api_poll_policy(max_seconds=POLL_CEILING_SECONDS, wait_seconds=60)
def _wait_for_data(url: str) -> requests.Response:
    """Poll the PJM API until a non-empty response is returned.

    Raises DataNotYetAvailable on each empty poll; the decorator catches that
    and waits with exponential jitter before retrying.
    """
    response = requests.get(url)
    response.raise_for_status()

    if not response.content:
        raise DataNotYetAvailable(
            "PJM DA Marginal Value API returned empty response"
        )

    logger.info("Data received from PJM API")
    return response


def _pull(
    response: requests.Response,
) -> pd.DataFrame:
    """
        Day-Ahead Marginal Value (binding-constraint shadow prices)
        https://dataminer2.pjm.com/feed/da_marginal_value/definition

        Posting Frequency: Daily
        Update Availability: Daily after the DA market clears (~13:30 EPT / 11:30 MST)
        First Available: 12/31/2010
    """

    # read data
    df = pd.read_csv(StringIO(response.text))

    return df


def _format(
    df: pd.DataFrame,
) -> pd.DataFrame:

    # Remove unwanted characters from column names
    df.columns = df.columns.str.replace('ï»¿', '')

    # Convert to datetime (format: 1/28/2026 5:00:00 AM)
    for col in [
        'datetime_beginning_utc',
        'datetime_beginning_ept',
        'datetime_ending_utc',
        'datetime_ending_ept',
    ]:
        df[col] = pd.to_datetime(df[col], format='%m/%d/%Y %I:%M:%S %p')

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
):

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=primary_key,
    )


def handle_event(payload: dict) -> None:
    """Called by the listener service when a notification arrives on
    'notifications_pjm_da_marginal_value'.

    Args:
        payload: JSON payload from pg_notify, containing:
            - table: source table name
            - operation: INSERT/UPDATE
            - da_date: the date that is now complete
    """
    da_date = payload.get("da_date")
    logger.info(f"Event received for da_date={da_date}: {payload}")
    # Placeholder for downstream logic (e.g., dbt runs, alerts)


def main(
    start_date: str = (datetime.now() + relativedelta(days=1)).strftime("%Y-%m-%d 00:00"),
    end_date: str = (datetime.now() + relativedelta(days=1)).strftime("%Y-%m-%d 23:00"),
) -> pd.DataFrame:

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=f"pjm.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    logger.header(API_SCRAPE_NAME)
    try:

        logger.section("Building URL ...")
        url: str = _build_url(start_date=start_date, end_date=end_date)

        logger.section("Waiting for data ...")
        response = _wait_for_data(url=url)

        logger.section("Pulling data ...")
        df = _pull(response=response)

        logger.section("Formatting data ...")
        df = _format(df=df)

        logger.section("Upserting data ...")
        _upsert(df=df)

        run.success(rows_processed=len(df))

    except Exception as e:
        logger.exception(f"Error pulling data: {e}")
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

    # start_date: str = (datetime.now() + relativedelta(days=0)).strftime("%Y-%m-%d 00:00")
    # end_date: str = (datetime.now() + relativedelta(days=0)).strftime("%Y-%m-%d 23:00")
    # df = main(start_date=start_date, end_date=end_date)
