import requests
from io import StringIO
from datetime import datetime
from pathlib import Path

import pandas as pd

from backend import credentials
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# SCRAPE
API_SCRAPE_NAME = "rt_short_term_mv_override"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
"""

def _pull() -> pd.DataFrame:
    """
        Short-Term Transmission Constraint Penalty Factor Overrides
        https://dataminer2.pjm.com/feed/rt_short_term_mv_override/definition

        Event log: per-constraint temporary penalty-factor overrides with
        precise effective / terminate timestamps. Pulled in full each run.
    """

    url: str = (
        "https://api.pjm.com/api/v1/rt_short_term_mv_override"
        "?rowCount=50000&startRow=1&format=csv"
        f"&subscription-key={credentials.PJM_API_KEY}"
    )
    response = requests.get(url)
    response.raise_for_status()

    # return empty DataFrame if API returns no data
    if not response.text.strip():
        return pd.DataFrame()

    # read data
    df = pd.read_csv(StringIO(response.text))

    # Remove unwanted characters from column names
    df.columns = df.columns.str.replace('ï»¿', '')

    # Convert to datetime
    for col in [
        'posted_day',
        'effective_datetime_utc',
        'effective_datetime_ept',
        'terminate_datetime_utc',
        'terminate_datetime_ept',
    ]:
        df[col] = pd.to_datetime(df[col])

    return df


def _upsert(
        df: pd.DataFrame,
        schema: str = "pjm",
        table_name: str = API_SCRAPE_NAME,
        primary_key: list = [
            'constraint_name',
            'contingency_description',
            'effective_datetime_utc',
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


def main():

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
        logger.section("Pulling full short-term-override snapshot...")
        df = _pull()

        # upsert
        if df.empty:
            logger.section("No data returned, skipping upsert.")
            run.success(rows_processed=0)
        else:
            logger.section(f"Upserting {len(df)} rows...")
            _upsert(df)
            logger.success(f"Successfully pulled and upserted {len(df)} rows!")
            run.success(rows_processed=len(df))

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
