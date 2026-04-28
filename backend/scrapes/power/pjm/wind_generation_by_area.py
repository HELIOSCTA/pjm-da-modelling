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
API_SCRAPE_NAME = "wind_generation_by_area"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
Wind Generation (wind_gen)
https://dataminer2.pjm.com/feed/wind_gen/definition

    Posting Frequency:    Hourly
    First Available:      2011-01-01
    Retention Time:       Indefinite

NOTE: This is *realized* wind generation, not a vintage-stamped forecast.
The feed has no ``evaluated_at_*`` column. Use it as the realized-observation
side of an AnEn (forecast, observation) pair — i.e. as a richer pool/label
input alongside ``pjm_fuel_mix_hourly`` (which is system-wide and starts
2020-01-01). Wind forecast vintages still need to be captured by daily
snapshotting of the rolling forecast feed; PJM does not publish a
``wind_frcstd_hist`` archive (404 confirmed).

Schema:
    datetime_beginning_utc    datetime  delivery hour (UTC)
    datetime_beginning_ept    datetime  delivery hour (EPT)
    area                      string    PJM area (MIDATL, OTHER, RFC, RTO, SOUTH, WEST)
    wind_generation_mw        float     realized wind generation (MW)

Areas observed: MIDATL, OTHER, RFC, RTO, SOUTH, WEST (6).
"""


def _pull(
        start_date: str = datetime.now().strftime("%Y-%m-%d 00:00"),
        end_date: str = datetime.now().strftime("%Y-%m-%d 23:00"),
    ) -> pd.DataFrame:
    """Pull one window of realized wind generation by area."""

    base_url = "https://api.pjm.com/api/v1/wind_gen"

    params = {
        "rowCount": 50000,
        "startRow": 1,
        "datetime_beginning_ept": f"{start_date} to {end_date}",
        "format": "csv",
        "subscription-key": "0e3e44aa6bde4d5da1699fda4511235e",
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()

    if not response.text.strip():
        return pd.DataFrame()

    df = pd.read_csv(StringIO(response.text))

    # Remove non-ascii / BOM characters from column names
    df.columns = df.columns.str.encode('ascii', errors='ignore').str.decode('ascii')

    if df.empty:
        return df

    # data types
    # datetime
    for col in ['datetime_beginning_utc', 'datetime_beginning_ept']:
        df[col] = pd.to_datetime(df[col])
    # string
    for col in ['area']:
        df[col] = df[col].astype(str)
    # float
    for col in ['wind_generation_mw']:
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
        primary_key = [
            'datetime_beginning_utc',
            'area',
        ],
    )


def main(
        start_date: datetime = (datetime.now() - relativedelta(days=7)),
        end_date: datetime = (datetime.now() + relativedelta(days=2)),
        delta: relativedelta = relativedelta(days=1),
    ):
    """Iterate the date range in ``delta``-sized chunks, pulling and upserting each.

    ~144 rows/day (24h x 6 areas), so 50,000-row API cap supports up to
    ~347 days per request. For backfill from 2011-01-01, ``relativedelta(days=90)``
    keeps each chunk well under the cap.
    """

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

        total_rows = 0
        current_date = start_date
        while current_date <= end_date:

            chunk_end = min(current_date + delta - relativedelta(days=1), end_date)

            params = {
                "start_date": current_date.strftime("%Y-%m-%d 00:00"),
                "end_date": chunk_end.strftime("%Y-%m-%d 23:00"),
            }

            logger.section(f"Pulling data for {params['start_date']} to {params['end_date']}...")
            df = _pull(
                start_date=params['start_date'],
                end_date=params['end_date'],
            )

            if df.empty:
                logger.section(f"No data returned for {params['start_date']} to {params['end_date']}, skipping upsert.")
            else:
                logger.info(
                    f"Pulled {len(df)} rows | "
                    f"areas={df['area'].nunique()} | "
                    f"latest hour={df['datetime_beginning_ept'].max()}"
                )
                logger.section(f"Upserting {len(df)} rows...")
                _upsert(df)
                logger.success(f"Successfully pulled and upserted data for {params['start_date']} to {params['end_date']}!")
                total_rows += len(df)

            current_date = chunk_end + relativedelta(days=1)

        run.success(rows_processed=total_rows)

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        # raise exception
        raise

    finally:
        logging_utils.close_logging()

    if 'df' in locals() and df is not None:
        return df


def _backfill(
        start_date: datetime = datetime(2011, 1, 1),
        end_date: datetime = datetime.now(),
        delta: relativedelta = relativedelta(days=90),
    ):
    """One-shot historical backfill — call manually, not from the scheduler.

    Feed begins 2011-01-01. Wider delta reduces request count vs the daily
    incremental in main()'s default.
    """
    main(start_date=start_date, end_date=end_date, delta=delta)


"""
"""

if __name__ == "__main__":
    df = main()