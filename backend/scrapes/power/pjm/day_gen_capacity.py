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
API_SCRAPE_NAME = "day_gen_capacity"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
Daily Generation Capacity (day_gen_capacity)
https://dataminer2.pjm.com/feed/day_gen_capacity/definition

    Posting Frequency:    Daily, posted ~05:00 a.m. EPT
    First Available:      2012-01-01
    Retention Time:       Indefinite

NOTE: This is a *backward-looking* feed. Today's and tomorrow's values are
not published — only previously-delivered hours. Use as a pool-side
realized supply feature; for a forward (query-side) value, roll yesterday's
``eco_max`` forward as a proxy or use ``total_committed`` directly (the RPM
value is structural and effectively flat day-to-day).

Schema:
    bid_datetime_beginning_utc   datetime  delivery hour (UTC)
    bid_datetime_beginning_ept   datetime  delivery hour (EPT)
    eco_max                      float     economic max MW offered into the energy market
                                           (cost-based offers, excludes emergency units,
                                           does NOT reflect outages on the system)
    emerg_max                    float     emerg + economic max MW offered (includes emergency units)
    total_committed              float     RPM-committed installed capacity (incl. FRR units).
                                           Flat intra-day; changes step-wise at RPM auction boundaries.

Filter convention:
    The API filter parameter is ``bid_datetime_beginning_ept``. Range syntax:
    ``YYYY-MM-DD HH:MM to YYYY-MM-DD HH:MM``.

Throughput:
    24 rows/day (system-wide, no area dimension). 50,000-row cap supports
    ~5,800 days per request — backfill from 2012-01-01 fits in a handful of chunks.
"""


def _pull(
        start_date: str = datetime.now().strftime("%Y-%m-%d 00:00"),
        end_date: str = datetime.now().strftime("%Y-%m-%d 23:00"),
    ) -> pd.DataFrame:
    """Pull one window of daily generation capacity (system-wide hourly)."""

    base_url = "https://api.pjm.com/api/v1/day_gen_capacity"

    params = {
        "rowCount": 50000,
        "startRow": 1,
        "bid_datetime_beginning_ept": f"{start_date} to {end_date}",
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
    for col in ['bid_datetime_beginning_utc', 'bid_datetime_beginning_ept']:
        df[col] = pd.to_datetime(df[col])
    # float
    for col in ['eco_max', 'emerg_max', 'total_committed']:
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
            'bid_datetime_beginning_utc',
        ],
    )


def main(
        start_date: datetime = (datetime.now() - relativedelta(days=7)),
        end_date: datetime = (datetime.now() + relativedelta(days=2)),
        delta: relativedelta = relativedelta(days=1),
    ):
    """Iterate the date range in ``delta``-sized chunks, pulling and upserting each.

    Default window pulls the last 7 days through 2 days forward. Forward-dated
    hours return empty (feed is backward-only); the loop handles that gracefully.
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
                    f"hours={df['bid_datetime_beginning_ept'].nunique()} | "
                    f"latest hour={df['bid_datetime_beginning_ept'].max()} | "
                    f"eco_max range={df['eco_max'].min():.0f}-{df['eco_max'].max():.0f} MW"
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
        start_date: datetime = datetime(2012, 1, 1),
        end_date: datetime = datetime.now(),
        delta: relativedelta = relativedelta(days=180),
    ):
    """One-shot historical backfill — call manually, not from the scheduler.

    Feed begins 2012-01-01. With only ~24 rows/day, 180-day chunks (~4,320 rows)
    sit comfortably under the 50,000-row API cap.
    """
    main(start_date=start_date, end_date=end_date, delta=delta)


"""
"""

if __name__ == "__main__":
    df = main()