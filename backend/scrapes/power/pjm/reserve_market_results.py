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
API_SCRAPE_NAME = "reserve_market_results"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
Reserve Market Results (reserve_market_results)
https://dataminer2.pjm.com/feed/reserve_market_results/definition

    Posting Frequency:    Hourly
    First Available:      2013-06-14
    Retention Time:       Indefinite

NOTE: Backward-looking feed (today and forward dates return empty). Use as
a pool-side regime feature: market clearing prices (MCP) for synchronized
and non-synchronized reserves spike before or with energy LMP spikes —
direct measure of system supply tightness that the energy LMP feature set
otherwise lacks. Highest-impact addition for forward-only KNN matching
beyond the current load / weather / gas / outage / renewable groups.

Schema (17 columns):
    datetime_beginning_utc   datetime  delivery hour (UTC)
    datetime_beginning_ept   datetime  delivery hour (EPT)
    locale                   string    PJM_RTO or MAD (Mid-Atlantic-Dominion sub-zone)
    service                  string    REG | SR | PR | 30MIN
    mcp                      float     market clearing price ($/MWh)
    mcp_capped               float     MCP after offer-cap application
    reg_ccp                  float     regulation capability clearing price (REG only)
    reg_pcp                  float     regulation performance clearing price (REG only)
    as_req_mw                float     ancillary service requirement (MW)
    total_mw                 float     total cleared supply (MW)
    as_mw                    float     ancillary service supply (MW)
    ss_mw                    float     self-supply (MW)
    tier1_mw                 float     Tier 1 supply (often null in recent data)
    ircmwt2                  float     interchange / ramp-capable Tier 2 (MW)
    dsr_as_mw                float     demand-side response AS supply (MW)
    nsr_mw                   float     non-synchronized reserve (MW; PR only)
    regd_mw                  float     Reg-D supply (MW; often null in recent data)

Service codes:
    REG    Regulation (RegA + RegD, with separate capability/performance prices)
    SR     Synchronized Reserve
    PR     Primary Reserve (synchronized + non-synchronized)
    30MIN  30-Minute Reserve

Locales:
    PJM_RTO   System-wide RTO market
    MAD       Mid-Atlantic-Dominion sub-zone (locational pricing for SR/PR)

Throughput:
    ~190-200 rows/day (2 locales x 4 services x ~24 hours, with some
    service/locale combinations not present every hour). 50,000-row cap
    supports ~250 days per request.

NULL handling:
    Several numeric columns are NULL where they do not apply to a given
    service (e.g., reg_ccp/reg_pcp only populated for service=REG). Use
    pd.to_numeric(errors='coerce') to preserve NaN rather than astype(float).
"""

_NUMERIC_COLS: list[str] = [
    'mcp',
    'mcp_capped',
    'reg_ccp',
    'reg_pcp',
    'as_req_mw',
    'total_mw',
    'as_mw',
    'ss_mw',
    'tier1_mw',
    'ircmwt2',
    'dsr_as_mw',
    'nsr_mw',
    'regd_mw',
]


def _pull(
        start_date: str = datetime.now().strftime("%Y-%m-%d 00:00"),
        end_date: str = datetime.now().strftime("%Y-%m-%d 23:00"),
    ) -> pd.DataFrame:
    """Pull one window of cleared reserve market results."""

    base_url = "https://api.pjm.com/api/v1/reserve_market_results"

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
    for col in ['locale', 'service']:
        df[col] = df[col].astype(str)
    # numeric — coerce so NULL cells (service-specific columns) survive as NaN
    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

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
            'locale',
            'service',
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
                    f"locales={df['locale'].nunique()} | "
                    f"services={df['service'].nunique()} | "
                    f"latest hour={df['datetime_beginning_ept'].max()} | "
                    f"mcp range={df['mcp'].min():.2f}-{df['mcp'].max():.2f}"
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
        start_date: datetime = datetime(2013, 6, 14),
        end_date: datetime = datetime.now(),
        delta: relativedelta = relativedelta(days=180),
    ):
    """One-shot historical backfill — call manually, not from the scheduler.

    Feed begins 2013-06-14. With ~190-200 rows/day, 180-day chunks (~36k rows)
    sit comfortably under the 50,000-row API cap.
    """
    main(start_date=start_date, end_date=end_date, delta=delta)


"""
"""

if __name__ == "__main__":
    df = main()
