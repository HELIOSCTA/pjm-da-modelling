import requests
from io import StringIO
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta

import pandas as pd

from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# SCRAPE
API_SCRAPE_NAME = "act_sch_interchange"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
PJM Actual & Scheduled Interchange (act_sch_interchange)
https://dataminer2.pjm.com/feed/act_sch_interchange/definition

    Posting Frequency:    Hourly, with a settlement lag (~5 days
                          observed 2026-05-08 — 2026-05-03 was the
                          latest populated date).
    First Available:      2014-01-01.

Schema (probed 2026-05-08):
    datetime_beginning_utc   timestamp  hour-beginning UTC
    datetime_beginning_ept   timestamp  hour-beginning EPT
    datetime_ending_utc      timestamp  hour-ending UTC
    datetime_ending_ept      timestamp  hour-ending EPT
    tie_line                 str        e.g. ALTE, ALTW, AMIL, AEP-C, ...
    actual_flow              numeric    metered flow (MW)
    sched_flow               numeric    scheduled flow (MW)
    inadv_flow               numeric    inadvertent = actual - scheduled (MW)

Why this feed:
    The hourly settlement-grade companion to `five_min_tie_flows`. PJM
    publishes `inadv_flow` natively as actual - scheduled at the BA-tie
    level — using this feed avoids us re-deriving that signed quantity
    (which is easy to get wrong on the western seam where loop flow is
    large and uncontracted).

    Note: PJM does NOT publish per-tie *DA-cleared* schedules. The DA
    market clears NSI (a single net number) — per-tie schedules are
    bilateral / fixed / virtual and surface only through this RT
    settlement feed. So `sched_flow` here is the RT scheduled value,
    not a DA market output.

Throughput:
    ~22 ties x 24 hours = ~528 rows/day. A 30-day pull is ~16k rows,
    well under the 50k single-page cap, so each daily pull is one
    request.
"""


def _pull(
    start_date: str = datetime.now().strftime("%Y-%m-%d 00:00"),
    end_date: str = datetime.now().strftime("%Y-%m-%d 23:00"),
) -> pd.DataFrame:

    url: str = f"https://api.pjm.com/api/v1/act_sch_interchange?rowCount=50000&startRow=1&datetime_beginning_ept={start_date}%20to%20{end_date}&format=csv&subscription-key=0e3e44aa6bde4d5da1699fda4511235e"
    response = requests.get(url)

    # return empty DataFrame if API returns no data
    if not response.text.strip():
        return pd.DataFrame()

    # read data
    df = pd.read_csv(StringIO(response.text))

    # Remove non-ascii / BOM characters from column names
    df.columns = df.columns.str.encode("ascii", errors="ignore").str.decode("ascii")

    # Convert to datetime
    for col in [
        "datetime_beginning_utc",
        "datetime_beginning_ept",
        "datetime_ending_utc",
        "datetime_ending_ept",
    ]:
        df[col] = pd.to_datetime(df[col])

    return df


def _upsert(
    df: pd.DataFrame,
    schema: str = "pjm",
    table_name: str = API_SCRAPE_NAME,
    primary_key: list = [
        "datetime_beginning_utc",
        "tie_line",
    ],
) -> None:

    data_types: list = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=primary_key,
    )


def main(
    start_date: datetime = (datetime.now() - relativedelta(days=30)),
    end_date: datetime = (datetime.now() + relativedelta(days=1)),
    delta: relativedelta = relativedelta(days=1),
):
    """Pull and upsert per-tie hourly actual/scheduled/inadvertent flow.

    Default 30-day lookback accommodates the ~5-day PJM settlement lag
    (matches `rt_settlements_verified_hourly_lmps` convention). The
    upsert means re-running over already-loaded days is safe.
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

        current_date = start_date
        while current_date <= end_date:
            # dates
            params = {
                "start_date": current_date.strftime("%Y-%m-%d 00:00"),
                "end_date": current_date.strftime("%Y-%m-%d 23:00"),
            }

            # pull
            logger.section(
                f"Pulling data for {params['start_date']} to {params['end_date']}..."
            )
            df = _pull(
                start_date=params["start_date"],
                end_date=params["end_date"],
            )

            # upsert
            if df.empty:
                logger.section(
                    f"No data returned for {params['start_date']} to {params['end_date']}, skipping upsert."
                )
            else:
                logger.section(
                    f"Upserting {len(df)} rows | ties={df['tie_line'].nunique()}..."
                )
                _upsert(df)
                logger.success(
                    f"Successfully pulled and upserted data for {params['start_date']} to {params['end_date']}!"
                )

            # increment
            current_date += delta

        run.success(rows_processed=len(df) if "df" in locals() else 0)

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        # raise exception
        raise

    finally:
        logging_utils.close_logging()

    if "df" in locals() and df is not None:
        return df


"""
"""

if __name__ == "__main__":
    df = main()
