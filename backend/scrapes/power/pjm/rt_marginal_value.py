"""PJM Real-Time Marginal Value (binding-constraint shadow prices).

The PJM API partitions this feed internally on `datetime_beginning_utc`,
not `_ept`. Filtering with `datetime_beginning_ept=YYYY-MM-DD 00:00 to
YYYY-MM-DD 23:00` silently drops boundary rows whose UTC bucket has
rolled to the next day — symptom: HTTP 200 with empty body for "today".
We therefore convert each EPT day to its covering UTC window and query
on `datetime_beginning_utc`.

Rolling-window backfill (today-7 -> today+2) keeps gaps from missed runs
self-healing without polling. The companion orchestration wrapper at
`backend/orchestration/power/pjm/rt_marginal_value.py` handles the
poll-and-wait single-day pattern for the hourly Prefect cadence.
"""
import requests
from io import StringIO
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta

import pandas as pd

from backend import credentials
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# SCRAPE
API_SCRAPE_NAME = "rt_marginal_value"

# Timezone handles
EPT = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


def _ept_day_to_utc_window(d: datetime) -> tuple[str, str]:
    """Convert an EPT calendar day into the UTC window that covers it.

    Returns (start_utc, end_utc) as ``YYYY-MM-DD HH:MM`` strings the PJM
    API accepts. DST-aware via zoneinfo — EDT shifts to EST automatically.
    """
    day_start_ept = datetime.combine(d.date(), time(0, 0), tzinfo=EPT)
    day_end_ept = datetime.combine(d.date(), time(23, 59), tzinfo=EPT)
    return (
        day_start_ept.astimezone(UTC).strftime("%Y-%m-%d %H:%M"),
        day_end_ept.astimezone(UTC).strftime("%Y-%m-%d %H:%M"),
    )


def _pull(start_utc: str, end_utc: str) -> pd.DataFrame:
    """Real-Time Marginal Value (binding-constraint shadow prices).

    https://dataminer2.pjm.com/feed/rt_marginal_value/definition
    Posting Frequency: Daily on business days, 11 AM-12 PM ET.
    Grain: 5-minute intervals (Apr 2018 onward); hourly before that.
    """

    url: str = (
        f"https://api.pjm.com/api/v1/rt_marginal_value"
        f"?rowCount=50000&startRow=1"
        f"&datetime_beginning_utc={start_utc} to {end_utc}"
        f"&format=csv"
        f"&subscription-key={credentials.PJM_API_KEY}"
    )
    response = requests.get(url)
    response.raise_for_status()

    # API returns 200 + empty body when the requested window has no data
    if not response.text.strip():
        return pd.DataFrame()

    df = pd.read_csv(StringIO(response.text))

    # Strip BOM that PJM occasionally injects on the first column
    df.columns = df.columns.str.replace('ï»¿', '')

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

    total_rows = 0  # accumulate across the loop so the final metric is truthful

    try:

        logger.header(f"{API_SCRAPE_NAME}")

        current_date = start_date
        while current_date <= end_date:

            # EPT calendar day -> UTC bounds (PJM filters on UTC partition)
            start_utc, end_utc = _ept_day_to_utc_window(current_date)
            logger.section(
                f"Pulling EPT day {current_date.date()} "
                f"(UTC window: {start_utc} -> {end_utc})..."
            )
            df = _pull(start_utc=start_utc, end_utc=end_utc)

            # upsert
            if df.empty:
                logger.section(
                    f"No data returned for EPT day {current_date.date()}, "
                    "skipping upsert."
                )
            else:
                logger.section(f"Upserting {len(df)} rows...")
                _upsert(df)
                total_rows += len(df)
                logger.success(
                    f"Successfully pulled and upserted {len(df)} rows "
                    f"for EPT day {current_date.date()}!"
                )

            # increment
            current_date += delta

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


if __name__ == "__main__":
    df = main()
