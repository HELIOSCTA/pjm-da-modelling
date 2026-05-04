"""Orchestration wrapper — RT Marginal Value (binding-constraint shadow prices).

Single-day, poll-and-wait variant for the hourly Prefect cadence. Mirrors
``da_marginal_value.py`` orchestration but:
  * filters on ``datetime_beginning_utc`` (PJM's internal partition key —
    an ``_ept`` filter silently drops boundary rows that have rolled to
    next-day UTC, which is what was producing empty pulls)
  * targets the current EPT day by default (RT publishes throughout the
    day; hourly cron rebuilds keep the day fresh as more data lands).

The bare-window backfill scrape lives at
``backend/scrapes/power/pjm/rt_marginal_value.py`` and is what the Prefect
flow currently invokes. Once the Prefect deployment is updated to import
this orchestration entrypoint, downtime gaps and PJM publication delays
are handled by the poll-and-land decorator instead of being silently lost.
"""
import requests
from io import StringIO
from datetime import datetime, time
from dateutil.relativedelta import relativedelta
from pathlib import Path
from zoneinfo import ZoneInfo

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

API_SCRAPE_NAME: str = "rt_marginal_value"

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


# PJM posts RT marginal value daily on business days, 11 AM-12 PM ET.
# 60s poll interval is plenty given that cadence; 2h ceiling covers late
# postings without competing with the latency-critical LMPs poll.
POLL_CEILING_SECONDS = 2 * 60 * 60  # 2 hours


def _ept_day_to_utc_window(d: datetime) -> tuple[str, str]:
    """Convert an EPT calendar day into the UTC window that covers it.

    Returns (start_utc, end_utc) as ``YYYY-MM-DD HH:MM`` strings the PJM
    API accepts. DST-aware via zoneinfo.
    """
    day_start_ept = datetime.combine(d.date(), time(0, 0), tzinfo=EPT)
    day_end_ept = datetime.combine(d.date(), time(23, 59), tzinfo=EPT)
    return (
        day_start_ept.astimezone(UTC).strftime("%Y-%m-%d %H:%M"),
        day_end_ept.astimezone(UTC).strftime("%Y-%m-%d %H:%M"),
    )


def _build_url(
    start_utc: str,
    end_utc: str,
    base_url: str = "https://api.pjm.com/api/v1/rt_marginal_value",
) -> str:
    """Build the PJM API URL for RT Marginal Value (binding-constraint shadow prices)."""

    url = (
        f"{base_url}"
        f"?rowCount=50000"
        f"&startRow=1"
        f"&datetime_beginning_utc={start_utc}%20to%20{end_utc}"
        f"&format=csv"
        f"&subscription-key={credentials.PJM_API_KEY}"
    )
    logger.info(f"Built URL: {url}")
    return url


@api_poll_policy(max_seconds=POLL_CEILING_SECONDS, wait_seconds=60)
def _wait_for_data(url: str) -> requests.Response:
    """Poll the PJM API until a non-empty response is returned.

    Raises DataNotYetAvailable on each empty poll; the decorator catches
    that and waits with fixed interval before retrying.
    """
    response = requests.get(url)
    response.raise_for_status()

    if not response.content:
        raise DataNotYetAvailable(
            "PJM RT Marginal Value API returned empty response"
        )

    logger.info("Data received from PJM API")
    return response


def _pull(
    response: requests.Response,
) -> pd.DataFrame:
    """
        Real-Time Marginal Value (binding-constraint shadow prices)
        https://dataminer2.pjm.com/feed/rt_marginal_value/definition

        Posting Frequency: Daily on business days, 11 AM-12 PM ET.
        Grain: 5-minute intervals (Apr 2018 onward); hourly before that.
    """

    df = pd.read_csv(StringIO(response.text))

    return df


def _format(
    df: pd.DataFrame,
) -> pd.DataFrame:

    # Remove BOM PJM injects on the first column header
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
    'notifications_pjm_rt_marginal_value'.

    Args:
        payload: JSON payload from pg_notify, containing:
            - table: source table name
            - operation: INSERT/UPDATE
            - rt_date: the date that just finalized
    """
    rt_date = payload.get("rt_date")
    logger.info(f"Event received for rt_date={rt_date}: {payload}")
    # Placeholder for downstream logic (e.g., dbt runs, alerts)


def main(
    target_ept_day: datetime = datetime.now(),
) -> pd.DataFrame:
    """Pull one EPT day of RT marginal value with poll-and-wait.

    Default target: today (EPT). PJM posts daily on business days
    (11 AM-12 PM ET); the poll-and-wait policy bridges runs that fire
    before the day's publish lands. Once-daily Prefect cadence (e.g.
    weekdays 11:30 ET, after publish) is the right shape — the previous
    hourly cron was overkill but harmless under this wrapper.
    """

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

        start_utc, end_utc = _ept_day_to_utc_window(target_ept_day)
        logger.section(
            f"Targeting EPT day {target_ept_day.date()} "
            f"(UTC window: {start_utc} -> {end_utc})"
        )

        logger.section("Building URL ...")
        url: str = _build_url(start_utc=start_utc, end_utc=end_utc)

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


if __name__ == "__main__":
    df = main()

    # Backfill a different EPT day:
    # df = main(target_ept_day=datetime.now() - relativedelta(days=1))
