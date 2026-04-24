from datetime import datetime
from pathlib import Path

import pandas as pd
import gridstatus

from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

from backend import credentials

# SCRAPE
API_SCRAPE_NAME = "pjm_solar_forecast_hourly"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
"""

def _get_timestamp_columns(
        df: pd.DataFrame,
        interval_start_column: str = "Interval Start",
        interval_end_column: str = "Interval End",
        publish_column: str = "Publish Time",
        timestamp_format: str = "%Y-%m-%d %H:%M:%S",
    ):

    # timezone_offset
    datetime_obj = df[interval_start_column][0]
    timezone_offset = int(datetime_obj.strftime("%z")[1:3])

    # interval_start
    interval_start = pd.to_datetime(df[interval_start_column])
    interval_start_local = interval_start.dt.tz_localize(None)
    interval_start_utc = interval_start_local + pd.Timedelta(hours=timezone_offset)

    # interval_end
    interval_end_local = pd.to_datetime(df[interval_end_column])
    interval_end_utc = interval_end_local + pd.Timedelta(hours=timezone_offset)

    # publish_time
    publish_time = pd.to_datetime(df[publish_column])
    publish_time_local = publish_time.dt.tz_localize(None)
    publish_time_utc = publish_time_local + pd.Timedelta(hours=timezone_offset)

    # Format timestamps
    df["interval_start_local"] = interval_start_local.dt.strftime(timestamp_format)
    df["interval_start_utc"] = interval_start_utc.dt.strftime(timestamp_format)
    df["interval_end_local"] = interval_end_local.dt.strftime(timestamp_format)
    df["interval_end_utc"] = interval_end_utc.dt.strftime(timestamp_format)
    df["publish_time_local"] = publish_time_local.dt.strftime(timestamp_format)
    df["publish_time_utc"] = publish_time_utc.dt.strftime(timestamp_format)

    # drop time
    df = df.drop(columns=[interval_start_column, interval_end_column, publish_column])

    # After creating the timestamp columns, reorder the columns
    timestamp_columns = ['interval_start_local', 'interval_start_utc', 'interval_end_local', 'interval_end_utc', 'publish_time_local', 'publish_time_utc']
    other_columns = [col for col in df.columns if col not in timestamp_columns]
    df = df[timestamp_columns + other_columns]

    return df


def _format(
    df: pd.DataFrame
) -> pd.DataFrame:

    # timestamps
    df = _get_timestamp_columns(df)

    # rename columns
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # dtypes
    columns = ['solar_forecast_btm', 'solar_forecast']
    df[columns] = df[columns].astype(float)

    # check data types
    cols = df.columns.tolist()
    data_types: list = azure_postgresql.infer_sql_data_types(df=df)
    for col, dtype in zip(cols, data_types):
        logger.info(f"\t{col} .. {dtype}")

    return df


def _pull(
        date,
    ):

    iso = gridstatus.PJM(api_key=credentials.PJM_API_KEY)
    df = iso.get_solar_forecast_hourly(date=date)

    return df


def _upsert(
        df: pd.DataFrame,
        database: str = "helioscta",
        schema: str = "gridstatus",
        table_name: str = API_SCRAPE_NAME,
    ):

    primary_key_candidates = ['interval_start_local', 'interval_start_utc', 'interval_end_local', 'interval_end_utc', 'publish_time_local', 'publish_time_utc']
    primary_keys = [col for col in primary_key_candidates if col in df.columns]
    if not primary_keys:
        raise ValueError(
            f"No valid primary keys found for {schema}.{table_name}. "
            f"Expected one of {primary_key_candidates}, got columns={df.columns.tolist()}"
        )

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
        primary_key = primary_keys,
    )


def main(
        dates: list[str] = ["latest"],
    ):

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=f"gridstatus.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:

        logger.header(f"{API_SCRAPE_NAME}")

        for date in dates:

            logger.section(f"Pulling data for {date}...")
            df = _pull(date=date)

            # format
            logger.section(f"Formatting data...")
            df = _format(df)

            # upsert
            logger.section(f"Upserting {len(df)} rows...")
            _upsert(df)
            logger.success(f"Successfully pulled and upserted data for {date}!")


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



