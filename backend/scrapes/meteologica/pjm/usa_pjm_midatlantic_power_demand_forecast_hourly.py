"""
Meteologica: USA PJM Mid-Atlantic power demand forecast Meteologica hourly

Content ID: 2688
Content Name: USA PJM Mid-Atlantic power demand forecast Meteologica hourly
Source: https://api-markets.meteologica.com/api/v1/
Account: ISO
"""

from pathlib import Path

import pandas as pd

from backend import secrets
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)
from backend.scrapes.meteologica.auth import make_get_request

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

API_SCRAPE_NAME = "usa_pjm_midatlantic_power_demand_forecast_hourly"
CONTENT_ID = 2688

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

COLUMN_RENAME_MAP = {
    "From yyyy-mm-dd hh:mm": "forecast_period_start",
    "To yyyy-mm-dd hh:mm": "forecast_period_end",
    "UTC offset from (UTC+/-hhmm)": "utc_offset_from",
    "UTC offset to (UTC+/-hhmm)": "utc_offset_to",
    "forecast": "forecast_mw",
}

COLUMN_ORDER = [
    "content_id",
    "content_name",
    "update_id",
    "issue_date",
    "forecast_period_start",
    "forecast_period_end",
    "utc_offset_from",
    "utc_offset_to",
    "forecast_mw",
]


# --------------------------------------------------------------------------- #
# _pull
# --------------------------------------------------------------------------- #

def _pull(content_id: int = CONTENT_ID) -> tuple[pd.DataFrame, dict]:
    response = make_get_request(f"contents/{content_id}/data", account="iso")
    payload = response.json()

    metadata = {
        "content_id": payload["content_id"],
        "content_name": payload["content_name"],
        "update_id": payload["update_id"],
        "issue_date": payload["issue_date"],
        "timezone": payload.get("timezone"),
        "unit": payload.get("unit"),
    }

    df = pd.DataFrame(payload["data"])

    logger.info(
        f"Pulled {len(df)} rows | update_id={metadata['update_id']} | "
        f"issue_date={metadata['issue_date']}"
    )

    return df, metadata


# --------------------------------------------------------------------------- #
# _format
# --------------------------------------------------------------------------- #

def _format(df: pd.DataFrame, metadata: dict) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_RENAME_MAP)

    df["content_id"] = metadata["content_id"]
    df["content_name"] = metadata["content_name"]
    df["update_id"] = metadata["update_id"]
    df["issue_date"] = metadata["issue_date"]

    for col in ["forecast_mw"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["forecast_period_start", "forecast_period_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M", errors="coerce")

    ordered_cols = [c for c in COLUMN_ORDER if c in df.columns]
    df = df[ordered_cols]

    logger.info(f"Formatted DataFrame: {len(df)} rows x {len(df.columns)} cols")

    return df


# --------------------------------------------------------------------------- #
# _upsert
# --------------------------------------------------------------------------- #

def _upsert(
    df: pd.DataFrame,
    database: str = "helioscta",
    schema: str = "meteologica",
    table_name: str = API_SCRAPE_NAME,
) -> None:
    primary_keys = ["update_id", "forecast_period_start"]
    data_types = azure_postgresql.infer_sql_data_types(df=df)

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=primary_keys,
    )


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def main():
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=f"meteologica.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(f"{API_SCRAPE_NAME}")

        logger.section("Pulling latest forecast data...")
        df, metadata = _pull()

        logger.section("Formatting data...")
        df = _format(df, metadata)

        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df)
        logger.success(
            f"Successfully pulled and upserted {len(df)} rows | "
            f"update_id={metadata['update_id']}"
        )

        run.success(rows_processed=len(df))

    except Exception as e:

        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        raise

    finally:
        logging_utils.close_logging()

    if 'df' in locals() and df is not None:
        return df


if __name__ == "__main__":
    df = main()
