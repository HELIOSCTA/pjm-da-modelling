from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.utils import logging_utils, pipeline_run_logger

from backend.scrapes.ice_python import utils
from backend.scrapes.ice_python.symbols.next_day_gas_symbols import (
    get_next_day_gas_symbols,
)

API_SCRAPE_NAME = "next_day_gas_v1_2025_dec_16"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


# ---------------------------------------------------------------------------
# Symbol loading & validation
# ---------------------------------------------------------------------------

_REQUIRED_KEYS = {"symbol", "description"}


def _load_symbols() -> list[dict]:
    """Load next-day gas symbols from the central registry and validate."""
    symbols = get_next_day_gas_symbols()

    if not symbols:
        raise ValueError(
            "No next-day gas symbols returned from symbol registry. "
            "Check backend/scrapes/ice_python/symbols/next_day_gas_symbols.py"
        )

    for idx, entry in enumerate(symbols):
        missing = _REQUIRED_KEYS - set(entry.keys())
        if missing:
            raise ValueError(
                f"Symbol entry [{idx}] is missing required keys: {missing}. "
                f"Entry: {entry}"
            )
        if not entry["symbol"] or not entry["symbol"].strip():
            raise ValueError(
                f"Symbol entry [{idx}] has an empty 'symbol' value. "
                f"Description: {entry.get('description', 'N/A')}"
            )

    return symbols


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------

def _pull(
    symbol: str,
    data_type: str = "VWAP Close",
    granularity: str = "D",
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    return utils.get_timeseries(
        symbol=symbol,
        data_type=data_type,
        granularity=granularity,
        start_date=start_date,
        end_date=end_date,
        date_col=date_col,
        date_format=date_format,
    )


def _format(
    df: pd.DataFrame,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    return utils.format_timeseries(
        df=df,
        date_col=date_col,
        date_format=date_format,
    )


def _upsert(
    df: pd.DataFrame,
    database: str = utils.DEFAULT_DATABASE,
    schema: str = utils.DEFAULT_SCHEMA,
    table_name: str = API_SCRAPE_NAME,
) -> None:
    utils.upsert_timeseries(
        df=df,
        database=database,
        schema=schema,
        table_name=table_name,
    )


def main(
    data_type: str = "VWAP Close",
    granularity: str = "D",
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    start_date = start_date or utils.default_start_date()
    end_date = end_date or utils.default_end_date()

    symbols = _load_symbols()

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="ice_python",
        target_table=f"{utils.DEFAULT_SCHEMA}.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    frames: list[pd.DataFrame] = []
    total_rows = 0
    processed_symbols = 0
    try:
        logger.header(API_SCRAPE_NAME)
        logger.info(f"Loaded {len(symbols)} symbols from registry:")
        for entry in symbols:
            logger.info(
                f"  {entry['symbol']:<16} | {entry['description']:<40} | {entry.get('region', 'unknown')}"
            )

        for entry in symbols:
            symbol = entry["symbol"]
            description = entry["description"]
            region = entry.get("region", "unknown")
            logger.section(f"Pulling {description} ({region}): {symbol}")

            df = _pull(
                symbol=symbol,
                data_type=data_type,
                granularity=granularity,
                start_date=start_date,
                end_date=end_date,
                date_col=date_col,
                date_format=date_format,
            )
            df = _format(df=df, date_col=date_col, date_format=date_format)

            if df.empty:
                logger.warning(f"No data returned for {description} ({symbol})")
                continue

            _upsert(df=df, table_name=API_SCRAPE_NAME)
            frames.append(df)
            total_rows += len(df)
            processed_symbols += 1

        run.success(
            rows_processed=total_rows,
            metadata={
                "symbols_processed": processed_symbols,
                "symbols_requested": len(symbols),
            },
        )
        return utils.combine_frames(frames, date_col=date_col)

    except Exception as exc:
        logger.exception(f"Pipeline failed: {exc}")
        run.failure(error=exc)
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()

    # from datetime import datetime, timedelta
    # start_date = datetime(2013, 1, 1) - timedelta(days=1)
    # end_date = datetime.now() + timedelta(days=1)

    # main(start_date=start_date, end_date=end_date)
