"""
ICE daily settle pull for RGGI carbon allowance futures.

Pulls the daily Settle for each symbol in the RGGI registry and
upserts long-format to ``ice_python.rggi_futures_v1`` keyed on
``(trade_date, symbol, data_type)``.

Settle only -- that's the canonical end-of-day mark for futures and
the auction-clearing-price index. Volume / Open Interest can be
added later by extending ``DATA_TYPES`` without a schema change
(the table is long-format on ``data_type``).

Entitlement note: as of 2026-05-13 this subscription does not appear
to be entitled to RGGI products on either IUSE or NYMG. The script
runs cleanly and upserts zero rows in that state; turn on the
entitlement and the same run starts producing data without code
changes.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.utils import logging_utils, pipeline_run_logger

from backend.scrapes.ice_python import utils
from backend.scrapes.ice_python.symbols.rggi_futures_symbols import (
    get_rggi_futures_symbols,
)

API_SCRAPE_NAME = "rggi_futures_v1"

DATA_TYPES: list[str] = ["Settle"]

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


_REQUIRED_KEYS = {"symbol", "description"}


def _load_symbols() -> list[dict]:
    symbols = get_rggi_futures_symbols()
    if not symbols:
        raise ValueError(
            "No RGGI futures symbols returned from symbol registry. "
            "Check backend/scrapes/ice_python/symbols/rggi_futures_symbols.py"
        )
    for idx, entry in enumerate(symbols):
        missing = _REQUIRED_KEYS - set(entry.keys())
        if missing:
            raise ValueError(
                f"Symbol entry [{idx}] missing required keys: {missing}. "
                f"Entry: {entry}"
            )
        if not entry["symbol"].strip():
            raise ValueError(
                f"Symbol entry [{idx}] has an empty 'symbol' value. "
                f"Description: {entry.get('description', 'N/A')}"
            )
    return symbols


def _pull(
    symbol: str,
    data_type: str,
    granularity: str = "D",
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    return utils.get_timeseries_with_retry(
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
    data_types: list[str] | None = None,
    granularity: str = "D",
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    date_col: str = utils.DEFAULT_DATE_COLUMN,
    date_format: str = utils.DEFAULT_DATE_FORMAT,
) -> pd.DataFrame:
    start_date = start_date or utils.default_start_date()
    end_date = end_date or utils.default_end_date()
    data_types = data_types or DATA_TYPES

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
    pairs_with_data = 0
    pairs_empty = 0
    try:
        logger.header(API_SCRAPE_NAME)
        logger.info(
            f"Loaded {len(symbols)} symbols x {len(data_types)} data_types "
            f"({len(symbols) * len(data_types)} pulls), window "
            f"{start_date:%Y-%m-%d} -> {end_date:%Y-%m-%d}"
        )
        for entry in symbols:
            logger.info(
                f"  {entry['symbol']:<16} | {entry['description']:<48} | "
                f"{entry.get('contract_type', '?'):<16} | "
                f"{entry.get('exchange', '?')}"
            )

        for entry in symbols:
            symbol = entry["symbol"]
            description = entry["description"]

            for data_type in data_types:
                logger.section(f"Pulling {description} | {symbol} | {data_type}")
                raw = _pull(
                    symbol=symbol,
                    data_type=data_type,
                    granularity=granularity,
                    start_date=start_date,
                    end_date=end_date,
                    date_col=date_col,
                    date_format=date_format,
                )
                df = _format(df=raw, date_col=date_col, date_format=date_format)

                if df.empty:
                    pairs_empty += 1
                    logger.warning(
                        f"No data for {symbol} / {data_type} "
                        "(symbol may be unentitled or no trades in window)"
                    )
                    continue

                _upsert(df=df, table_name=API_SCRAPE_NAME)
                frames.append(df)
                total_rows += len(df)
                pairs_with_data += 1

        run.success(
            rows_processed=total_rows,
            metadata={
                "symbols": len(symbols),
                "data_types": data_types,
                "pairs_with_data": pairs_with_data,
                "pairs_empty": pairs_empty,
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
    # start_date = datetime(2014, 1, 1)
    # end_date = datetime.now() + timedelta(days=1)
    # main(start_date=start_date, end_date=end_date)
