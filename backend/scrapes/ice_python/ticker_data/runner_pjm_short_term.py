"""
ICE ticker data (tick-level trades) for PJM short-term power products.

Pulls every trade execution from the start of today (MST) through now via
``ice.get_timesales()`` for each symbol in the PJM registry, and upserts to
``ice_python.ticker_data`` in long format (one row per exec × symbol × field).

Default fields: Price, Quantity. To discover the full list for a symbol, run
``ice.get_timesales_fields('PDA D1-IUS')`` and pass via the ``fields`` arg.

Idempotent: re-running the same window overwrites via
``(exec_time_local, symbol, field)`` primary key.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from backend.utils import logging_utils, pipeline_run_logger

from backend.scrapes.ice_python import utils
from backend.scrapes.ice_python.symbols.pjm_short_term_symbols import (
    get_pjm_symbol_codes,
    resolve_pjm_symbol_entries,
)
from backend.scrapes.ice_python.ticker_data import ice_ticker_data_utils

API_SCRAPE_NAME = "runner_pjm_short_term_ticker_data"
TARGET_TABLE_NAME = ice_ticker_data_utils.TICKER_DATA_TABLE_NAME

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


def _select_symbol_entries(symbols: list[str] | None = None) -> list[dict]:
    symbol_entries = resolve_pjm_symbol_entries(symbols=symbols)
    if symbol_entries:
        logger.info(
            f"Selected {len(symbol_entries)} PJM symbols from "
            "backend/scrapes/ice_python/symbols/pjm_short_term_symbols.py"
        )
    return symbol_entries


def _pull(
    symbols: list[str],
    fields: list[str] | None,
    start_date: datetime | None,
    end_date: datetime | None,
) -> list:
    if not symbols:
        logger.warning("No PJM symbols configured — nothing to pull")
        return []

    fields = fields or ice_ticker_data_utils.DEFAULT_TIMESALES_FIELDS
    logger.info(
        f"Requesting time-and-sales for {len(symbols)} symbols × {len(fields)} fields"
    )
    return ice_ticker_data_utils.get_timesales_batch(
        symbols=symbols,
        fields=fields,
        start_date=start_date,
        end_date=end_date,
    )


def _format(
    raw_data: list,
    symbols: list[str],
    fields: list[str] | None,
) -> pd.DataFrame:
    df = ice_ticker_data_utils.format_ticker_data(
        raw_data=raw_data,
        symbols=symbols,
        fields=fields,
    )
    if df.empty:
        logger.warning("Formatted DataFrame is empty")
    else:
        logger.info(
            f"Formatted {len(df)} tick-level rows "
            f"from {df['exec_time_local'].min()} to {df['exec_time_local'].max()}"
        )
    return df


def _upsert(
    df: pd.DataFrame,
    database: str = utils.DEFAULT_DATABASE,
    schema: str = utils.DEFAULT_SCHEMA,
    table_name: str = TARGET_TABLE_NAME,
) -> None:
    ice_ticker_data_utils.upsert_ticker_data(
        df=df,
        database=database,
        schema=schema,
        table_name=table_name,
    )


def main(
    symbols: list[str] | None = None,
    fields: list[str] | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
) -> pd.DataFrame:
    """Capture intraday tick-level trades for PJM symbols.

    Parameters
    ----------
    symbols : list[str] | None
        Optional ICE symbol codes. Defaults to the full PJM registry.
    fields : list[str] | None
        Optional ICE fields (e.g. ['Price', 'Quantity']). Defaults to
        ice_ticker_data_utils.DEFAULT_TIMESALES_FIELDS.
    start_date, end_date : datetime | None
        Optional window. Defaults to today (MST) 00:00 through now (UTC).
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="ice_python",
        target_table=f"{utils.DEFAULT_SCHEMA}.{TARGET_TABLE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)
        symbol_entries = _select_symbol_entries(symbols=symbols)
        selected_symbols = get_pjm_symbol_codes(symbol_entries)

        raw_data = _pull(
            symbols=selected_symbols,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
        )
        if not raw_data or len(raw_data) <= 1:
            logger.warning("No ticker data returned from ICE")
            run.success(
                rows_processed=0,
                metadata={
                    "symbols_requested": len(selected_symbols),
                    "symbols_returned": 0,
                    "symbols_selected": selected_symbols,
                },
            )
            return ice_ticker_data_utils.empty_ticker_data_frame()

        df = _format(
            raw_data=raw_data,
            symbols=selected_symbols,
            fields=fields,
        )

        if df.empty:
            run.success(
                rows_processed=0,
                metadata={
                    "symbols_requested": len(selected_symbols),
                    "symbols_returned": 0,
                },
            )
            return df

        returned_symbols = set(df["symbol"].unique())
        requested_symbols = set(selected_symbols)
        missing_symbols = requested_symbols - returned_symbols
        if missing_symbols:
            logger.warning(
                f"No ticks for {len(missing_symbols)}/{len(requested_symbols)} "
                f"symbols: {sorted(missing_symbols)}"
            )

        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df=df)
        logger.success("Ticker data upserted successfully")

        run.success(
            rows_processed=len(df),
            metadata={
                "symbols_requested": len(requested_symbols),
                "symbols_returned": len(returned_symbols),
                "symbols_missing": sorted(missing_symbols),
                "exec_time_min": str(df["exec_time_local"].min()),
                "exec_time_max": str(df["exec_time_local"].max()),
            },
        )
        return df

    except Exception as exc:
        logger.exception(f"Pipeline failed: {exc}")
        run.failure(error=exc)
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()
