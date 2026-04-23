"""
Utility helpers for ICE contract dates (Strip / Startdt / Enddt).

Fetches the rolling contract date metadata for short-term products
via ``ice.get_quotes()`` and upserts to ``ice_python.contract_dates``.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone

import pandas as pd
import pytz

from backend.scrapes.ice_python import utils
from backend.utils import azure_postgresql_utils as azure_postgresql

_logger = logging.getLogger(__name__)

DEFAULT_DATABASE = utils.DEFAULT_DATABASE
DEFAULT_SCHEMA = utils.DEFAULT_SCHEMA

# ICE API field names for contract date metadata.
CONTRACT_DATE_FIELDS: list[str] = ["Strip", "Startdt", "Enddt"]

# ICE field name -> PostgreSQL column name
FIELD_TO_COLUMN: dict[str, str] = {
    "Strip": "strip",
    "Startdt": "start_date",
    "Enddt": "end_date",
}

# Table definition
CONTRACT_DATES_TABLE_NAME = "contract_dates"
CONTRACT_DATES_COLUMNS: list[str] = [
    "trade_date",
    "symbol",
    "strip",
    "start_date",
    "end_date",
]
CONTRACT_DATES_DATA_TYPES: list[str] = [
    "DATE",
    "VARCHAR",
    "VARCHAR",
    "DATE",
    "DATE",
]
CONTRACT_DATES_PRIMARY_KEY: list[str] = [
    "trade_date",
    "symbol",
]

MT = pytz.timezone("America/Edmonton")

QUOTES_MAX_SYMBOLS_PER_REQUEST = 500


def empty_contract_dates_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=CONTRACT_DATES_COLUMNS)


def current_trade_date_mst() -> date:
    return datetime.now(timezone.utc).astimezone(MT).date()


def _chunk_symbols(
    symbols: list[str],
    chunk_size: int = QUOTES_MAX_SYMBOLS_PER_REQUEST,
) -> list[list[str]]:
    return [
        symbols[i : i + chunk_size]
        for i in range(0, len(symbols), chunk_size)
    ]


def get_contract_dates_snapshot(
    symbols: list[str],
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> list:
    ice = utils.get_icepython_module()

    all_results: list = []
    chunks = _chunk_symbols(symbols)

    for chunk in chunks:
        result = _get_contract_dates_with_retry(
            ice=ice,
            symbols=chunk,
            max_retries=max_retries,
            backoff_base=backoff_base,
        )
        if result:
            all_results.extend(result if not all_results else result[1:])

    return all_results


def _get_contract_dates_with_retry(
    ice,
    symbols: list[str],
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> list:
    for attempt in range(1, max_retries + 1):
        try:
            data = ice.get_quotes(symbols, CONTRACT_DATE_FIELDS)
            if data:
                return data
            _logger.warning(
                f"get_quotes (contract dates) returned empty "
                f"(attempt {attempt}/{max_retries})"
            )
        except Exception as exc:
            _logger.warning(
                f"get_quotes (contract dates) attempt "
                f"{attempt}/{max_retries} failed: {exc}"
            )
        if attempt < max_retries:
            wait = backoff_base ** attempt
            _logger.info(f"Retrying in {wait:.1f}s...")
            time.sleep(wait)

    _logger.error(f"All {max_retries} get_quotes (contract dates) attempts failed")
    return []


def format_contract_dates(
    raw_data: list,
    trade_date: date | None = None,
) -> pd.DataFrame:
    if not raw_data or len(raw_data) <= 1:
        return empty_contract_dates_frame()

    trade_date = trade_date or current_trade_date_mst()

    header = raw_data[0]
    rows = raw_data[1:]

    df = pd.DataFrame(rows, columns=header)

    first_col = df.columns[0]
    df = df.rename(columns={first_col: "symbol"})
    df = df.rename(columns=FIELD_TO_COLUMN)

    df["trade_date"] = trade_date
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date

    df = df.dropna(subset=["symbol", "start_date", "end_date"])

    return (
        df[CONTRACT_DATES_COLUMNS]
        .sort_values(CONTRACT_DATES_PRIMARY_KEY)
        .reset_index(drop=True)
    )


def ensure_contract_dates_table(
    table_name: str = CONTRACT_DATES_TABLE_NAME,
    database: str = DEFAULT_DATABASE,
    schema: str = DEFAULT_SCHEMA,
) -> None:
    connection = azure_postgresql._connect_to_azure_postgressql(database=database)
    cursor = connection.cursor()

    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
            trade_date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            strip VARCHAR,
            start_date DATE,
            end_date DATE,
            created_at TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/Edmonton'),
            updated_at TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/Edmonton'),
            PRIMARY KEY (trade_date, symbol)
        );
    """
    validate_columns_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    validate_constraint_query = f"""
        SELECT tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.table_schema = '{schema}'
          AND tc.table_name = '{table_name}'
          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        GROUP BY tc.constraint_name
        HAVING string_agg(
            kcu.column_name,
            ', ' ORDER BY kcu.ordinal_position
        ) = 'trade_date, symbol';
    """
    add_constraint_query = f"""
        ALTER TABLE {schema}.{table_name}
        ADD CONSTRAINT {table_name}_upsert_key
        UNIQUE (trade_date, symbol);
    """

    try:
        cursor.execute(create_schema_query)
        cursor.execute(create_table_query)
        connection.commit()

        cursor.execute(validate_columns_query)
        existing_columns = [row[0] for row in cursor.fetchall()]
        expected = CONTRACT_DATES_COLUMNS + ["created_at", "updated_at"]
        if existing_columns != expected:
            raise ValueError(
                f"Expected {schema}.{table_name} columns "
                f"{expected}, found {existing_columns}"
            )

        cursor.execute(validate_constraint_query)
        if not cursor.fetchall():
            cursor.execute(add_constraint_query)
            connection.commit()
    finally:
        cursor.close()
        connection.close()


def upsert_contract_dates(
    df: pd.DataFrame,
    table_name: str = CONTRACT_DATES_TABLE_NAME,
    database: str = DEFAULT_DATABASE,
    schema: str = DEFAULT_SCHEMA,
    primary_key: list[str] | None = None,
) -> None:
    if df.empty:
        return

    ensure_contract_dates_table(
        table_name=table_name,
        database=database,
        schema=schema,
    )

    primary_key = primary_key or CONTRACT_DATES_PRIMARY_KEY
    upsert_df = (
        df[CONTRACT_DATES_COLUMNS]
        .drop_duplicates(subset=primary_key, keep="last")
        .reset_index(drop=True)
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=upsert_df,
        columns=CONTRACT_DATES_COLUMNS,
        data_types=CONTRACT_DATES_DATA_TYPES,
        primary_key=primary_key,
    )
