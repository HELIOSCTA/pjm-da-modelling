"""
Utility helpers for ICE ticker data (tick-level trade executions).

Wraps ``ice.get_timesales()`` per the ICE XL Python Guide (section 3.3):
    - Max 10 symbols per request
    - Max 1 request per second

Data is returned as a tuple with exec_time in the leftmost column and
symbols/fields as headers. One row per trade execution (Ticker feed).

Timezone note: ICE XL interprets start_date/end_date as publisher-local
(the ICE XL host's timezone — Mountain here) AND returns timestamps in
the same publisher-local zone. We store the raw local timestamp in
``exec_time_local`` — no TZ conversion in the format step.

Output schema (long format, one row per (exec_time, symbol, field)):
    exec_time_local  TIMESTAMP  - trade execution timestamp (publisher-local, i.e. MT)
    trade_date       DATE       - local calendar date at exec_time
    symbol           VARCHAR    - ICE symbol code
    field            VARCHAR    - 'Price', 'Size', 'Type', 'Conditions', 'Value', 'Bid', 'Ask', …
    value            TEXT       - raw field value (numeric fields cast in dbt)

PK: (exec_time_local, symbol, field)
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime

import pandas as pd
import pytz

from backend.scrapes.ice_python import utils
from backend.utils import azure_postgresql_utils as azure_postgresql

_logger = logging.getLogger(__name__)

DEFAULT_DATABASE = utils.DEFAULT_DATABASE
DEFAULT_SCHEMA = utils.DEFAULT_SCHEMA

# ICE API limits (ICE XL Python Guide, section 6.1)
TIMESALES_MAX_SYMBOLS_PER_REQUEST = 10
TIMESALES_MIN_INTERVAL_SECONDS = 1.0

# Fields pulled per trade. Confirmed valid via ice.get_timesales_fields('PDA D1-IUS').
#   Price      - trade execution price
#   Size       - trade quantity (lots / MWh depending on contract)
#   Type       - trade type indicator (always 'TRADE' for the trade feed)
#   Conditions - condition flags ('SetByBid', 'SetByAsk', 'Leg', etc.)
#   Bid / Ask  - book bid/ask at the moment of the trade
DEFAULT_TIMESALES_FIELDS: list[str] = [
    "Price",
    "Size",
    "Type",
    "Conditions",
    "Bid",
    "Ask",
]

# Numeric-typed fields — dbt source pivot casts these to DOUBLE PRECISION.
# Everything else (Type, Conditions, etc.) is kept as text.
NUMERIC_TIMESALES_FIELDS: set[str] = {"Price", "Size", "Bid", "Ask"}

TICKER_DATA_TABLE_NAME = "ticker_data"
TICKER_DATA_COLUMNS: list[str] = [
    "exec_time_local",
    "trade_date",
    "symbol",
    "field",
    "value",
]
TICKER_DATA_DATA_TYPES: list[str] = [
    "TIMESTAMP",
    "DATE",
    "VARCHAR",
    "VARCHAR",
    "TEXT",
]
TICKER_DATA_PRIMARY_KEY: list[str] = [
    "exec_time_local",
    "symbol",
    "field",
]

MT = pytz.timezone("America/Edmonton")


def empty_ticker_data_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=TICKER_DATA_COLUMNS)


def chunk_symbols(
    symbols: list[str],
    chunk_size: int = TIMESALES_MAX_SYMBOLS_PER_REQUEST,
) -> list[list[str]]:
    return [
        symbols[i : i + chunk_size]
        for i in range(0, len(symbols), chunk_size)
    ]


def current_trade_date_local() -> date:
    """Today's date in publisher-local (Mountain) time."""
    return datetime.now(MT).date()


def default_start_datetime() -> datetime:
    """Start of current trade date in publisher-local (MT) time, as naive datetime.

    ICE XL interprets start_date/end_date as publisher-local — so we pass
    naive MT datetimes directly.
    """
    return datetime.combine(current_trade_date_local(), datetime.min.time())


def default_end_datetime() -> datetime:
    """Current moment in publisher-local (MT) time, as naive datetime."""
    return datetime.now(MT).replace(tzinfo=None)


def _ensure_publisher_awake(ice) -> None:
    try:
        if ice.get_hibernation():
            ice.set_hibernation(False)
            _logger.info("ICE XL publisher hibernation disabled")
    except Exception as exc:
        _logger.warning(f"Could not check/set publisher hibernation: {exc}")


def get_timesales_batch(
    symbols: list[str],
    fields: list[str] | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> list:
    """Fetch time-and-sales data respecting the 10-symbol / 1-req-per-sec limit."""
    ice = utils.get_icepython_module()
    _ensure_publisher_awake(ice)

    fields = fields or DEFAULT_TIMESALES_FIELDS
    start_date = start_date or default_start_datetime()
    end_date = end_date or default_end_datetime()

    all_results: list = []
    chunks = chunk_symbols(symbols, TIMESALES_MAX_SYMBOLS_PER_REQUEST)

    for i, chunk in enumerate(chunks):
        if i > 0:
            time.sleep(TIMESALES_MIN_INTERVAL_SECONDS)

        result = _get_timesales_with_retry(
            ice=ice,
            symbols=chunk,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            max_retries=max_retries,
            backoff_base=backoff_base,
        )
        if result:
            all_results.extend(result if not all_results else result[1:])

    return all_results


def _get_timesales_with_retry(
    ice,
    symbols: list[str],
    fields: list[str],
    start_date: datetime,
    end_date: datetime,
    max_retries: int = 3,
    backoff_base: float = 2.0,
) -> list:
    date_fmt = "%Y-%m-%d %H:%M:%S"
    for attempt in range(1, max_retries + 1):
        try:
            data = ice.get_timesales(
                symbols,
                fields,
                start_date=start_date.strftime(date_fmt),
                end_date=end_date.strftime(date_fmt),
            )
            if data:
                return data
            _logger.warning(
                f"get_timesales returned empty (attempt {attempt}/{max_retries})"
            )
        except Exception as exc:
            _logger.warning(
                f"get_timesales attempt {attempt}/{max_retries} failed: {exc}"
            )
        if attempt < max_retries:
            wait = backoff_base ** attempt
            _logger.info(f"Retrying in {wait:.1f}s...")
            time.sleep(wait)

    _logger.error(f"All {max_retries} get_timesales attempts failed")
    return []


def format_ticker_data(
    raw_data: list,
    symbols: list[str],
    fields: list[str] | None = None,
) -> pd.DataFrame:
    """Parse get_timesales response into long-format tick-level rows.

    Per ICE XL Guide, the response is a tuple with exec time in column 0
    and column headers of the form "SYMBOL.FIELD" (e.g. "PDA D1-IUS.Price").

    All values are stored as text in the raw table; the dbt source view
    casts numeric fields to DOUBLE PRECISION at pivot time. This lets
    string-valued fields (Type, Conditions) coexist with numeric ones.
    """
    if not raw_data or len(raw_data) <= 1:
        return empty_ticker_data_frame()

    fields = fields or DEFAULT_TIMESALES_FIELDS

    header = raw_data[0]
    rows = raw_data[1:]
    wide = pd.DataFrame(rows, columns=header)

    exec_col = wide.columns[0]
    wide[exec_col] = pd.to_datetime(wide[exec_col], errors="coerce")
    wide = wide.dropna(subset=[exec_col])
    wide = wide.rename(columns={exec_col: "exec_time_local"})

    value_columns = [c for c in wide.columns if c != "exec_time_local"]
    if not value_columns:
        return empty_ticker_data_frame()

    long_df = wide.melt(
        id_vars=["exec_time_local"],
        value_vars=value_columns,
        var_name="symbol_field",
        value_name="value",
    )

    # Column headers are "<SYMBOL>.<FIELD>" per ICE docs.
    split = long_df["symbol_field"].str.rsplit(".", n=1, expand=True)
    long_df["symbol"] = split[0]
    long_df["field"] = split[1]
    long_df = long_df.drop(columns=["symbol_field"])

    # Drop NaN / empty values (blank cells in the raw tuple). Keep everything
    # else as text — numeric and string fields both land in the same column.
    long_df = long_df.dropna(subset=["symbol", "field"])
    long_df = long_df[long_df["value"].notna()]
    long_df["value"] = long_df["value"].astype(str).str.strip()
    long_df = long_df[long_df["value"] != ""]
    if long_df.empty:
        return empty_ticker_data_frame()

    long_df["trade_date"] = long_df["exec_time_local"].dt.tz_localize(None).dt.date

    return (
        long_df[TICKER_DATA_COLUMNS]
        .sort_values(TICKER_DATA_PRIMARY_KEY)
        .reset_index(drop=True)
    )


def ensure_ticker_data_table(
    table_name: str = TICKER_DATA_TABLE_NAME,
    database: str = DEFAULT_DATABASE,
    schema: str = DEFAULT_SCHEMA,
) -> None:
    connection = azure_postgresql._connect_to_azure_postgressql(database=database)
    cursor = connection.cursor()

    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name}(
            exec_time_local TIMESTAMP NOT NULL,
            trade_date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            field VARCHAR NOT NULL,
            value TEXT,
            created_at TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/Edmonton'),
            updated_at TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/Edmonton'),
            PRIMARY KEY (exec_time_local, symbol, field)
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
        ) = 'exec_time_local, symbol, field';
    """
    add_constraint_query = f"""
        ALTER TABLE {schema}.{table_name}
        ADD CONSTRAINT {table_name}_upsert_key
        UNIQUE (exec_time_local, symbol, field);
    """

    try:
        cursor.execute(create_schema_query)
        cursor.execute(create_table_query)
        connection.commit()

        cursor.execute(validate_columns_query)
        existing_columns = [row[0] for row in cursor.fetchall()]
        expected = TICKER_DATA_COLUMNS + ["created_at", "updated_at"]
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


def upsert_ticker_data(
    df: pd.DataFrame,
    table_name: str = TICKER_DATA_TABLE_NAME,
    database: str = DEFAULT_DATABASE,
    schema: str = DEFAULT_SCHEMA,
    primary_key: list[str] | None = None,
) -> None:
    if df.empty:
        return

    ensure_ticker_data_table(
        table_name=table_name,
        database=database,
        schema=schema,
    )

    primary_key = primary_key or TICKER_DATA_PRIMARY_KEY
    upsert_df = (
        df[TICKER_DATA_COLUMNS]
        .drop_duplicates(subset=primary_key, keep="last")
        .reset_index(drop=True)
    )

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=upsert_df,
        columns=TICKER_DATA_COLUMNS,
        data_types=TICKER_DATA_DATA_TYPES,
        primary_key=primary_key,
    )
