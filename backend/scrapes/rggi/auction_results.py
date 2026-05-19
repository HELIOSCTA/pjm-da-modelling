"""
RGGI CO2 Allowance Auction Results.

Scrapes the public auction-results table at
``https://www.rggi.org/auctions/auction-results/prices-volumes`` --
a single HTML ``<table>`` listing every RGGI auction since #1 (Sep 2008)
with clearing price ($/short ton CO2) and quantities.

Upserts to ``rggi.auction_results`` keyed on ``auction_number``.

Refresh cadence: auctions are quarterly (March / June / Sept / Dec),
so a weekly or monthly scheduled run is plenty. Re-running between
auctions is a no-op (idempotent upsert on the same N rows).

Why not EIA: EIA does not publish RGGI allowance prices in the v2
Open Data API. The auction results page is the canonical primary
source; everything else (S&P, ClearBlue, BNEF) re-publishes it.
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd
import requests

from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# Config

API_SCRAPE_NAME = "auction_results"
SOURCE_URL = "https://www.rggi.org/auctions/auction-results/prices-volumes"
REQUEST_TIMEOUT_SECONDS = 30
USER_AGENT = "Mozilla/5.0 (compatible; helioscta-rggi-scraper)"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


# Core pipeline functions


def _pull() -> pd.DataFrame:
    """Fetch the auction-results page and parse the single results table."""
    logger.section(f"Fetching {SOURCE_URL}")
    resp = requests.get(
        SOURCE_URL,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()

    # The page contains exactly one tabular view -- pandas.read_html
    # would happily grab any other <table> the CMS adds later, so we
    # filter on the column header signature instead of taking [0].
    tables = pd.read_html(StringIO(resp.text), flavor="lxml")
    expected = {"Auction", "Date", "Quantity Offered", "Clearing Price"}
    matched = [t for t in tables if expected.issubset(set(t.columns.astype(str)))]
    if not matched:
        raise RuntimeError(
            f"Auction results table not found in {SOURCE_URL} "
            f"(found {len(tables)} tables with columns "
            f"{[list(t.columns) for t in tables]})"
        )
    if len(matched) > 1:
        logger.warning(
            f"Multiple tables match the auction-results signature "
            f"({len(matched)}); using the first."
        )
    return matched[0]


def _parse_money(series: pd.Series) -> pd.Series:
    """Strip $ and commas from a money-formatted column."""
    return pd.to_numeric(
        series.astype(str).str.replace(r"[\$,]", "", regex=True),
        errors="coerce",
    )


def _parse_count(series: pd.Series) -> pd.Series:
    """Strip commas from a thousands-formatted integer column."""
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )


def _format(raw: pd.DataFrame) -> pd.DataFrame:
    """Shape the RGGI table into the warehouse schema."""
    if raw.empty:
        return pd.DataFrame()

    df = raw.rename(
        columns={
            "Auction": "auction_label",
            "Date": "auction_date",
            "Quantity Offered": "quantity_offered_tons",
            "CCR Sold": "ccr_sold_tons",
            "Quantity Sold": "quantity_sold_tons",
            "Clearing Price": "clearing_price_usd_per_ton",
            "Total Proceeds": "total_proceeds_usd",
        }
    )

    df["auction_number"] = pd.to_numeric(
        df["auction_label"].astype(str).str.extract(r"(\d+)", expand=False),
        errors="coerce",
    ).astype("Int64")

    df["auction_date"] = pd.to_datetime(df["auction_date"], errors="coerce").dt.date

    for col in (
        "quantity_offered_tons",
        "ccr_sold_tons",
        "quantity_sold_tons",
    ):
        df[col] = _parse_count(df[col]).astype("Int64")

    df["clearing_price_usd_per_ton"] = _parse_money(df["clearing_price_usd_per_ton"])
    df["total_proceeds_usd"] = _parse_money(df["total_proceeds_usd"])

    df["source_url"] = SOURCE_URL
    df["scrape_timestamp"] = pd.Timestamp.utcnow().tz_localize(None)

    out_cols = [
        "auction_number",
        "auction_date",
        "quantity_offered_tons",
        "ccr_sold_tons",
        "quantity_sold_tons",
        "clearing_price_usd_per_ton",
        "total_proceeds_usd",
        "source_url",
        "scrape_timestamp",
    ]
    df = df[out_cols].dropna(subset=["auction_number", "auction_date"])
    df = df.sort_values("auction_number").drop_duplicates(
        subset=["auction_number"], keep="last"
    )
    return df.reset_index(drop=True)


# Explicit column types -- avoids the shared infer helper, which only
# inspects df.loc[0] and chokes on pandas <NA> in early-auction CCR
# (the concept post-dates Auction #1).
COLUMN_DATA_TYPES: dict[str, str] = {
    "auction_number": "INTEGER",
    "auction_date": "DATE",
    "quantity_offered_tons": "BIGINT",
    "ccr_sold_tons": "BIGINT",
    "quantity_sold_tons": "BIGINT",
    "clearing_price_usd_per_ton": "FLOAT",
    "total_proceeds_usd": "FLOAT",
    "source_url": "VARCHAR",
    "scrape_timestamp": "TIMESTAMP",
}


def _upsert(
    df: pd.DataFrame,
    schema: str = "rggi",
    table_name: str = API_SCRAPE_NAME,
    primary_key: list[str] | None = None,
) -> None:
    primary_key = primary_key or ["auction_number"]
    columns = df.columns.tolist()
    data_types = [COLUMN_DATA_TYPES[c] for c in columns]
    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=columns,
        data_types=data_types,
        primary_key=primary_key,
    )


# Entrypoint


def main() -> pd.DataFrame:
    """Orchestrate: pull -> format -> upsert."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="rggi",
        target_table=f"rggi.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        raw = _pull()
        logger.info(f"Parsed {len(raw)} rows from auction-results table")

        logger.section("Formatting...")
        df = _format(raw)

        if df.empty:
            logger.warning("No auction rows after formatting, skipping upsert.")
            run.success(rows_processed=0)
            return df

        latest = df.iloc[-1]
        logger.section(
            f"Upserting {len(df)} auctions "
            f"(latest: #{int(latest['auction_number'])} on "
            f"{latest['auction_date']} @ "
            f"${latest['clearing_price_usd_per_ton']:.2f}/ton)..."
        )
        _upsert(df)

        run.success(
            rows_processed=len(df),
            metadata={
                "latest_auction_number": int(latest["auction_number"]),
                "latest_auction_date": str(latest["auction_date"]),
                "latest_clearing_price_usd_per_ton": float(
                    latest["clearing_price_usd_per_ton"]
                ),
            },
        )

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()

    return df


if __name__ == "__main__":
    df = main()
    if not df.empty:
        print(df.tail(5).to_string(index=False))
