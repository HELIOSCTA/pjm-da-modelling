"""
EIA Annual Coal Price by Region (price-by-rank).

Pulls annual average coal prices by state-region from EIA Form 7A /
MSHA Form 7000-2 (the only coal-price dataset exposed by the v2 API).
Weekly Coal Commodity Spot Prices are NOT in the free API -- those
are proprietary S&P Global data published only in the Weekly Coal
Markets PDF; see ``coal_markets_weekly.py`` for that scrape.

Upserts to ``eia.coal_price_annual`` keyed on
``(state_region_id, coal_rank_id, year)``.

PJM-relevant basin codes (from ``stateRegionId`` facet)
-------------------------------------------------------
- APC  Appalachia Central       (~ Central App / CAPP)
- APN  Appalachia Northern      (~ Northern App / NAPP)
- APS  Appalachia Southern
- IL   Illinois                 (Illinois Basin -- bituminous)
- INO  Other Interior           (Illinois Basin, Indiana side)
- KYE  Kentucky East
- KYW  Kentucky West            (Illinois Basin, KY side)
- PRB  Powder River Basin
- UNT  Uinta Basin
- US   U.S. Total                (fallback / sanity-check)

Coal rank: ``TOT`` (weighted average across ranks) -- the right value
when a basin produces a mix of bituminous and subbituminous.

Orchestration: scheduled (annual, once a year in the spring when EIA
publishes new annual coal data, typically Q1).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from backend.scrapes.eia._client import fetch_route_data
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# Config

API_SCRAPE_NAME = "coal_price_annual"

# PJM-relevant state/region codes from the EIA stateRegionId facet
PJM_REGION_CODES: list[str] = [
    "APC",
    "APN",
    "APS",
    "IL",
    "INO",
    "KYE",
    "KYW",
    "PRB",
    "UNT",
    "US",
]

# Coal rank: pull all (TOT) plus the rank-specific series for PRB (SUB)
# and Appalachia (BIT) so dbt joins can pick the right one.
COAL_RANK_CODES: list[str] = ["TOT", "BIT", "SUB"]

# Btu/lb assumptions for $/MMBtu conversion. EIA publishes annual
# average heat content per region in the Coal Annual report; these
# match the Weekly Coal Commodity Spot Price table assumptions and
# are stable enough to hard-code.
BTU_PER_LB_BY_REGION: dict[str, int] = {
    "APC": 12500,
    "APN": 13000,
    "APS": 12500,
    "IL": 11800,
    "INO": 11800,
    "KYE": 12500,
    "KYW": 11800,
    "PRB": 8800,
    "UNT": 11700,
    "US": 11000,  # blended national avg
}

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


# Core pipeline functions


def _pull() -> pd.DataFrame:
    """Pull annual coal price-by-rank for the configured regions."""
    logger.section(
        f"Fetching annual coal price-by-rank for {len(PJM_REGION_CODES)} regions "
        f"x {len(COAL_RANK_CODES)} ranks..."
    )
    return fetch_route_data(
        route="coal/price-by-rank",
        frequency="annual",
        facets={
            "stateRegionId": PJM_REGION_CODES,
            "coalRankId": COAL_RANK_CODES,
        },
        data_fields=("price",),
    )


def _format(raw: pd.DataFrame) -> pd.DataFrame:
    """Shape EIA response into the warehouse schema."""
    if raw.empty:
        return pd.DataFrame()

    df = raw.rename(
        columns={
            "stateRegionId": "state_region_id",
            "stateRegionDescription": "state_region_name",
            "coalRankId": "coal_rank_id",
            "coalRankDescription": "coal_rank_name",
            "price-units": "price_units",
        }
    )

    df["year"] = df["period"].dt.year
    df["price_usd_per_short_ton"] = pd.to_numeric(df["price"], errors="coerce")
    df["btu_per_lb"] = df["state_region_id"].map(BTU_PER_LB_BY_REGION)
    df["mmbtu_per_short_ton"] = df["btu_per_lb"] * 2000 / 1_000_000
    df["price_usd_per_mmbtu"] = (
        df["price_usd_per_short_ton"] / df["mmbtu_per_short_ton"]
    )

    df["scrape_timestamp"] = pd.Timestamp.utcnow().tz_localize(None)
    df["source_url"] = "https://api.eia.gov/v2/coal/price-by-rank/data/"

    out_cols = [
        "state_region_id",
        "state_region_name",
        "coal_rank_id",
        "coal_rank_name",
        "year",
        "price_usd_per_short_ton",
        "price_units",
        "btu_per_lb",
        "price_usd_per_mmbtu",
        "source_url",
        "scrape_timestamp",
    ]
    df = df[out_cols].dropna(
        subset=["state_region_id", "coal_rank_id", "year", "price_usd_per_short_ton"]
    )
    df = df.sort_values(["state_region_id", "coal_rank_id", "year"])
    df = df.drop_duplicates(
        subset=["state_region_id", "coal_rank_id", "year"], keep="last"
    )
    return df.reset_index(drop=True)


def _upsert(
    df: pd.DataFrame,
    schema: str = "eia",
    table_name: str = API_SCRAPE_NAME,
    primary_key: list[str] | None = None,
) -> None:
    primary_key = primary_key or ["state_region_id", "coal_rank_id", "year"]
    data_types = azure_postgresql.infer_sql_data_types(df=df)
    azure_postgresql.upsert_to_azure_postgresql(
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=primary_key,
    )


# Entrypoint


def main() -> pd.DataFrame:
    """Orchestrate: pull -> format -> upsert."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="eia",
        target_table=f"eia.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(API_SCRAPE_NAME)

        raw = _pull()

        logger.section("Formatting...")
        df = _format(raw)

        if df.empty:
            logger.section("No coal price rows returned, skipping upsert.")
            run.success(rows_processed=0)
            return df

        logger.section(
            f"Upserting {len(df)} rows ({df['state_region_id'].nunique()} regions, "
            f"{df['year'].min()} -> {df['year'].max()})..."
        )
        _upsert(df)

        run.success(rows_processed=len(df))

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()

    return df


if __name__ == "__main__":
    df = main()
