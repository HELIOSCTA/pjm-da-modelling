import requests
from io import StringIO
from pathlib import Path

import pandas as pd

from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# SCRAPE
API_SCRAPE_NAME = "agg_definitions"

# logging
logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)

"""
PJM Aggregate Definitions (agg_definitions)
https://dataminer2.pjm.com/feed/agg_definitions/definition

    Posting Frequency:    Static (changes only when PJM publishes a new
                          aggregate definition or terminates an existing
                          one — typically a few updates per year).
    First Available:      Historical back to PJM's pnode model inception.
    Retention Time:       Indefinite.

What it is:
    Bus distribution factors used in settlements for aggregates with
    static definitions. Maps each aggregate pnode (HUB, ZONE, etc.) to
    its constituent bus pnodes with a proportional weight (factor).

Why we want it:
    The PSS/E .raw network model describes physical topology (buses,
    branches) but not market geography. This feed is the bridge:
    given an aggregate name like "WESTERN HUB", returns the list of
    bus pnodes that compose it. Lets brief subagents answer "is this
    outage's bus inside Western Hub?" via deterministic lookup rather
    than per-call LLM reasoning.

Schema:
    effective_date_ept     datetime  membership start (EPT)
    terminate_date_ept     datetime  membership end (null = still active)
    agg_pnode_id           int       aggregate pnode ID (the hub/zone)
    agg_pnode_name         str       e.g. "WESTERN HUB", "AEP-DAYTON HUB"
    bus_pnode_id           int       constituent bus pnode ID
    bus_pnode_name         str       bus name
    bus_pnode_factor       float     proportional contribution weight
                                     (some hubs are unequal-weighted)

Filter convention:
    PJM does NOT support a server-side `active` filter for this feed
    (probed 2026-05-07 — returns 400). Active filtering is client-side:
    `terminate_date_ept` is null or in the future. The optional
    `agg_pnode_name` server-side filter is supported and dramatically
    reduces row count for single-hub pulls (e.g. WESTERN HUB = 1,822
    historical rows; 89 currently active).

    `main()` pulls full feed and filters active rows client-side.
    `_backfill()` pulls full feed and keeps everything for historical
    point-in-time queries.

Throughput:
    Full feed = ~1M rows total (most are historical/terminated);
    currently active subset is much smaller. The 50k row cap means
    we paginate via `startRow` increments. Single-hub pulls fit in
    one request.
"""


def _pull(
    active_only: bool = True,
    agg_pnode_name: str | None = None,
    page_size: int = 50000,
) -> pd.DataFrame:
    """Pull aggregate-pnode-to-bus-pnode mappings, paginated.

    active_only=True (default) filters client-side to rows where
    `terminate_date_ept` is null or in the future.

    agg_pnode_name (optional) restricts the pull to a single aggregate
    (e.g. "WESTERN HUB") — server-side filter, single request.

    page_size is the per-request row cap (PJM enforces 50k max).
    """

    base_url = "https://api.pjm.com/api/v1/agg_definitions"

    base_params = {
        "rowCount": page_size,
        "format": "csv",
        "subscription-key": "0e3e44aa6bde4d5da1699fda4511235e",
    }
    if agg_pnode_name:
        base_params["agg_pnode_name"] = agg_pnode_name

    chunks: list[pd.DataFrame] = []
    start_row = 1
    while True:
        params = {**base_params, "startRow": start_row}
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        if not response.text.strip():
            break

        chunk = pd.read_csv(StringIO(response.text))
        if chunk.empty:
            break

        chunks.append(chunk)
        if len(chunk) < page_size:
            break
        start_row += page_size

    if not chunks:
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)

    # Remove non-ascii / BOM characters from column names
    df.columns = df.columns.str.encode("ascii", errors="ignore").str.decode("ascii")

    if df.empty:
        return df

    # data types
    # datetime — explicit format avoids per-row dateutil fallback
    for col in ["effective_date_ept", "terminate_date_ept"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    # int (nullable)
    for col in ["agg_pnode_id", "bus_pnode_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    # float
    if "bus_pnode_factor" in df.columns:
        df["bus_pnode_factor"] = pd.to_numeric(
            df["bus_pnode_factor"], errors="coerce"
        ).astype(float)
    # str (strip whitespace — agg names are sometimes padded)
    for col in ["agg_pnode_name", "bus_pnode_name"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    if active_only:
        today = pd.Timestamp.now().normalize()
        df = df[
            df["terminate_date_ept"].isna() | (df["terminate_date_ept"] > today)
        ].reset_index(drop=True)

    return df


def _upsert(
    df: pd.DataFrame,
    database: str = "helioscta",
    schema: str = "pjm",
    table_name: str = API_SCRAPE_NAME,
):
    # Active rows have terminate_date_ept = NaT ("still active"). The
    # upsert utility runs df.fillna(0) before COPY, which would write
    # integer 0 into a TIMESTAMP column and fail. Convert NaT to a
    # high-date sentinel (standard SCD2 pattern) so terminate_date_ept
    # is always a valid timestamp downstream — readers detect "still
    # active" by `terminate_date_ept >= '9999-01-01'`.
    df = df.copy()
    if "terminate_date_ept" in df.columns:
        df["terminate_date_ept"] = df["terminate_date_ept"].fillna(
            pd.Timestamp("9999-12-31")
        )

    # Explicit dtypes — the utility's infer_sql_data_types defaults
    # `int` to INTEGER (32-bit), but PJM bus_pnode_id values exceed
    # 2,147,483,647 (e.g. 2156114228). Force BIGINT for both id cols.
    explicit_dtypes = {
        "effective_date_ept": "TIMESTAMP",
        "terminate_date_ept": "TIMESTAMP",
        "agg_pnode_id": "BIGINT",
        "agg_pnode_name": "VARCHAR",
        "bus_pnode_id": "BIGINT",
        "bus_pnode_name": "VARCHAR",
        "bus_pnode_factor": "DOUBLE PRECISION",
    }
    data_types = [explicit_dtypes[col] for col in df.columns]

    azure_postgresql.upsert_to_azure_postgresql(
        database=database,
        schema=schema,
        table_name=table_name,
        df=df,
        columns=df.columns.tolist(),
        data_types=data_types,
        primary_key=[
            "agg_pnode_id",
            "bus_pnode_id",
            "effective_date_ept",
        ],
    )


def main(active_only: bool = True):
    """Pull and upsert aggregate-pnode definitions.

    Default active_only=True is the routine call — it captures the
    current hub/zone composition. Run on a slow cadence (weekly or
    monthly is plenty; the feed rarely changes).
    """

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=f"pjm.{API_SCRAPE_NAME}",
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    try:
        logger.header(f"{API_SCRAPE_NAME} (active_only={active_only})")

        logger.section("Pulling aggregate definitions...")
        df = _pull(active_only=active_only)

        if df.empty:
            logger.section("No data returned, skipping upsert.")
            run.success(rows_processed=0)
            return df

        logger.info(
            f"Pulled {len(df)} rows | "
            f"aggregates={df['agg_pnode_name'].nunique()} | "
            f"buses={df['bus_pnode_id'].nunique()} | "
            f"sample agg names: {sorted(df['agg_pnode_name'].unique())[:5]}"
        )

        logger.section(f"Upserting {len(df)} rows...")
        _upsert(df)
        logger.success(f"Successfully pulled and upserted {len(df)} rows.")

        run.success(rows_processed=len(df))

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        run.failure(error=e)

        # raise exception
        raise

    finally:
        logging_utils.close_logging()

    if "df" in locals() and df is not None:
        return df


def _backfill():
    """One-shot historical pull including terminated memberships.

    Run manually once to seed the table; thereafter `main()` keeps the
    active set current. Historical rows let backtests resolve hub
    composition as of any past date.
    """
    main(active_only=False)


if __name__ == "__main__":
    main()
