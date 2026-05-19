"""
EIA-923 Monthly Fuel Receipts and Costs (via PUDL).

Pulls PUDL's ``out_eia923__monthly_fuel_receipts_costs`` parquet --
the per-plant, per-month, per-fuel-type aggregate of fuel deliveries
filed on EIA Form 923. Every U.S. power plant >1 MW reports monthly
volumes received, $/MMBtu, heat content, and sulfur/ash/moisture
content. This is the canonical structural anchor for the supply
stack's fuel-price inputs -- delivered prices, plant-specific,
implicitly carrying pipeline basis + transport + contract terms.

Source: ``s3://pudl.catalyst.coop/stable/out_eia923__monthly_fuel_receipts_costs.parquet``
PUDL docs: https://docs.catalyst.coop/pudl/data_sources/eia923.html

Upserts to ``eia.eia923_fuel_receipts_monthly`` keyed on
``(plant_id_eia, report_date, fuel_type_code_pudl)``.

US-wide coverage. dbt downstream filters to PJM via the plant
audit/crosswalk tables.

Reporting lag: typically 3-6 months (EIA-923 has a 60-day filing
window; PUDL re-builds quarterly to ``stable``).

Orchestration: scheduled (monthly, mid-month after PUDL's monthly
stable refresh).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from backend.scrapes.eia._pudl import read_pudl_table
from backend.utils import (
    azure_postgresql_utils as azure_postgresql,
    logging_utils,
    pipeline_run_logger,
)

# Config

API_SCRAPE_NAME = "eia923_fuel_receipts_monthly"
PUDL_TABLE = "out_eia923__monthly_fuel_receipts_costs"

# Pull every column except data_maturity (PUDL maturity flag, not interesting
# downstream). All columns are documented in the docstring schema above.
COLUMNS: list[str] = [
    "report_date",
    "plant_id_eia",
    "plant_id_pudl",
    "plant_name_eia",
    "utility_id_eia",
    "utility_id_pudl",
    "utility_name_eia",
    "state",
    "fuel_type_code_pudl",
    "fuel_received_units",
    "fuel_mmbtu_per_unit",
    "fuel_cost_per_mmbtu",
    "fuel_consumed_mmbtu",
    "total_fuel_cost",
    "fuel_cost_per_mmbtu_source",
    "sulfur_content_pct",
    "ash_content_pct",
    "mercury_content_ppm",
    "moisture_content_pct",
    "chlorine_content_ppm",
]

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


# Core pipeline functions


def _pull() -> pd.DataFrame:
    logger.section(f"Reading {PUDL_TABLE}.parquet from PUDL S3...")
    df = read_pudl_table(PUDL_TABLE, columns=COLUMNS)
    logger.success(
        f"  {len(df):,} rows, "
        f"{df['plant_id_eia'].nunique():,} plants, "
        f"{df['report_date'].min()} -> {df['report_date'].max()}"
    )
    return df


def _format(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["report_date"] = pd.to_datetime(df["report_date"]).dt.normalize()
    df["scrape_timestamp"] = pd.Timestamp.utcnow().tz_localize(None)
    df["source_url"] = f"s3://pudl.catalyst.coop/stable/{PUDL_TABLE}.parquet"

    # PUDL uses pyarrow dictionary types for some categorical cols; cast
    # to plain str so the upsert helper's CSV path doesn't choke.
    for col in ("fuel_type_code_pudl", "fuel_cost_per_mmbtu_source", "state"):
        if col in df.columns:
            df[col] = df[col].astype("object").astype(str)

    df = df.sort_values(["plant_id_eia", "report_date", "fuel_type_code_pudl"])
    df = df.drop_duplicates(
        subset=["plant_id_eia", "report_date", "fuel_type_code_pudl"], keep="last"
    )
    return df.reset_index(drop=True)


def _upsert(
    df: pd.DataFrame,
    schema: str = "eia",
    table_name: str = API_SCRAPE_NAME,
    primary_key: list[str] | None = None,
) -> None:
    primary_key = primary_key or [
        "plant_id_eia",
        "report_date",
        "fuel_type_code_pudl",
    ]
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
            logger.section("No rows returned, skipping upsert.")
            run.success(rows_processed=0)
            return df

        logger.section(
            f"Upserting {len(df):,} rows "
            f"({df['plant_id_eia'].nunique():,} plants, "
            f"{df['report_date'].min().date()} -> {df['report_date'].max().date()})..."
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
