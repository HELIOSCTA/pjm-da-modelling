"""
EIA-923 Per-Delivery Fuel Receipts and Costs (via PUDL).

Pulls PUDL's ``core_eia923__fuel_receipts_costs`` parquet -- the raw
per-delivery rows underneath the monthly aggregate. Each row is one
fuel shipment to one plant with the supplier, mine (for coal),
contract type (spot vs term), transportation mode, and natural-gas
delivery firmness. Strictly richer than
``eia923_fuel_receipts_monthly``: that table is a `groupby(plant,
month, fuel_type).sum()` of this one.

Source: ``s3://pudl.catalyst.coop/stable/core_eia923__fuel_receipts_costs.parquet``
PUDL docs: https://docs.catalyst.coop/pudl/data_sources/eia923.html

Upserts to ``eia.eia923_fuel_receipts_deliveries`` keyed on
``(plant_id_eia, report_date, delivery_num)`` where ``delivery_num``
is a deterministic 1..N sequence per ``(plant_id_eia, report_date)``
ordered by ``(energy_source_code, supplier_name, mine_id_pudl,
fuel_received_units, fuel_cost_per_mmbtu)``. PUDL has no native
delivery ID; this ordering is reproducible across runs so re-scrapes
overwrite the same logical rows.

Use cases beyond price-anchoring
--------------------------------
- Spot-vs-contract mix per plant (contract_type_code) -> proxy for
  fuel-cost stickiness when commodity prices move.
- Contract expiration dates -> forward-looking re-pricing risk.
- Mine-level coal sourcing (mine_id_pudl, supplier_name) -> trace
  outages at specific mines to plants that depend on them.
- Natural-gas firmness (natural_gas_transport_code: F=firm,
  I=interruptible) -> dispatch risk on winter peak days.

Reporting lag and orchestration: same as the monthly version --
monthly refresh after PUDL's stable build.
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

API_SCRAPE_NAME = "eia923_fuel_receipts_deliveries"
PUDL_TABLE = "core_eia923__fuel_receipts_costs"

COLUMNS: list[str] = [
    "plant_id_eia",
    "report_date",
    "contract_type_code",
    # contract_expiration_date dropped here: ~57% NaT, and the shared
    # upsert helper applies df.fillna(0) which turns NaT into the int 0
    # and Postgres rejects as "date 0". Re-add when the helper grows
    # nullable-datetime support; consumers needing contract maturity
    # can read it directly from the PUDL parquet.
    "energy_source_code",
    "fuel_type_code_pudl",
    "fuel_group_code",
    "mine_id_pudl",
    "supplier_name",
    "fuel_received_units",
    "fuel_mmbtu_per_unit",
    "sulfur_content_pct",
    "ash_content_pct",
    "mercury_content_ppm",
    "fuel_cost_per_mmbtu",
    "primary_transportation_mode_code",
    "secondary_transportation_mode_code",
    "natural_gas_transport_code",
    "natural_gas_delivery_contract_type_code",
    "moisture_content_pct",
    "chlorine_content_ppm",
]

# Deterministic ordering within (plant, month) for delivery_num assignment.
# Columns chosen so re-scrapes of the same source data produce identical
# delivery_num. NaN-bearing columns are pushed to the end via na_position.
DELIVERY_ORDER_COLS: list[str] = [
    "energy_source_code",
    "supplier_name",
    "mine_id_pudl",
    "fuel_received_units",
    "fuel_cost_per_mmbtu",
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

    # All string-code columns -> plain str with "" for nulls. The shared
    # upsert helper's type inference reads df[col].iloc[0]; pandas NAType
    # and float NaN both blow up the dispatch, so normalize here.
    string_cols = (
        "contract_type_code",
        "energy_source_code",
        "fuel_type_code_pudl",
        "fuel_group_code",
        "supplier_name",
        "primary_transportation_mode_code",
        "secondary_transportation_mode_code",
        "natural_gas_transport_code",
        "natural_gas_delivery_contract_type_code",
    )
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").fillna("").astype(str)

    # Deterministic delivery_num within (plant_id_eia, report_date).
    df = df.sort_values(
        ["plant_id_eia", "report_date", *DELIVERY_ORDER_COLS],
        na_position="last",
        kind="mergesort",  # stable
    )
    df["delivery_num"] = df.groupby(["plant_id_eia", "report_date"]).cumcount() + 1

    df["scrape_timestamp"] = pd.Timestamp.utcnow().tz_localize(None)
    df["source_url"] = f"s3://pudl.catalyst.coop/stable/{PUDL_TABLE}.parquet"

    return df.reset_index(drop=True)


def _upsert(
    df: pd.DataFrame,
    schema: str = "eia",
    table_name: str = API_SCRAPE_NAME,
    primary_key: list[str] | None = None,
) -> None:
    primary_key = primary_key or ["plant_id_eia", "report_date", "delivery_num"]
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

        logger.section("Formatting + assigning delivery_num...")
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
