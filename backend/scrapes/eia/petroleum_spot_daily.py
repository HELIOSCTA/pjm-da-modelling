"""
EIA Petroleum Spot Prices (daily + weekly).

Pulls the petroleum spot series that map to supply-stack oil-unit
fuel inputs, plus WTI and Brent as reference benchmarks. Some series
are daily, some weekly; both upsert to ``eia.petroleum_spot_daily``
keyed on ``(product_code, date)``.

Source: ``/v2/petroleum/pri/spt/data/`` with ``facets[series][]=<CODE>``.

Fleet mapping (modelling/da_models/supply_stack/configs.py)
-----------------------------------------------------------
- "NY No 2 Distillate"   -> EER_EPD2F_PF4_Y35NY_DPG (NY Harbor No 2
                            Heating Oil Spot FOB, daily, $/gal)
- "NY No 54 Jet Fuel"    -> EER_EPJK_PF4_RGC_DPG    (Gulf Coast jet
                            kerosene -- closest EIA proxy for NY jet,
                            weekly, $/gal)

Gulf Coast No 6 residual fuel oil is NOT in the v2 API (EIA stopped
publishing the daily series ~2012). It's scraped separately from the
Weekly Petroleum Status Report -- see ``petroleum_no6_weekly.py``.

Orchestration: scheduled (daily, ~10am ET; weekly series hold their
last-published value between Thursday refreshes).
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

API_SCRAPE_NAME = "petroleum_spot_daily"

# product_code -> (series, frequency, units_hint, fuel_role, product_name)
PETROLEUM_SERIES: dict[str, dict] = {
    "WTI": {
        "series": "RWTC",
        "frequency": "daily",
        "product_name": "Cushing OK WTI Spot",
        "units_hint": "USD/barrel",
        "fuel_role": "benchmark",
    },
    "BRENT": {
        "series": "RBRTE",
        "frequency": "daily",
        "product_name": "Europe Brent Spot",
        "units_hint": "USD/barrel",
        "fuel_role": "benchmark",
    },
    "NY_NO2": {
        "series": "EER_EPD2F_PF4_Y35NY_DPG",
        "frequency": "daily",
        "product_name": "NY Harbor No 2 Heating Oil Spot",
        "units_hint": "USD/gallon",
        "fuel_role": "distillate_oil",
    },
    "GC_JET": {
        "series": "EER_EPJK_PF4_RGC_DPG",
        "frequency": "weekly",
        "product_name": "Gulf Coast Kerosene-Type Jet Fuel Spot",
        "units_hint": "USD/gallon",
        "fuel_role": "jet_kerosene",
    },
}

# Conversion factors to $/MMBtu (high heating values).
# Source: EIA Monthly Energy Review, Appendix A.
USD_GAL_TO_USD_MMBTU = {
    "distillate_oil": 1 / 0.1387,  # 138,700 Btu/gal
    "jet_kerosene": 1 / 0.1355,  # 135,500 Btu/gal
}
USD_BBL_TO_USD_MMBTU = 1 / 5.800  # crude oil ~5.8 MMBtu/bbl

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


# Core pipeline functions


def _pull(start: str | None = None) -> pd.DataFrame:
    """Pull all configured petroleum spot series.

    EIA's spt route mixes daily and weekly series under one endpoint
    but the ``frequency`` query param scopes the response, so we
    query once per frequency group.
    """
    by_freq: dict[str, list[str]] = {}
    series_to_code: dict[str, str] = {}
    for code, cfg in PETROLEUM_SERIES.items():
        by_freq.setdefault(cfg["frequency"], []).append(cfg["series"])
        series_to_code[cfg["series"]] = code

    frames: list[pd.DataFrame] = []
    for freq, series_list in by_freq.items():
        logger.section(
            f"Fetching {len(series_list)} {freq} petroleum spot series "
            f"from {start or 'start-of-record'}..."
        )
        df = fetch_route_data(
            route="petroleum/pri/spt",
            frequency=freq,
            facets={"series": series_list},
            start=start,
        )
        if df.empty:
            logger.warning(f"EIA returned no rows for frequency={freq}")
            continue
        df["frequency"] = freq
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    raw = pd.concat(frames, ignore_index=True)
    raw["product_code"] = raw["series"].map(series_to_code)
    return raw


def _to_usd_mmbtu(row: pd.Series) -> float:
    role = row["fuel_role"]
    val = row["value"]
    if pd.isna(val):
        return float("nan")
    if role == "benchmark":
        return val * USD_BBL_TO_USD_MMBTU
    factor = USD_GAL_TO_USD_MMBTU.get(role)
    if factor is None:
        return float("nan")
    return val * factor


def _format(raw: pd.DataFrame) -> pd.DataFrame:
    """Shape the EIA response into the warehouse schema."""
    if raw.empty:
        return pd.DataFrame()

    meta = pd.DataFrame(
        [
            {
                "product_code": k,
                "product_name": v["product_name"],
                "units_hint": v["units_hint"],
                "fuel_role": v["fuel_role"],
            }
            for k, v in PETROLEUM_SERIES.items()
        ]
    )

    df = raw.merge(meta, on="product_code", how="left")
    df["price_usd_per_mmbtu"] = df.apply(_to_usd_mmbtu, axis=1)
    df = df.rename(
        columns={
            "period": "date",
            "value": "price_native",
            "units": "price_units",
            "series-description": "series_description",
        }
    )
    df["series_id"] = df["series"]

    df["scrape_timestamp"] = pd.Timestamp.utcnow().tz_localize(None)
    df["source_url"] = "https://api.eia.gov/v2/petroleum/pri/spt/data/"

    out_cols = [
        "product_code",
        "product_name",
        "fuel_role",
        "date",
        "price_native",
        "price_units",
        "price_usd_per_mmbtu",
        "series_id",
        "series_description",
        "frequency",
        "source_url",
        "scrape_timestamp",
    ]
    df = df[out_cols].dropna(subset=["date", "price_native"])
    df = df.sort_values(["product_code", "date", "scrape_timestamp"])
    # EIA occasionally republishes the same (series, period) -- keep the
    # most recent value for each PK tuple.
    df = df.drop_duplicates(subset=["product_code", "date"], keep="last")
    return df.reset_index(drop=True)


def _upsert(
    df: pd.DataFrame,
    schema: str = "eia",
    table_name: str = API_SCRAPE_NAME,
    primary_key: list[str] | None = None,
) -> None:
    primary_key = primary_key or ["product_code", "date"]
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
            logger.section("No petroleum spot rows returned, skipping upsert.")
            run.success(rows_processed=0)
            return df

        logger.section(
            f"Upserting {len(df)} rows ({df['product_code'].nunique()} products, "
            f"{df['date'].min().date()} -> {df['date'].max().date()})..."
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
