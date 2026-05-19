"""Prefect flow: scrape PJM reserve_market_results, build dbt mart, export parquet.

Reserve-market clearing is a backward-only feed (today and forward dates
return empty), so the umbrella runs daily a few hours after the gas-day
boundary: pull the rolling D-7..D+2 window into ``pjm.reserve_market_results``,
rebuild the ``pjm_reserve_market_results_hourly`` mart, mirror it to the
modelling parquet cache. Consumed by ``modelling/da_models/supply_stack/``
to replace the static operating-reserve constant with per-hour cleared
requirement.
"""

from __future__ import annotations

import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import (
    azure_postgresql_utils,
    logging_utils,
    model_cache_utils,
    pipeline_run_logger,
)

logger = logging.getLogger(__name__)

SCRAPES: list[tuple[str, str]] = [
    ("backend.scrapes.power.pjm.reserve_market_results", "reserve_market_results"),
]

MARTS: list[str] = [
    "pjm_reserve_market_results_hourly",
]


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_reserve_market_results_hourly')."""
    dbt_logger = logging_utils.init_logging(
        name="DBT_RUN",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )
    dbt_logger.header("dbt")
    dbt_logger.section(f"Running dbt: select={select}")
    result = dbtRunner().invoke(
        [
            "run",
            "--select",
            select,
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROJECT_DIR,
        ]
    )
    if not result.success:
        dbt_logger.error(f"dbt run failed: {result.exception}")
        raise RuntimeError(f"dbt run failed: {result.exception}")
    dbt_logger.info(f"dbt run completed successfully: select={select}")


@task(name="scrape", retries=3, retry_delay_seconds=[30, 120, 300])
def run_scrape(module_path: str) -> None:
    mod = importlib.import_module(module_path)
    mod.main()


@flow(name="PJM Reserve Market Results Hourly")
def pjm_reserve_market_results_hourly() -> None:
    """Daily umbrella: scrape the cleared reserve market, rebuild the mart,
    mirror the parquet for the supply-stack model.

    Any scrape exception is a hard failure (no NoDataFoundException variant
    for this feed -- empty windows just upsert zero rows and the scrape's
    own log says "No data returned ..., skipping upsert"). The dbt step
    still runs even if a window is partly empty, so the mart reflects the
    last good upsert."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_reserve_market_results_hourly",
        source="power",
    )
    run.start()
    hard_failures: list[str] = []
    try:
        # 1. Scrape latest D-7..D+2 window into pjm.reserve_market_results.
        for module_path, label in SCRAPES:
            try:
                run_scrape(module_path)
            except Exception as scrape_err:  # noqa: BLE001
                hard_failures.append(label)
                logger.exception(f"{label} scrape failed: {scrape_err}")

        # 2. Rebuild the dbt mart (view).
        select = " ".join(f"+{mart}" for mart in MARTS)
        run_dbt(select)

        # 3. Pull each mart from Postgres and mirror to the parquet cache.
        for mart in MARTS:
            df = azure_postgresql_utils.pull_from_db(
                f"SELECT * FROM {DBT_SCHEMA}.{mart}"
            )
            model_cache_utils.write_mart_cache(df, mart=mart, pipeline_name=__name__)

        if hard_failures:
            raise RuntimeError(
                f"Flow completed but {len(hard_failures)} scrape(s) hard-failed: {hard_failures}"
            )
        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_reserve_market_results_hourly()
