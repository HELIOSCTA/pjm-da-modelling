import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import azure_postgresql_utils, logging_utils, pipeline_run_logger, model_cache_utils


logger = logging.getLogger(__name__)

SCRAPE_MODULE = "backend.scrapes.wsi.hourly_observed_temp_v2_2025_07_22"
MART = "wsi_pjm_hourly_observed_temp"


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+wsi_hourly_observed_temp')."""
    dbt_logger = logging_utils.init_logging(
        name="DBT_RUN",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )
    dbt_logger.header("dbt")
    dbt_logger.section(f"Running dbt: select={select}")
    result = dbtRunner().invoke([
        "run",
        "--select", select,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
    ])
    if not result.success:
        dbt_logger.error(f"dbt run failed: {result.exception}")
        raise RuntimeError(f"dbt run failed: {result.exception}")
    dbt_logger.info(f"dbt run completed successfully: select={select}")


@task(name="scrape", retries=1)
def run_scrape(module_path: str) -> None:
    mod = importlib.import_module(module_path)
    mod.main()


@flow(name="WSI Hourly Observed")
def wsi_hourly_observed():
    """Daily catch-up: scrape rolling 7-day window of WSI observed weather, refresh incremental mart, export parquet."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="wsi_hourly_observed", source="weather",
    )
    run.start()
    try:
        # ────── 1. Scrape rolling 7-day observed window ──────
        run_scrape(SCRAPE_MODULE)

        # ────── 2. Incrementally refresh the observed mart (+ upstream) ──────
        run_dbt(f"+{MART}")

        # ────── 3. Pull mart from Postgres and export to parquet ──────
        df = azure_postgresql_utils.pull_from_db(f"SELECT * FROM {DBT_SCHEMA}.{MART}")
        model_cache_utils.write_mart_cache(df, mart=MART, pipeline_name=__name__)

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    wsi_hourly_observed()
