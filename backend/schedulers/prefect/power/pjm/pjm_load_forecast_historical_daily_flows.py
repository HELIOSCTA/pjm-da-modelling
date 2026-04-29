import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils, model_cache_utils


logger = logging.getLogger(__name__)

MART = "pjm_load_forecast_hourly_da_cutoff_historical"

SCRAPE_MODULE = "backend.scrapes.power.pjm.historical_load_forecasts"


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_load_forecast_hourly_da_cutoff_historical')."""
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


@task(name="scrape", retries=3, retry_delay_seconds=[30, 120, 300])
def run_scrape(module_path: str) -> None:
    mod = importlib.import_module(module_path)
    mod.main()


@flow(name="PJM Load Forecast Historical Daily")
def pjm_load_forecast_historical_daily():
    """Daily catch-up of PJM historical load forecast vintages.

    Pulls the last 7 days of preserved vintages from PJM's load_frcstd_hist
    feed (which retains ~5-6 vintages per delivery hour back to 2011-01-01),
    rebuilds the D-1 @ 10:00 AM EPT cutoff mart, and publishes the parquet
    to the modelling cache (local + Azure Blob).
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_load_forecast_historical_daily", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape last 7 days of vintages ──────
        run_scrape(SCRAPE_MODULE)

        # ────── 2. Rebuild historical DA-cutoff mart (+ upstream) ──────
        run_dbt(f"+{MART}")

        # ────── 3. Pull mart from Postgres and export to parquet / blob ──────
        df = azure_postgresql_utils.pull_from_db(
            f"SELECT * FROM {DBT_SCHEMA}.{MART}"
        )
        model_cache_utils.write_mart_cache(df, mart=MART, pipeline_name=__name__)

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_load_forecast_historical_daily()
