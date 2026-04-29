import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils, model_cache_utils


logger = logging.getLogger(__name__)

SCRAPES = [
    (
        "backend.scrapes.energy_aspects.timeseries.ea_pjm_installed_capacity_monthly",
        "ea_pjm_installed_capacity_monthly",
    ),
]

# Marts to build and export. The leading `+` selects all upstream
# (source/staging) dependencies in the same dbt invocation.
MARTS = [
    "ea_pjm_installed_capacity_monthly",
]


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+ea_pjm_installed_capacity_monthly')."""
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


@flow(name="EA PJM Installed Capacity Monthly")
def ea_pjm_installed_capacity_monthly():
    """Monthly umbrella flow — scrape Energy Aspects PJM installed-capacity
    forecast, build dbt mart, export parquet for the modelling cache.

    Energy Aspects publishes monthly revisions out through ~2030 with
    fuel-disaggregated capacity (NG, coal, nuclear, oil, solar, on/offshore
    wind, hydro, battery). This is the forward-looking capacity baseline
    used by reserve-margin features in the forecasting models — see
    modelling/@TODO/pjm-research-for-modelling/backward_vs_forward_looking.md.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="ea_pjm_installed_capacity_monthly", source="energy_aspects",
    )
    run.start()
    scrape_failures: list[str] = []
    try:
        # ────── 1. Scrape latest EA installed-capacity feed ──────
        for module_path, label in SCRAPES:
            try:
                run_scrape(module_path)
            except Exception as scrape_err:
                scrape_failures.append(label)
                logger.exception(f"{label} scrape failed: {scrape_err}")

        # ────── 2. Run dbt for the mart (incl. upstream source/staging) ──────
        select = " ".join(f"+{mart}" for mart in MARTS)
        run_dbt(select)

        # ────── 3. Pull each mart from Postgres and export to parquet ──────
        for mart in MARTS:
            df = azure_postgresql_utils.pull_from_db(
                f"SELECT * FROM {DBT_SCHEMA}.{mart}"
            )
            model_cache_utils.write_mart_cache(df, mart=mart, pipeline_name=__name__)

        if scrape_failures:
            raise RuntimeError(
                f"Flow completed but {len(scrape_failures)} scrape(s) failed: {scrape_failures}"
            )

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    ea_pjm_installed_capacity_monthly()
