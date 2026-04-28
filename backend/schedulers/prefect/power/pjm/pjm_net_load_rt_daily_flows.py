import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils, model_cache_utils


logger = logging.getLogger(__name__)

DBT_MARTS = [
    "pjm_solar_gen_rt_hourly",
    "pjm_wind_gen_rt_hourly",
    "pjm_net_load_rt_hourly",
]

EXPORT_MART = "pjm_net_load_rt_hourly"

SCRAPES = [
    ("backend.scrapes.power.pjm.solar_generation_by_area", "solar_gen_by_area"),
    ("backend.scrapes.power.pjm.wind_generation_by_area",  "wind_gen_by_area"),
]


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax."""
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


@flow(name="PJM Net Load RT Daily")
def pjm_net_load_rt_daily():
    """Daily realized solar/wind generation + derived net load.

    Pulls PJM solar and wind generation by area, rebuilds the two generation marts
    plus the net-load mart (load - solar - wind), then exports the net-load mart
    to the modelling cache. Scheduled after pjm_load_rt_daily so the upstream
    pjm_load_rt_hourly values driving the net-load join are already fresh.

    Scrapes are loosely coupled: failure in one does not block the other or dbt.
    The net-load mart is left-joined on solar/wind, so a missing scrape leaves
    those columns NULL for the affected hours and net_load_mw is NULL for those
    rows by design.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_net_load_rt_daily", source="power",
    )
    run.start()
    scrape_failures: list[str] = []
    try:
        # ────── 1. Scrape solar + wind generation by area ──────
        for module_path, label in SCRAPES:
            try:
                run_scrape(module_path)
            except Exception as scrape_err:
                scrape_failures.append(label)
                logger.exception(f"{label} scrape failed: {scrape_err}")

        # ────── 2. Rebuild the three incremental marts ──────
        # Explicit list (not `+pjm_net_load_rt_hourly`) so we don't re-run
        # pjm_load_rt_hourly, which has its own dedicated daily flow.
        run_dbt(" ".join(DBT_MARTS))

        # ────── 3. Pull the net-load mart and export to the cache ──────
        df = azure_postgresql_utils.pull_from_db(
            f"SELECT * FROM {DBT_SCHEMA}.{EXPORT_MART}"
        )
        model_cache_utils.write_mart_cache(df, mart=EXPORT_MART, pipeline_name=__name__)

        if scrape_failures:
            raise RuntimeError(
                f"Flow completed but {len(scrape_failures)} scrape(s) failed: {scrape_failures}"
            )

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_net_load_rt_daily()
