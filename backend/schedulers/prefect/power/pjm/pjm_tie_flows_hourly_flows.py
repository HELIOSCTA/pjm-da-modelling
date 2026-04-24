import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils, model_cache_utils


logger = logging.getLogger(__name__)

SCRAPES = [
    ("backend.scrapes.power.pjm.five_min_tie_flows", "five_min_tie_flows"),
]

MARTS = [
    "pjm_tie_flows_hourly",
]


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_tie_flows_hourly')."""
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


@flow(name="PJM Tie Flows Hourly")
def pjm_tie_flows_hourly():
    """Hourly umbrella flow — scrape PJM 5-min tie flows, build dbt hourly
    aggregation mart, export parquet.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_tie_flows_hourly", source="power",
    )
    run.start()
    scrape_failures: list[str] = []
    try:
        # ────── 1. Scrape latest 5-min tie flows ──────
        for module_path, label in SCRAPES:
            try:
                run_scrape(module_path)
            except Exception as scrape_err:
                scrape_failures.append(label)
                logger.exception(f"{label} scrape failed: {scrape_err}")

        # ────── 2. Run dbt for the hourly aggregation mart ──────
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
    pjm_tie_flows_hourly()
