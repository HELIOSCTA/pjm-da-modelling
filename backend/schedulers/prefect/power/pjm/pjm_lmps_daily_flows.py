import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import CACHE_DIR, DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils


logger = logging.getLogger(__name__)

MART = "pjm_lmps_hourly"

SCRAPES = [
    ("backend.scrapes.power.pjm.da_hrl_lmps", "da_backfill"),
    ("backend.scrapes.power.pjm.rt_settlements_verified_hourly_lmps", "rt_verified"),
]


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_lmps_hourly')."""
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


@flow(name="PJM LMPs Daily")
def pjm_lmps_daily():
    """Daily LMPs truth-up — scrape DA backfill (7-day lookback) + RT verified, rebuild LMP hourly mart, export parquet.

    Scrapes are loosely coupled: failure in one does not block the other or dbt. The mart's
    priority hierarchy (verified > unverified) ensures whichever sources land will overwrite
    the prior intra-day unverified values within the 10-day incremental lookback.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_lmps_daily", source="power",
    )
    run.start()
    scrape_failures: list[str] = []
    try:
        # ────── 1. Scrape DA backfill + RT verified ──────
        for module_path, label in SCRAPES:
            try:
                run_scrape(module_path)
            except Exception as scrape_err:
                scrape_failures.append(label)
                logger.exception(f"{label} scrape failed: {scrape_err}")

        # ────── 2. Rebuild incremental LMP hourly mart (+ upstream) ──────
        run_dbt(f"+{MART}")

        # ────── 3. Pull mart from Postgres and export to parquet ──────
        df = azure_postgresql_utils.pull_from_db(f"SELECT * FROM {DBT_SCHEMA}.{MART}")
        cache_file = CACHE_DIR / f"{MART}.parquet"
        df.to_parquet(cache_file, index=False)
        logger.info(f"Wrote {len(df):,} rows → {cache_file}")

        if scrape_failures:
            raise RuntimeError(
                f"Flow completed but {len(scrape_failures)} scrape(s) failed: {scrape_failures}"
            )

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_lmps_daily()
