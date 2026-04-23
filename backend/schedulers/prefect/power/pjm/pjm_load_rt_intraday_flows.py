import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow

from backend.settings import CACHE_DIR, DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils


logger = logging.getLogger(__name__)

MART = "pjm_load_rt_hourly"


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_load_rt_hourly')."""
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


@flow(name="PJM Load RT Intraday")
def pjm_load_rt_intraday():
    """Hourly intraday RT load — scrape 5-min instantaneous, rebuild RT hourly mart, export parquet.

    The mart is incremental with a 10-day lookback. The instantaneous values land at the
    bottom of the priority hierarchy, so once daily metered/prelim arrive the daily flow
    overrides them naturally.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_load_rt_intraday", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape latest 5-min instantaneous load ──────
        mod = importlib.import_module(
            "backend.scrapes.power.pjm.five_min_instantaneous_load_v1_2025_OCT_15"
        )
        mod.main()

        # ────── 2. Rebuild incremental RT hourly mart (+ upstream) ──────
        run_dbt(f"+{MART}")

        # ────── 3. Pull mart from Postgres and export to parquet ──────
        df = azure_postgresql_utils.pull_from_db(
            f"SELECT * FROM {DBT_SCHEMA}.{MART}"
        )
        cache_file = CACHE_DIR / f"{MART}.parquet"
        df.to_parquet(cache_file, index=False)
        logger.info(f"Wrote {len(df):,} rows → {cache_file}")

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_load_rt_intraday()
