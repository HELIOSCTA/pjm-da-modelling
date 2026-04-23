import importlib
import logging
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta
from dbt.cli.main import dbtRunner
from prefect import flow


from backend.settings import CACHE_DIR, DBT_PROJECT_DIR, DBT_SCHEMA
from backend.schedulers.prefect.power.pjm.pjm_lmps_da_notifications import notify_da_lmps
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils

logger = logging.getLogger(__name__)

MART = "pjm_lmps_hourly"


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


@flow(name="PJM LMPs DA")
def pjm_lmps_da():
    """Day-Ahead Hourly LMPs — poll PJM API with tenacity retries, upsert to PostgreSQL, run dbt."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_lmps_da", source="power",
    )
    run.start()
    try:
        # ────── 1. Poll PJM API and upsert raw data to PostgreSQL ──────
        mod = importlib.import_module("backend.orchestration.power.pjm.da_hrl_lmps")
        mod.main()

        # ────── 2. Send Slack notification with LMP summary ──────
        target_date = (datetime.now() + relativedelta(days=1)).strftime("%Y-%m-%d")
        notify_da_lmps(target_date)

        # ────── 3. Rebuild incremental LMP hourly mart (+ upstream) ──────
        run_dbt(f"+{MART}")

        # ────── 4. Pull mart from Postgres and export to parquet ──────
        df = azure_postgresql_utils.pull_from_db(f"SELECT * FROM {DBT_SCHEMA}.{MART}")
        cache_file = CACHE_DIR / f"{MART}.parquet"
        df.to_parquet(cache_file, index=False)
        logger.info(f"Wrote {len(df):,} rows → {cache_file}")

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_lmps_da()
