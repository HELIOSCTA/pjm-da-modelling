import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from gridstatus.base import NoDataFoundException
from prefect import flow, task

from backend.settings import CACHE_DIR, DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils


logger = logging.getLogger(__name__)

SCRAPES = [
    ("backend.scrapes.power.gridstatus.pjm.pjm_fuel_mix_hourly", "pjm_fuel_mix_hourly"),
]

MARTS = [
    "pjm_fuel_mix_hourly",
]


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_fuel_mix_hourly')."""
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


def _retry_unless_no_data(task, task_run, state) -> bool:
    # Skip retry when upstream just hasn't published yet — retrying in 30s won't fix a gap that lasts hours.
    try:
        state.result(raise_on_failure=True)
    except NoDataFoundException:
        return False
    except Exception:
        return True
    return True


@task(name="scrape", retries=3, retry_delay_seconds=[30, 120, 300], retry_condition_fn=_retry_unless_no_data)
def run_scrape(module_path: str) -> None:
    mod = importlib.import_module(module_path)
    mod.main()


@flow(name="PJM Fuel Mix Hourly")
def pjm_fuel_mix_hourly():
    """Hourly umbrella flow — scrape gridstatus PJM fuel mix, build dbt
    fuel-mix mart, export parquet.

    `NoDataFoundException` from the upstream PJM `gen_by_fuel` endpoint is treated
    as a soft outcome (warning, flow still succeeds) — it's a transient publish gap,
    not a real error. Any other exception is a hard failure and raises.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_fuel_mix_hourly", source="power",
    )
    run.start()
    hard_failures: list[str] = []
    no_data: list[str] = []
    try:
        # ────── 1. Scrape latest hourly fuel mix ──────
        for module_path, label in SCRAPES:
            try:
                run_scrape(module_path)
            except NoDataFoundException as scrape_err:
                no_data.append(label)
                logger.warning(f"{label}: upstream has no data yet — {scrape_err}")
            except Exception as scrape_err:
                hard_failures.append(label)
                logger.exception(f"{label} scrape failed: {scrape_err}")

        # ────── 2. Run dbt for the fuel-mix mart ──────
        #   dbt still runs when a scrape had no-data so the mart reflects the
        #   last good upsert.
        select = " ".join(f"+{mart}" for mart in MARTS)
        run_dbt(select)

        # ────── 3. Pull each mart from Postgres and export to parquet ──────
        for mart in MARTS:
            df = azure_postgresql_utils.pull_from_db(
                f"SELECT * FROM {DBT_SCHEMA}.{mart}"
            )
            cache_file = CACHE_DIR / f"{mart}.parquet"
            df.to_parquet(cache_file, index=False)
            logger.info(f"Wrote {len(df):,} rows → {cache_file}")

        if hard_failures:
            raise RuntimeError(
                f"Flow completed but {len(hard_failures)} scrape(s) hard-failed: {hard_failures}"
            )

        if no_data:
            logger.warning(f"Flow succeeded with no-data scrapes: {no_data}")
        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_fuel_mix_hourly()
