import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from gridstatus.base import NoDataFoundException
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR, DBT_SCHEMA
from backend.utils import logging_utils, pipeline_run_logger, azure_postgresql_utils, model_cache_utils


logger = logging.getLogger(__name__)

SCRAPES = [
    ("backend.scrapes.power.pjm.seven_day_load_forecast_v1_2025_08_13", "load"),
    ("backend.scrapes.power.gridstatus.pjm.pjm_solar_forecast_hourly", "solar"),
    ("backend.scrapes.power.gridstatus.pjm.pjm_wind_forecast_hourly", "wind"),
]

MARTS = [
    "pjm_load_forecast_hourly_da_cutoff_historical",
    "pjm_solar_forecast_hourly_da_cutoff_historical",
    "pjm_wind_forecast_hourly_da_cutoff_historical",
    "pjm_net_load_forecast_hourly_da_cutoff_historical",
]


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


@flow(name="PJM Forecast Hourly")
def pjm_forecast_hourly():
    """Hourly umbrella flow — scrape load/solar/wind forecasts, incrementally
    rebuild the historical DA-cutoff marts (load, solar, wind, net_load), export parquet.

    The historical marts are the canonical source: they materialize every as_of_date
    snapshot, with `as_of_date = today (EPT)` matching what the live mart would show.
    Incremental dbt only recomputes the trailing 3-day window per run.

    Scrapes are loosely coupled: a failure in one does not block the others or dbt,
    so the net-load mart still rebuilds from whatever fresh inputs landed.

    `NoDataFoundException` from the upstream PJM API is treated as a soft outcome
    (warning, flow still succeeds) — it's a transient publish gap, not a real error.
    Any other exception is a hard failure and raises.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_forecast_hourly", source="power",
    )
    run.start()
    hard_failures: list[str] = []
    no_data: list[str] = []
    try:
        # ────── 1. Scrape latest forecasts ──────
        for module_path, label in SCRAPES:
            try:
                run_scrape(module_path)
            except NoDataFoundException as scrape_err:
                no_data.append(label)
                logger.warning(f"{label}: upstream has no data yet — {scrape_err}")
            except Exception as scrape_err:
                hard_failures.append(label)
                logger.exception(f"{label} scrape failed: {scrape_err}")

        # ────── 2. Run dbt for all four forecast marts in one invocation ──────
        #   dbt still runs even when a scrape had no-data, so the net-load mart
        #   rebuilds from whatever fresh inputs landed.
        select = " ".join(f"+{mart}" for mart in MARTS)
        run_dbt(select)

        # ────── 3. Pull each mart from Postgres and export to parquet ──────
        for mart in MARTS:
            df = azure_postgresql_utils.pull_from_db(
                f"SELECT * FROM {DBT_SCHEMA}.{mart}"
            )
            model_cache_utils.write_mart_cache(df, mart=mart, pipeline_name=__name__)

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
    pjm_forecast_hourly()
