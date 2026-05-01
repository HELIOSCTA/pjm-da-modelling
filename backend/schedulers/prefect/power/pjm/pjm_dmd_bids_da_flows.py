import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR
from backend.utils import logging_utils, pipeline_run_logger


logger = logging.getLogger(__name__)


SCRAPE = ("backend.orchestration.power.pjm.hrl_dmd_bids", "hrl_dmd_bids")


# Terminal mart. The `+` prefix walks the source + staging upstream
# (source_v1_pjm_hrl_dmd_bids -> staging_v1_pjm_load_da_hourly), so this
# single invocation rebuilds the queryable DA load mart.
MARTS = [
    "pjm_load_da_hourly",
]


def _dbt_logger():
    return logging_utils.init_logging(
        name="DBT_RUN",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_load_da_hourly')."""
    log = _dbt_logger()
    log.header("dbt")
    log.section(f"Running dbt: select={select}")
    result = dbtRunner().invoke([
        "run",
        "--select", select,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
    ])
    if not result.success:
        log.error(f"dbt run failed: {result.exception}")
        raise RuntimeError(f"dbt run failed: {result.exception}")
    log.info(f"dbt run completed successfully: select={select}")


@task(name="scrape", retries=1)
def run_scrape(module_path: str) -> None:
    mod = importlib.import_module(module_path)
    mod.main()


@flow(name="PJM DMD Bids DA")
def pjm_dmd_bids_da():
    """Daily flow — poll PJM after market clear for tomorrow's DA hourly demand
    bids (cleared load by area and market region), then rebuild the
    pjm_load_da_hourly view mart (which adds the SOUTH = RTO - MIDATL - WEST
    derivation).

    The orchestration wrapper polls until data lands (2h ceiling). dbt runs
    only after the scrape's upsert succeeds; failure re-raises so Prefect
    surfaces it.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_dmd_bids_da", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape DA demand bids (poll until available) ──────
        module_path, _label = SCRAPE
        run_scrape(module_path)

        # ────── 2. Build the DA load mart ──────
        select = " ".join(f"+{mart}" for mart in MARTS)
        run_dbt(select)

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_dmd_bids_da()
