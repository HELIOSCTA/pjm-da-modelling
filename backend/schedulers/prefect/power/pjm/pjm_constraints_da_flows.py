import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR
from backend.utils import logging_utils, pipeline_run_logger


logger = logging.getLogger(__name__)


# DA-side feeds for the binding-constraints pivot. Both use the
# orchestration/ poll-and-land variants: the flow runs at clearing time and
# blocks until data appears (2h ceiling) instead of silently no-op'ing.
SCRAPES = [
    ("backend.orchestration.power.pjm.da_transmission_constraints", "da_transmission_constraints"),
    ("backend.orchestration.power.pjm.da_marginal_value",           "da_marginal_value"),
]


# Terminal mart. The `+` prefix walks upstream (DA + RT sources, dates utility,
# long-form view) so this single invocation rebuilds the unified DA/RT/DART
# pivot. The RT flow runs the same dbt selection — DART rows materialize
# correctly only after both DA and RT data are present, so the pivot
# self-heals as each side lands.
MARTS = [
    "pjm_constraints_hourly_pivot",
]


def _dbt_logger():
    return logging_utils.init_logging(
        name="DBT_RUN",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_constraints_hourly_pivot')."""
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


@flow(name="PJM Constraints DA")
def pjm_constraints_da():
    """DA-side flow — poll PJM after market clear for tomorrow's DA binding
    constraints + marginal values, upsert to Postgres, then rebuild the unified
    DA/RT/DART pivot mart.

    Scrapes run sequentially; each blocks via `_wait_for_data`'s 2-hour poll
    ceiling if PJM hasn't published yet. dbt runs only after both scrapes land.
    Failure of any scrape re-raises so Prefect surfaces it.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_constraints_da", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape DA constraint feeds (poll until available) ──────
        for module_path, _label in SCRAPES:
            run_scrape(module_path)

        # ────── 2. Build the pivot mart (one invocation walks upstream) ──────
        select = " ".join(f"+{mart}" for mart in MARTS)
        run_dbt(select)

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_constraints_da()
