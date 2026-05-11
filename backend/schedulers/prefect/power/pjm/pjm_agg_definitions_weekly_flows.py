import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR
from backend.utils import logging_utils, pipeline_run_logger


logger = logging.getLogger(__name__)


# Single static-reference scrape. Direct import — no orchestration
# wrapper needed because the API returns the full feed unconditionally
# (no "wait for tomorrow's data to land" semantics).
SCRAPES = [
    ("backend.scrapes.power.pjm.agg_definitions", "agg_definitions"),
]


# Terminal mart. The `+` prefix walks upstream (source + staging + mart),
# so this single invocation rebuilds the active-aggregates table from
# the freshly upserted source data.
MARTS = [
    "pjm_agg_definitions_active",
]


def _dbt_logger():
    return logging_utils.init_logging(
        name="DBT_RUN",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_agg_definitions_active')."""
    log = _dbt_logger()
    log.header("dbt")
    log.section(f"Running dbt: select={select}")
    result = dbtRunner().invoke(
        [
            "run",
            "--select",
            select,
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROJECT_DIR,
        ]
    )
    if not result.success:
        log.error(f"dbt run failed: {result.exception}")
        raise RuntimeError(f"dbt run failed: {result.exception}")
    log.info(f"dbt run completed successfully: select={select}")


@task(name="scrape", retries=1)
def run_scrape(module_path: str) -> None:
    mod = importlib.import_module(module_path)
    mod.main()


@flow(name="PJM Aggregate Definitions Weekly")
def pjm_agg_definitions_weekly():
    """Weekly flow — refresh PJM aggregate-pnode → bus-pnode mappings,
    then rebuild the active-aggregates mart.

    Static reference data: the upstream feed barely changes, so the
    composite-PK upsert is effectively idempotent and most weekly runs
    add zero new rows. The mart rebuild is cheap (~3s for 28k rows).
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_agg_definitions_weekly",
        source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape full active feed and upsert to pjm.agg_definitions ──────
        for module_path, _label in SCRAPES:
            run_scrape(module_path)

        # ────── 2. Rebuild the active-aggregates mart ──────
        select = " ".join(f"+{mart}" for mart in MARTS)
        run_dbt(select)

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_agg_definitions_weekly()
