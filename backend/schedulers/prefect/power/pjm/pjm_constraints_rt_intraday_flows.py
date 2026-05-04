import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR
from backend.utils import logging_utils, pipeline_run_logger


logger = logging.getLogger(__name__)


# RT-side feeds for the binding-constraints pivot.
#   rt_marginal_value         - 5-min shadow prices (aggregated to hourly in
#                               dbt staging via DATE_TRUNC + AVG). Uses the
#                               orchestration poll-and-land wrapper (PJM
#                               posts daily on business days, 11 AM-12 PM ET);
#                               the wrapper polls every 60s for up to 2h
#                               so a fire just before publish is fine.
#   rt_default_mv_override    - long-running penalty-factor reference table
#                               (rolling-window scrape; small reference data)
#   rt_short_term_mv_override - short-term operator override events
#                               (rolling-window scrape; small reference data)
SCRAPES = [
    ("backend.orchestration.power.pjm.rt_marginal_value",   "rt_marginal_value"),
    ("backend.scrapes.power.pjm.rt_default_mv_override",    "rt_default_mv_override"),
    ("backend.scrapes.power.pjm.rt_short_term_mv_override", "rt_short_term_mv_override"),
]


# Terminal mart. Same selection as the DA flow — both sides rebuild the same
# unified pivot. DART rows surface only when both DA and RT bound the same
# (hour, monitored, contingency), so the view self-heals as each side lands.
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


@flow(name="PJM Constraints RT Daily")
def pjm_constraints_rt_intraday():
    """RT-side flow — pull RT binding-constraint shadow prices and override
    reference tables, then rebuild the unified DA/RT/DART pivot mart.

    Runs once per business day after PJM's 11 AM-12 PM ET publish window.
    The rt_marginal_value step uses the orchestration poll-and-land wrapper
    so the run survives a late publish. Override tables are small reference
    data via rolling-window scrapes; full re-pulls are cheap.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_constraints_rt_intraday", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape RT feeds (rolling-window backfill) ──────
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
    pjm_constraints_rt_intraday()
