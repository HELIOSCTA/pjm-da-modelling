import importlib
import logging
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect import flow, task

from backend.settings import DBT_PROJECT_DIR
from backend.utils import logging_utils, pipeline_run_logger


logger = logging.getLogger(__name__)

SCRAPE = ("backend.scrapes.power.pjm.transmission_outages", "transmission_outages")

SNAPSHOT = "pjm_transmission_outages_snapshot"

# View marts. The `+` prefix tells dbt to include all upstream (source +
# staging) dependencies, so one invocation walks the shared ancestors once.
#
# Two 24h-changes variants run side-by-side for a week so the team can compare:
#   _simple   — uses created_at / last_revised on the source table; no diff
#               columns, no CLEARED detection, but works on day 1.
#   _snapshot — diffs against the SCD2 snapshot; surfaces prev_* columns and
#               CLEARED tickets, but returns empty on day 1 of running.
# Pick one and delete the other once that comparison is done.
MARTS = [
    "pjm_transmission_outages_active",
    "pjm_transmission_outages_window_7d",
    "pjm_transmission_outages_changes_24h_simple",
    "pjm_transmission_outages_changes_24h_snapshot",
]


def _dbt_logger():
    return logging_utils.init_logging(
        name="DBT_RUN",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )


def run_dbt_snapshot(select: str) -> None:
    """Run dbt snapshot to capture SCD2 history for the source.

    Must run after the scrape's upsert finishes, so the snapshot reflects today's
    state of pjm.transmission_outages. Builds rows in pjm_snapshots schema.
    """
    log = _dbt_logger()
    log.header("dbt")
    log.section(f"Running dbt snapshot: select={select}")
    result = dbtRunner().invoke([
        "snapshot",
        "--select", select,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
    ])
    if not result.success:
        log.error(f"dbt snapshot failed: {result.exception}")
        raise RuntimeError(f"dbt snapshot failed: {result.exception}")
    log.info(f"dbt snapshot completed successfully: select={select}")


def run_dbt(select: str) -> None:
    """Run dbt models by selection syntax (e.g. '+pjm_transmission_outages_active')."""
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


@flow(name="PJM Transmission Outages Daily")
def pjm_transmission_outages_daily():
    """Daily flow — scrape eDART linesout.txt, capture SCD2 snapshot, then build
    the three view marts (active / 7-day window / 24h changes).

    The dbt snapshot runs only after the scrape's upsert succeeds. If the scrape
    fails the snapshot is skipped so we don't capture a stale baseline; the flow
    re-raises so Prefect surfaces the failure.

    No Azure Blob export — the marts are queried directly from Postgres by the
    MCP server.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_transmission_outages_daily", source="power",
    )
    run.start()
    try:
        # ────── 1. Scrape latest outage feed ──────
        module_path, _label = SCRAPE
        run_scrape(module_path)

        # ────── 2. Capture SCD2 snapshot of today's state ──────
        run_dbt_snapshot(SNAPSHOT)

        # ────── 3. Build view marts (one invocation walks shared ancestors) ──────
        select = " ".join(f"+{mart}" for mart in MARTS)
        run_dbt(select)

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_transmission_outages_daily()
