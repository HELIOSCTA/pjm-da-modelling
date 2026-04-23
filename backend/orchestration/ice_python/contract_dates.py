"""Task Scheduler entry point for the PJM short-term ICE contract-dates scrape.

Wraps `backend.scrapes.ice_python.contract_dates.runner_pjm_short_term`
with:
  1. A weekday gate (Mon–Fri MT) as a backstop for ad-hoc weekend fires.
     The contract calendar is idempotent on re-runs, so a weekend pull isn't
     harmful — just stale and duplicative.
  2. A narrow ICE-transient retry for cold-start COM failures.

Intended cadence: hourly 05:00–10:00 MT Mon–Fri. Task Scheduler owns the
window via .ps1; this gate only catches off-schedule invocations.

Usage (local Windows host, via Task Scheduler):
    python -m backend.orchestration.ice_python.contract_dates
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from backend.orchestration.ice_python._policies import (
    ice_transient_retry_policy,
    is_weekday,
    TRADING_TZ,
)
from backend.scrapes.ice_python.contract_dates import runner_pjm_short_term
from backend.utils import logging_utils

API_SCRAPE_NAME = "orchestration_ice_python_contract_dates"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


@ice_transient_retry_policy(attempts=2)
def _run_scrape() -> None:
    runner_pjm_short_term.main()


def main() -> int:
    """Run the contract-dates scrape if it's a weekday. Returns an exit code."""
    try:
        logger.header(API_SCRAPE_NAME)

        now = datetime.now(TRADING_TZ)
        if not is_weekday(now):
            logger.info(
                f"Weekend fire ({now:%Y-%m-%d %H:%M %Z}, "
                f"{now.strftime('%a')}) — skipping."
            )
            return 0

        logger.section(
            f"Weekday ({now:%Y-%m-%d %H:%M %Z}) — invoking contract-dates scrape"
        )
        _run_scrape()
        logger.success("Contract-dates scrape completed")
        return 0

    except Exception as exc:
        logger.exception(f"Orchestration failed: {exc}")
        return 1

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    sys.exit(main())
