"""Task Scheduler entry point for the PJM short-term ICE ticker data scrape.

Wraps `backend.scrapes.ice_python.ticker_data.runner_pjm_short_term.main`
with:
  1. A weekday + market-hours gate (05:00–16:00 MT). Off-hours fires exit 0
     with a stdout note so Task Scheduler logs stay readable and the
     pipeline_runs table doesn't fill with no-op rows.
  2. A narrow ICE-transient retry for cold-start COM failures that raise
     before the per-symbol retry loop in get_timesales_batch can catch them.

Usage (local Windows host, via Task Scheduler):
    python -m backend.orchestration.ice_python.ticker_data
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

from backend.orchestration.ice_python._policies import (
    ice_transient_retry_policy,
    is_within_trading_hours,
    TRADING_TZ,
)
from backend.scrapes.ice_python.ticker_data import runner_pjm_short_term
from backend.utils import logging_utils

API_SCRAPE_NAME = "orchestration_ice_python_ticker_data"

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
    """Run the ICE ticker scrape if the market is open. Returns an exit code."""
    try:
        logger.header(API_SCRAPE_NAME)

        now = datetime.now(TRADING_TZ)
        if not is_within_trading_hours(now):
            logger.info(
                f"Outside trading hours ({now:%Y-%m-%d %H:%M %Z}, "
                f"weekday={now.strftime('%a')}) — skipping."
            )
            return 0

        logger.section(
            f"Trading hours ({now:%Y-%m-%d %H:%M %Z}) — invoking ticker scrape"
        )
        _run_scrape()
        logger.success("Ticker scrape completed")
        return 0

    except Exception as exc:
        logger.exception(f"Orchestration failed: {exc}")
        return 1

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    sys.exit(main())
