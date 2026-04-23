"""Task Scheduler entry point for the ICE next-day gas settles scrape.

Wraps `backend.scrapes.ice_python.next_day_gas.next_day_gas_v1_2025_dec_16`
with:
  1. A weekday gate (Mon–Fri MT). ICE doesn't post new gas settles on
     weekends, so a weekend fire would just re-upsert Friday's values.
  2. A narrow ICE-transient retry for cold-start COM failures.

Intended cadence: hourly 08:00–14:00 MT Mon–Fri. Task Scheduler owns the
window via .ps1; the weekday gate here only catches off-schedule fires.

Usage (local Windows host, via Task Scheduler):
    python -m backend.orchestration.ice_python.next_day_gas
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
from backend.scrapes.ice_python.next_day_gas import next_day_gas_v1_2025_dec_16
from backend.utils import logging_utils

API_SCRAPE_NAME = "orchestration_ice_python_next_day_gas"

logger = logging_utils.init_logging(
    name=API_SCRAPE_NAME,
    log_dir=Path(__file__).parent / "logs",
    log_to_file=True,
    delete_if_no_errors=True,
)


@ice_transient_retry_policy(attempts=2)
def _run_scrape() -> None:
    next_day_gas_v1_2025_dec_16.main()


def main() -> int:
    """Run the next-day gas scrape if it's a weekday. Returns an exit code."""
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
            f"Weekday ({now:%Y-%m-%d %H:%M %Z}) — invoking next-day gas scrape"
        )
        _run_scrape()
        logger.success("Next-day gas scrape completed")
        return 0

    except Exception as exc:
        logger.exception(f"Orchestration failed: {exc}")
        return 1

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    sys.exit(main())
