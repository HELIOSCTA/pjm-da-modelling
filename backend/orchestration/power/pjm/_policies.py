"""Reusable tenacity policies for PJM orchestration wrappers."""
import logging

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_delay,
    stop_after_attempt,
    wait_exponential_jitter,
    wait_fixed,
)

logger = logging.getLogger("orchestration.power.pjm")


class DataNotYetAvailable(Exception):
    """Raised when the PJM API returns a successful but empty response."""


API_TRANSIENT = (
    DataNotYetAvailable,
    requests.ConnectionError,
    requests.Timeout,
    ConnectionResetError,
)


def api_poll_policy(max_seconds: int = 7_200, wait_seconds: int = 10):
    """Poll-until-available with fixed wait between attempts.

    Default 2h ceiling, 10s between polls — appropriate for DA LMPs which
    is the time-critical feed (downstream consumers wait on it). Less
    time-critical feeds (DMD bids, constraints) should pass a larger
    wait_seconds (e.g. 30 or 60) so they don't compete for HTTP / agent
    capacity with the LMPs poll.

    PJM DA LMPs are posted daily between 12:00–01:30 PM EPT
    (10:00–11:30 AM MST). Script starts at 11:00 AM MST so data
    is usually already there or imminent; aggressive polling catches
    it fast. 2h ceiling covers late postings.
    """
    return retry(
        stop=stop_after_delay(max_seconds),
        wait=wait_fixed(wait_seconds),
        retry=retry_if_exception_type(API_TRANSIENT),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


def api_transient_retry_policy(attempts: int = 3):
    """Short retry on transient HTTP / network errors. No polling.

    Use this around the download+upsert phase, AFTER the poll phase has
    confirmed data is available.
    """
    return retry(
        stop=stop_after_attempt(attempts),
        wait=wait_exponential_jitter(initial=10, max=120),
        retry=retry_if_exception_type(
            (requests.ConnectionError, requests.Timeout, ConnectionResetError)
        ),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
