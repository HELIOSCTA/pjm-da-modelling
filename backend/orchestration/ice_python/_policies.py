"""Reusable policies for ICE Python orchestration wrappers."""
from __future__ import annotations

import logging
from datetime import datetime

import pytz
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

logger = logging.getLogger("orchestration.ice_python")


# ────── Market-hours gate ──────
# ICE XL publisher runs on the host's local time (Mountain). We gate fires
# to weekday 05:00 ≤ hour < 16:00 MT so Task Scheduler can safely fire every
# few minutes all day without generating off-hours no-ops.
TRADING_TZ = pytz.timezone("America/Edmonton")
TRADING_START_HOUR = 5
TRADING_END_HOUR = 16  # half-open: the 16:00 fire is already gated off


def is_within_trading_hours(now: datetime | None = None) -> bool:
    """True on weekdays between [TRADING_START_HOUR, TRADING_END_HOUR) MT."""
    now = now or datetime.now(TRADING_TZ)
    if now.tzinfo is None:
        now = TRADING_TZ.localize(now)
    if now.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    return TRADING_START_HOUR <= now.hour < TRADING_END_HOUR


def is_weekday(now: datetime | None = None) -> bool:
    """True Mon–Fri in MT. Used by once-a-day scrapes (next-day gas settles,
    etc.) where the hour doesn't matter but weekend fires should no-op."""
    now = now or datetime.now(TRADING_TZ)
    if now.tzinfo is None:
        now = TRADING_TZ.localize(now)
    return now.weekday() < 5


# ────── Transient-error retry policy ──────
# ICE XL's COM bridge occasionally throws on cold-start (publisher waking,
# auth handshake, DCOM reconnect). These bubble up as OSError / RuntimeError
# before the per-symbol retry loop in ice_ticker_data_utils can catch them,
# so we wrap the whole orchestration `main()` with a narrow outer retry.
#
# pywintypes is only present on Windows + pywin32; we probe for it so the
# module stays importable on CI / Linux without the ICE XL dependency.
_ICE_TRANSIENT: tuple[type[BaseException], ...] = (
    OSError,
    RuntimeError,
    ConnectionError,
    TimeoutError,
)
try:
    import pywintypes

    _ICE_TRANSIENT = _ICE_TRANSIENT + (pywintypes.com_error,)
except ImportError:
    pass


def ice_transient_retry_policy(attempts: int = 2):
    """Short retry around ICE XL cold-start / COM-layer flakes.

    Narrow by design: per-symbol retries already live inside
    `ice_ticker_data_utils.get_timesales_batch`. This outer layer only
    exists to survive ICE XL publisher hiccups that raise before the
    batch loop gets a chance to retry.
    """
    return retry(
        stop=stop_after_attempt(attempts),
        wait=wait_exponential_jitter(initial=10, max=120),
        retry=retry_if_exception_type(_ICE_TRANSIENT),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
