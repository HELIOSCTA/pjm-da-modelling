"""Standalone data-validation preflight primitives shared by all model families.

This package is deliberately decoupled from the forecast pipelines: a model's
``preflight.py`` loads the same inputs the forecast will see, runs a battery of
checks defined here, and raises :class:`DataValidationError` before the forecast
runs. Nothing in this package imports a model family — keep it family-agnostic.

See ``backend/modelling/README.md`` ("Preflight data validation") for the
contract and how to run a preflight.
"""

from backend.modelling.da_models.common.validation.checks import (
    CheckResult,
    CheckStatus,
    check_freshness,
    check_frame_non_empty,
    check_lead_days,
    check_no_all_nan,
    check_no_duplicate_keys,
    check_row_count_per_day,
    check_target_date_present,
    check_value_range,
)
from backend.modelling.da_models.common.validation.errors import DataValidationError
from backend.modelling.da_models.common.validation.runner import (
    ValidationReport,
    print_report,
    run_checks,
)

__all__ = [
    "CheckResult",
    "CheckStatus",
    "DataValidationError",
    "ValidationReport",
    "check_freshness",
    "check_frame_non_empty",
    "check_lead_days",
    "check_no_all_nan",
    "check_no_duplicate_keys",
    "check_row_count_per_day",
    "check_target_date_present",
    "check_value_range",
    "print_report",
    "run_checks",
]
