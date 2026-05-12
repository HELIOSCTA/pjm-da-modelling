"""Exception raised when a preflight finds bad input data."""

from __future__ import annotations


class DataValidationError(RuntimeError):
    """A forecast input failed one or more validation checks.

    Raised by :meth:`ValidationReport.raise_if_failed`. The message lists
    every failed (ERROR-severity) check so a CLI caller's non-zero exit or a
    Prefect task failure carries the full picture, not just the first problem.
    """
