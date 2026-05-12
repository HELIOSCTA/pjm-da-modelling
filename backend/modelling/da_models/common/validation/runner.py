"""Run a list of checks, collect every result, and abort once if any failed.

The runner is fail-slow on purpose: it executes *all* check thunks before
deciding whether to raise, so one preflight run surfaces every problem instead
of fix-one / rerun / discover-the-next. A thunk that itself blows up (wrong
column, etc.) becomes an ERROR result rather than crashing the run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Sequence

from backend.modelling.da_models.common.validation.checks import (
    CheckResult,
    CheckStatus,
)
from backend.modelling.da_models.common.validation.errors import DataValidationError

# A check spec is a zero-arg thunk: the preflight binds the loaded frame and
# parameters at construction time, e.g. ``lambda: check_target_date_present(...)``.
CheckThunk = Callable[[], CheckResult]


@dataclass
class ValidationReport:
    """All check results from one preflight run, plus the abort decision."""

    results: list[CheckResult] = field(default_factory=list)

    @property
    def errors(self) -> list[CheckResult]:
        return [r for r in self.results if r.status is CheckStatus.ERROR]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.status is CheckStatus.WARN]

    @property
    def passed(self) -> list[CheckResult]:
        return [r for r in self.results if r.status is CheckStatus.PASS]

    @property
    def ok(self) -> bool:
        """True when nothing reached ERROR severity."""
        return not self.errors

    def summary_line(self) -> str:
        return (
            f"{len(self.passed)} passed, {len(self.warnings)} warning(s), "
            f"{len(self.errors)} error(s)"
        )

    def raise_if_failed(self) -> None:
        """Raise one :class:`DataValidationError` listing every failed check."""
        if not self.errors:
            return
        lines = [f"  [{r.status.value}] {r.name}: {r.detail}" for r in self.errors]
        raise DataValidationError(
            f"{len(self.errors)} data-validation check(s) failed:\n" + "\n".join(lines)
        )


def run_checks(specs: Sequence[CheckThunk]) -> ValidationReport:
    """Execute every check thunk, capturing exceptions as ERROR results."""
    results: list[CheckResult] = []
    for spec in specs:
        try:
            results.append(spec())
        except Exception as exc:  # noqa: BLE001 - any check failure must not crash the run
            name = getattr(spec, "__name__", repr(spec))
            results.append(
                CheckResult(
                    name=name,
                    status=CheckStatus.ERROR,
                    detail=f"check raised {type(exc).__name__}: {exc}",
                )
            )
    return ValidationReport(results=results)


def print_report(report: ValidationReport, *, logger=None) -> None:
    """Print each result line, then the summary. Uses ``logger`` if given.

    ``logger`` is the object returned by ``backend.utils.logging_utils.init_logging``
    (it has ``.success`` / ``.warning`` / ``.error`` / ``.info``); when ``None``
    the lines go to ``print`` so the function works outside a logging context.
    """

    def emit(status: CheckStatus, text: str) -> None:
        if logger is None:
            print(f"[{status.value}] {text}")
            return
        if status is CheckStatus.PASS:
            logger.success(f"[PASS] {text}")
        elif status is CheckStatus.WARN:
            logger.warning(f"[WARN] {text}")
        else:
            logger.error(f"[ERROR] {text}")

    for r in report.results:
        emit(r.status, f"{r.name}: {r.detail}")

    if logger is None:
        print(report.summary_line())
    elif report.ok:
        logger.success(report.summary_line())
    else:
        logger.error(report.summary_line())
