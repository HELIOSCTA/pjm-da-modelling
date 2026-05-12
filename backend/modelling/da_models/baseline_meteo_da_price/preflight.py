"""Data-validation preflight for the ``baseline_meteo_da_price`` forecast.

Run this BEFORE the forecast pipeline. It loads exactly the inputs
``pipelines/forecast_single_day.py`` consumes — the Meteologica Western-Hub
DA-price forecast for the target date, and (when it exists) the settled DA LMP
used for the Actual / Error rows — asserts a battery of validity rules, prints a
per-check report, and raises :class:`DataValidationError` if any ERROR-severity
check failed. It writes nothing and never touches the forecast pipeline.

Usage::

    python -m backend.modelling.da_models.baseline_meteo_da_price.preflight
    python modelling/da_models/baseline_meteo_da_price/preflight.py

Exit code is 0 when inputs are healthy, non-zero (DataValidationError) otherwise.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.modelling.da_models.common.validation import (  # noqa: E402
    ValidationReport,
    check_freshness,
    check_frame_non_empty,
    check_lead_days,
    check_no_all_nan,
    check_no_duplicate_keys,
    check_row_count_per_day,
    check_target_date_present,
    check_value_range,
    print_report,
    run_checks,
)
from backend.modelling.da_models.common.validation.checks import (  # noqa: E402
    DA_LMP_MAX_USD,
    DA_LMP_MIN_USD,
    CheckResult,
    CheckStatus,
)
from backend.utils.logging_utils import init_logging, print_header  # noqa: E402

# ── Defaults (mirror pipelines/forecast_single_day.py) ─────────────────────
TARGET_DATE: date | None = None  # None -> tomorrow
HUB: str = "WESTERN HUB"
LEAD_DAYS: int | None = 1  # DA-cutoff vintage
CACHE_DIR: Path | None = None
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"

_PRICE_SERIES: tuple[str, ...] = (
    "da_price_deterministic",
    "da_price_ens_average",
    "da_price_ens_bottom",
    "da_price_ens_top",
)


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def validate(
    target_date: date | None = TARGET_DATE,
    *,
    hub: str = HUB,
    lead_days: int | None = LEAD_DAYS,
    cache_dir: Path | None = CACHE_DIR,
) -> ValidationReport:
    """Load the baseline's inputs and run every check; return the report.

    Does NOT raise on failure — call ``report.raise_if_failed()`` for that.
    """
    resolved_date = _resolve_target_date(target_date)

    meteo = loader.load_meteologica_da_price_forecast(
        cache_dir=cache_dir, lead_days=lead_days
    )
    lmps = loader.load_lmps_da(cache_dir=cache_dir)
    lmps_at_hub = lmps[lmps["region"].astype(str) == hub] if not lmps.empty else lmps

    specs = [
        lambda: check_frame_non_empty("meteo_da_price: frame non-empty", meteo),
        lambda: check_target_date_present(
            "meteo_da_price: target date present", meteo, resolved_date
        ),
        lambda: check_row_count_per_day(
            "meteo_da_price: 24 hours on target date", meteo, resolved_date
        ),
        lambda: check_no_duplicate_keys(
            "meteo_da_price: unique (date, hour_ending)", meteo, ["date", "hour_ending"]
        ),
        lambda: check_lead_days(
            "meteo_da_price: DA-cutoff vintage",
            meteo,
            resolved_date,
            lead_days=lead_days if lead_days is not None else 1,
        ),
        lambda: check_no_all_nan(
            "meteo_da_price: price series not all-NaN",
            meteo,
            _PRICE_SERIES,
            target_date=resolved_date,
        ),
        lambda: check_value_range(
            "meteo_da_price: deterministic in sane $/MWh range",
            meteo,
            "da_price_deterministic",
            low=DA_LMP_MIN_USD,
            high=DA_LMP_MAX_USD,
            target_date=resolved_date,
        ),
        lambda: check_value_range(
            "meteo_da_price: ENS average in sane $/MWh range",
            meteo,
            "da_price_ens_average",
            low=DA_LMP_MIN_USD,
            high=DA_LMP_MAX_USD,
            target_date=resolved_date,
        ),
        lambda: check_freshness(
            "meteo_da_price: vintage freshness",
            meteo,
            reference_date=resolved_date,
            # as_of_date == date - lead_days, so the "newest" as_of is roughly
            # the target date minus lead; allow lead + slack before warning.
            max_age_days=(lead_days or 1) + 3,
        ),
        # Settled DA LMP only exists once the market clears (the typical
        # tomorrow case has none) -> a missing/empty hub frame is a WARN here,
        # not an ERROR, because the forecast still runs without it.
        lambda: (
            check_value_range(
                "settled DA LMP at hub: sane $/MWh range when present",
                lmps_at_hub,
                "lmp",
                low=DA_LMP_MIN_USD,
                high=DA_LMP_MAX_USD,
            )
            if not lmps_at_hub.empty
            else _settled_lmp_absent_warn(hub)
        ),
    ]
    return run_checks(specs)


def _settled_lmp_absent_warn(hub: str) -> CheckResult:
    return CheckResult(
        name="settled DA LMP at hub: sane $/MWh range when present",
        status=CheckStatus.WARN,
        detail=f"no settled DA LMP rows for hub {hub} yet (expected for a future target date)",
    )


def run(
    target_date: date | None = TARGET_DATE,
    *,
    hub: str = HUB,
    lead_days: int | None = LEAD_DAYS,
    cache_dir: Path | None = CACHE_DIR,
    quiet: bool = False,
) -> ValidationReport:
    """Preflight entrypoint: validate, print the report, raise if it failed."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="preflight_baseline_meteo_da_price", log_dir=LOG_DIR)
    try:
        resolved_date = _resolve_target_date(target_date)
        if not quiet:
            print_header(
                f"PREFLIGHT - baseline_meteo_da_price | {hub} | target {resolved_date}",
                "=",
                100,
            )
        report = validate(
            target_date=target_date, hub=hub, lead_days=lead_days, cache_dir=cache_dir
        )
        if not quiet:
            print_report(report, logger=pl)
        report.raise_if_failed()
        return report
    finally:
        pl.close()


if __name__ == "__main__":
    run()
