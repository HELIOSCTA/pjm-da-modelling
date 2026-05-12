"""Data-validation preflight for the ``like_day_model_knn_sunny`` forecast.

Run this BEFORE
``pjm_rto_hourly/pipelines/forecast_single_day.py``. It loads the model's core
inputs and asserts structural validity, prints a per-check report, and raises
:class:`DataValidationError` on any ERROR-severity failure. It writes nothing
and never touches the forecast pipeline.

Coverage today (the inputs whose absence/corruption breaks the run hardest):

  - DA LMP history (``load_lmps_da``) at the model hub — the label pool.
  - DA System Energy Price (``load_lmp_system_energy_da``) — the alt label source.
  - The PJM calendar (``load_pjm_dates_daily``) including the target-date row
    (there is a known leap-day gap in this parquet — see the project memory).
  - The hourly feature feeds the analog query row is built from: RT load,
    observed weather, solar / wind coalesced.

TODO: deeper per-domain coverage (DA-cutoff load forecast vintages, outage
forecast history, fuel mix, gas) is not yet asserted here — those flow through
``like_day_model_knn_sunny/domains.py`` and degrade gracefully (``_safe_load``
returns ``None``) rather than hard-failing, so they're lower priority. Add
column-level checks for them when a real bug warrants it.

Usage::

    python -m backend.modelling.da_models.like_day_model_knn_sunny.preflight
    python modelling/da_models/like_day_model_knn_sunny/preflight.py

Exit code is 0 when inputs are healthy, non-zero (DataValidationError) otherwise.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.modelling.da_models.common.validation import (  # noqa: E402
    ValidationReport,
    check_frame_non_empty,
    check_no_all_nan,
    check_no_duplicate_keys,
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
from backend.modelling.da_models.like_day_model_knn_sunny import configs  # noqa: E402
from backend.utils.logging_utils import init_logging, print_header  # noqa: E402

# ── Defaults (mirror pjm_rto_hourly/pipelines/forecast_single_day.py) ──────
TARGET_DATE: date | None = None  # None -> tomorrow
HUB: str = configs.HUB
CACHE_DIR: Path | None = None  # None -> configs.CACHE_DIR via the loader
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"

# How recent the latest date in an hourly feature feed must be for the
# target-date analog query row to be buildable. The query needs the day
# before the target (lag features), so the feed must reach target - 2 at worst.
_FEATURE_FEED_MAX_AGE_DAYS: int = 5


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _frame_reaches(
    name: str, df, target_date: date, *, date_col: str = "date"
) -> CheckResult:
    """ERROR if the frame's latest date is too far behind the target date."""
    if df is None or df.empty:
        return CheckResult(name, CheckStatus.ERROR, "frame is empty")
    if date_col not in df.columns:
        return CheckResult(
            name, CheckStatus.ERROR, f"no '{date_col}' column; has {list(df.columns)}"
        )
    latest = pd.to_datetime(df[date_col], errors="coerce").dt.date.dropna()
    if latest.empty:
        return CheckResult(name, CheckStatus.ERROR, f"'{date_col}' present but all NaT")
    newest = max(latest)
    cutoff = target_date - timedelta(days=_FEATURE_FEED_MAX_AGE_DAYS)
    if newest < cutoff:
        return CheckResult(
            name,
            CheckStatus.ERROR,
            f"latest date {newest} is older than target {target_date} - {_FEATURE_FEED_MAX_AGE_DAYS}d",
        )
    return CheckResult(name, CheckStatus.PASS, f"latest date {newest}")


def validate(
    target_date: date | None = TARGET_DATE,
    *,
    hub: str = HUB,
    cache_dir: Path | None = CACHE_DIR,
) -> ValidationReport:
    """Load the model's core inputs and run every check; return the report."""
    resolved_date = _resolve_target_date(target_date)

    lmps = loader.load_lmps_da(cache_dir=cache_dir)
    lmps_at_hub = lmps[lmps["region"].astype(str) == hub] if not lmps.empty else lmps
    sep = loader.load_lmp_system_energy_da(cache_dir=cache_dir)
    dates = loader.load_pjm_dates_daily(cache_dir=cache_dir)
    load_rt = loader.load_load_rt(cache_dir=cache_dir)
    weather = loader.load_weather_observed_hourly(cache_dir=cache_dir)
    solar = loader.load_solar_coalesced(cache_dir=cache_dir)
    wind = loader.load_wind_coalesced(cache_dir=cache_dir)

    specs = [
        # ── DA LMP label pool ────────────────────────────────────────────
        lambda: check_frame_non_empty("lmps_da: frame non-empty", lmps),
        lambda: check_frame_non_empty(f"lmps_da: rows at hub {hub}", lmps_at_hub),
        lambda: check_no_all_nan(
            "lmps_da: lmp not all-NaN at hub", lmps_at_hub, ["lmp"]
        ),
        lambda: check_value_range(
            "lmps_da: lmp in sane $/MWh range at hub",
            lmps_at_hub,
            "lmp",
            low=DA_LMP_MIN_USD,
            high=DA_LMP_MAX_USD,
        ),
        # Source parquet has known exact-duplicate days (DST fall-back HE2, the
        # odd full-day double-publish); the analog pool's pivot_table mean
        # absorbs them, so surface it as a WARN rather than aborting.
        lambda: check_no_duplicate_keys(
            "lmps_da: unique (region, date, hour_ending)",
            lmps,
            ["region", "date", "hour_ending"],
            severity=CheckStatus.WARN,
        ),
        # ── System Energy Price (alt label source) ───────────────────────
        lambda: check_frame_non_empty("lmp_system_energy_da: frame non-empty", sep),
        lambda: check_value_range(
            "lmp_system_energy_da: in sane $/MWh range",
            sep,
            "lmp_system_energy_price",
            low=DA_LMP_MIN_USD,
            high=DA_LMP_MAX_USD,
        ),
        # ── PJM calendar (needed for season window + day-type overrides) ──
        lambda: check_frame_non_empty("pjm_dates_daily: frame non-empty", dates),
        lambda: check_target_date_present(
            "pjm_dates_daily: target date row present", dates, resolved_date
        ),
        # ── Hourly feature feeds the analog query row is built from ───────
        lambda: _frame_reaches(
            "load_rt: reaches near target date", load_rt, resolved_date
        ),
        lambda: _frame_reaches(
            "weather_observed_hourly: reaches near target date", weather, resolved_date
        ),
        lambda: _frame_reaches(
            "solar_coalesced: reaches near target date", solar, resolved_date
        ),
        lambda: _frame_reaches(
            "wind_coalesced: reaches near target date", wind, resolved_date
        ),
    ]
    return run_checks(specs)


def run(
    target_date: date | None = TARGET_DATE,
    *,
    hub: str = HUB,
    cache_dir: Path | None = CACHE_DIR,
    quiet: bool = False,
) -> ValidationReport:
    """Preflight entrypoint: validate, print the report, raise if it failed."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="preflight_like_day_model_knn_sunny", log_dir=LOG_DIR)
    try:
        resolved_date = _resolve_target_date(target_date)
        if not quiet:
            print_header(
                f"PREFLIGHT - like_day_model_knn_sunny | {hub} | target {resolved_date}",
                "=",
                100,
            )
        report = validate(target_date=target_date, hub=hub, cache_dir=cache_dir)
        if not quiet:
            print_report(report, logger=pl)
        report.raise_if_failed()
        return report
    finally:
        pl.close()


if __name__ == "__main__":
    run()
