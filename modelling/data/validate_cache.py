"""Validate the modelling cache before a forecast run.

Reads each expected parquet in ``modelling/data/cache/`` directly and peeks the
latest row to build two summary tables:

  - Actuals  — historical sources; status is `stale (>Nd)` if the latest row
    lags ``today`` by more than the per-source ``max_lag_days``.
  - Forecasts — forward-looking sources; status is `stale (< target)` if the
    latest row does not reach the forecast ``target_date``.

Exit code is 0 when every required source is ``ok`` and 1 when any required
source is missing / empty / error / stale. Optional-source failures surface
as warnings and do not fail the run.

Usage:
    python modelling/data/validate_cache.py                       # target = tomorrow
    python modelling/data/validate_cache.py --target-date 2026-04-27
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

# Ensure modelling/ is importable regardless of CWD.
_MODELLING_ROOT = Path(__file__).resolve().parent.parent
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from utils import logging_utils  # noqa: E402

# Column candidates used to find the "latest row" date for a parquet.
_DATE_CANDIDATES = ("date", "gas_day", "forecast_date", "trade_date")

# Actuals — (source name, max_lag_days). ``max_lag_days=None`` disables lag check.
_ACTUAL_SOURCES: tuple[tuple[str, int | None, bool], ...] = (
    # (source,                                      max_lag_days, required)
    ("pjm_lmps_hourly",                             1,            True),
    ("pjm_load_rt_hourly",                          1,            False),
    ("pjm_fuel_mix_hourly",                         1,            False),
    ("pjm_tie_flows_hourly",                        1,            False),
    ("pjm_outages_actual_daily",                    2,            False),
    ("wsi_pjm_hourly_observed_temp",                1,            False),
    ("ice_python_next_day_gas_hourly",              1,            False),
)

# Forecasts — these must reach ``target_date``.
_FORECAST_SOURCES: tuple[tuple[str, bool], ...] = (
    # (source,                                              required)
    ("pjm_outages_forecast_history",                        False),
    ("wsi_pjm_hourly_forecast_temp_latest",                 False),
    ("pjm_load_forecast_hourly_da_cutoff",                  False),
    ("pjm_solar_forecast_hourly_da_cutoff",                 False),
    ("pjm_wind_forecast_hourly_da_cutoff",                  False),
    ("pjm_net_load_forecast_hourly_da_cutoff",              False),
    ("meteologica_pjm_load_forecast_hourly_da_cutoff",      False),
    ("meteologica_pjm_net_load_forecast_hourly_da_cutoff",  False),
    ("meteologica_pjm_solar_forecast_hourly_da_cutoff",     False),
    ("meteologica_pjm_wind_forecast_hourly_da_cutoff",      False),
)


@dataclass
class SourceInfo:
    source: str
    required: bool
    status: str = "ok"              # ok | missing | empty | error | stale (>Nd) | stale (< target)
    rows: int = 0
    date: date | None = None
    hour_ending: int | None = None
    datetime: pd.Timestamp | None = None
    error: str | None = None
    lag_days: int | None = None
    lead_days: int | None = None
    max_lag: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def _peek_source_latest(source: str, *, required: bool, cache_dir: Path) -> SourceInfo:
    """Return status + latest (datetime, date, hour_ending, rows) for a parquet."""
    info = SourceInfo(source=source, required=required)
    path = cache_dir / f"{source}.parquet"

    if not path.exists():
        info.status = "missing"
        return info

    try:
        df = pd.read_parquet(path)
    except Exception as exc:  # noqa: BLE001 — surface any parquet read error
        info.status = "error"
        info.error = str(exc)
        return info

    info.rows = len(df)
    if len(df) == 0:
        info.status = "empty"
        return info

    date_col = next((c for c in _DATE_CANDIDATES if c in df.columns), None)
    if date_col is None:
        info.status = "error"
        info.error = "no date-like column"
        return info

    dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
    if dates.empty:
        info.status = "empty"
        return info
    latest_date = dates.max().date()
    info.date = latest_date

    if "hour_ending" in df.columns:
        mask = pd.to_datetime(df[date_col], errors="coerce").dt.date == latest_date
        hes = pd.to_numeric(df.loc[mask, "hour_ending"], errors="coerce").dropna()
        if not hes.empty:
            he = int(hes.max())
            info.hour_ending = he
            if he == 24:
                info.datetime = pd.Timestamp(latest_date) + pd.Timedelta(days=1)
            else:
                info.datetime = pd.Timestamp(latest_date) + pd.Timedelta(hours=he)

    if info.datetime is None:
        info.datetime = pd.Timestamp(latest_date)

    return info


def _scan_actuals(today: date, cache_dir: Path) -> list[SourceInfo]:
    rows: list[SourceInfo] = []
    for source, max_lag, required in _ACTUAL_SOURCES:
        r = _peek_source_latest(source, required=required, cache_dir=cache_dir)
        r.max_lag = max_lag
        if r.status == "ok" and r.date is not None:
            r.lag_days = (today - r.date).days
            if max_lag is not None and r.lag_days > max_lag:
                r.status = f"stale (>{max_lag}d)"
        rows.append(r)
    return rows


def _scan_forecasts(target_date: date, cache_dir: Path) -> list[SourceInfo]:
    rows: list[SourceInfo] = []
    for source, required in _FORECAST_SOURCES:
        r = _peek_source_latest(source, required=required, cache_dir=cache_dir)
        if r.status == "ok" and r.date is not None:
            r.lead_days = (r.date - target_date).days
            if r.lead_days < 0:
                r.status = "stale (< target)"
        rows.append(r)
    return rows


def _render_table(
    log,
    title: str,
    rows: list[SourceInfo],
    metric_attr: str,
    metric_label: str,
) -> None:
    """Render one summary block with the given metric column."""
    name_w = max((len(r.source) for r in rows), default=10)
    req_w = 8
    dt_w = 19
    date_w = 10
    he_w = 3
    metric_w = 5
    rows_w = 9

    header = (
        f"  {'source':<{name_w}}  {'req':<{req_w}}  "
        f"{'datetime':<{dt_w}}  {'date':<{date_w}}  "
        f"{'HE':>{he_w}}  {metric_label:>{metric_w}}  {'rows':>{rows_w}}  status"
    )

    log.section(title)
    log.info(header)
    log.info("  " + "-" * (len(header) - 2))

    for r in rows:
        req_str = "required" if r.required else "optional"
        dt_str = r.datetime.strftime("%Y-%m-%d %H:%M:%S") if r.datetime is not None else "-"
        date_str = r.date.isoformat() if r.date is not None else "-"
        he_str = str(r.hour_ending) if r.hour_ending is not None else "-"
        metric_val = getattr(r, metric_attr)
        metric_str = f"{metric_val:+d}" if isinstance(metric_val, int) else "-"
        rows_str = f"{r.rows:,}" if r.rows else "-"
        status = r.status if r.error is None else f"{r.status}: {r.error}"

        line = (
            f"  {r.source:<{name_w}}  {req_str:<{req_w}}  "
            f"{dt_str:<{dt_w}}  {date_str:<{date_w}}  "
            f"{he_str:>{he_w}}  {metric_str:>{metric_w}}  {rows_str:>{rows_w}}  {status}"
        )
        if r.status == "ok":
            log.info(line)
        elif r.status in ("missing", "empty") or r.status.startswith("stale"):
            # Errors only when the problem is on a required source.
            (log.error if r.required else log.warning)(line)
        else:
            log.error(line)


def _exit_code(actuals: list[SourceInfo], forecasts: list[SourceInfo]) -> int:
    """Non-zero when any required source has a non-ok status."""
    for r in actuals + forecasts:
        if r.required and r.status != "ok":
            return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the modelling cache for a forecast run",
    )
    parser.add_argument("--target-date", help="Forecast target (YYYY-MM-DD). Defaults to tomorrow.")
    parser.add_argument(
        "--cache-dir",
        help="Cache directory to validate. Defaults to da_models configured CACHE_DIR.",
    )
    args = parser.parse_args()

    log = logging_utils.init_logging(
        name="validate_cache",
        log_dir=_MODELLING_ROOT / "logs",
    )

    today = date.today()
    target_date = date.fromisoformat(args.target_date) if args.target_date else today + timedelta(days=1)

    if args.cache_dir:
        cache_dir = Path(args.cache_dir).expanduser()
    else:
        from da_models.common.configs import CACHE_DIR
        cache_dir = CACHE_DIR

    log.section(f"Cache Validation - target {target_date}")
    log.info(f"  today      {today}")
    log.info(f"  target     {target_date}")
    log.info(f"  cache_dir  {cache_dir}")

    actuals = _scan_actuals(today=today, cache_dir=cache_dir)
    forecasts = _scan_forecasts(target_date=target_date, cache_dir=cache_dir)

    _render_table(log, "Actuals (lag vs today)", actuals, "lag_days", "lag")
    _render_table(log, f"Forecasts (lead vs target {target_date})", forecasts, "lead_days", "lead")

    # Summary
    all_rows = actuals + forecasts
    n_ok = sum(1 for r in all_rows if r.status == "ok")
    n_bad_req = sum(1 for r in all_rows if r.required and r.status != "ok")
    n_bad_opt = sum(1 for r in all_rows if not r.required and r.status != "ok")

    log.section("Summary")
    log.info(f"  ok                 {n_ok}/{len(all_rows)}")
    log.info(f"  required failed    {n_bad_req}")
    log.info(f"  optional degraded  {n_bad_opt}")

    if n_bad_req:
        log.error("VALIDATION FAILED")
        return 1
    log.success("VALIDATION PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
