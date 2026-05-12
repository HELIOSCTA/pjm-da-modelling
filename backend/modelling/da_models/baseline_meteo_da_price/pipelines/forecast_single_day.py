"""Single-day Meteologica DA-price baseline — terminal output.

Loads ``load_meteologica_da_price_forecast`` for the target date,
fetches settled DA LMP at the hub if available, and prints two
tables in the ``forecast_single_day``-style layout:

  1. **Det + ENS Summary** — Actual? / Det / ENS Avg / ENS Bottom /
     ENS Top, plus per-series Error rows when actuals exist. Each
     named series has its own consistent color across both tables.
  2. **ENS Members** — ENS Bottom row, the 51 ECMWF members ranked by
     OnPeak (HE8-23) ascending, then ENS Top row. The 51 in-between
     members are dim so the eye lands on the colored floor / ceiling.

When settled DA LMP doesn't yet exist for the target date (the typical
case for tomorrow), only forecast rows are printed; Actual / Error rows
auto-appear once the DA market has cleared.

Tunable defaults live in module-level constants — edit directly or
override via ``run(...)`` from a notebook.

Usage::

    python -m backend.modelling.da_models.baseline_meteo_da_price.pipelines.forecast_single_day
    python modelling/da_models/baseline_meteo_da_price/pipelines/forecast_single_day.py
"""

from __future__ import annotations

import sys
import uuid
from datetime import date, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[5]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.modelling.da_models.baseline_meteo_da_price.printers import (  # noqa: E402
    build_bands_table,
    build_bands_vs_actuals,
    build_forecast_vs_actuals,
    build_members_table,
    compute_dispersion_metrics,
    print_bands_section,
    print_bands_vs_actuals_section,
    print_config,
    print_forecast_vs_actuals_section,
)
from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.modelling.da_models.common.forecast.output import actuals_from_pool  # noqa: E402
from backend.modelling.da_models.common.publish import publish_forecast_run  # noqa: E402
from backend.utils.logging_utils import (  # noqa: E402
    init_logging,
    print_divider,
    print_header,
)

# ── Defaults (edit here instead of using CLI flags) ────────────────────────
TARGET_DATE: date | None = None  # None -> tomorrow
# Forecast vintage -- the date the run is produced (None -> date.today()).
RUN_DATE: date | None = None
HUB: str = "WESTERN HUB"
LEAD_DAYS: int | None = 1  # DA-cutoff vintage; None for all vintages
CACHE_DIR: Path | None = None
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"
# Frontend ingestion identity for pjm_model_outputs.forecast_runs. Distinct
# from the ICE-anchored sibling so the two coexist in the picker; the payload
# carries an ICE-anchor block with applied=False (no anchor for this variant).
PUBLISHED_MODEL_NAME: str = "baseline_meteo_da_price"
PUBLISHED_MODEL_FAMILY: str = "baseline"
# The pipeline always publishes the run to pjm_model_outputs.forecast_runs
# (one row, upserted via backend.modelling.da_models.common.publish.publish_forecast_run) so the
# frontend can read it. Batch/backtest callers that must NOT write a row per
# date pass publish=False to run().
PUBLISH: bool = True


def _resolve_target_date(target_date: date | None) -> date:
    return target_date if target_date is not None else date.today() + timedelta(days=1)


def _first_or_none(s: pd.Series) -> pd.Timestamp | None:
    s = s.dropna()
    return None if s.empty else pd.Timestamp(s.iloc[0])


def run(
    target_date: date | None = TARGET_DATE,
    run_date: date | None = RUN_DATE,
    hub: str = HUB,
    lead_days: int | None = LEAD_DAYS,
    cache_dir: Path | None = CACHE_DIR,
    publish: bool = PUBLISH,
    quiet: bool = False,
) -> dict:
    """Run the baseline and print the two-table report.

    Returns a dict with: ``forecast_date``, ``hub``, ``summary_table``,
    ``members_table``, ``has_actuals``, ``det_forecast_executed``,
    ``ens_forecast_executed``, ``df_forecast``, ``run_id``. ``quiet``
    suppresses all printing while keeping the return dict populated
    (the harness contract from the python-scripts skill).
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="baseline_meteo_da_price", log_dir=LOG_DIR)
    try:
        resolved_date = _resolve_target_date(target_date)
        resolved_run_date = run_date if run_date is not None else date.today()
        run_id = str(uuid.uuid4())

        with pl.timer("load Meteologica DA-price forecast"):
            full = loader.load_meteologica_da_price_forecast(
                cache_dir=cache_dir, lead_days=lead_days
            )
        df_forecast = full[full["date"] == resolved_date].copy()

        det_exec = (
            _first_or_none(df_forecast["det_forecast_execution_datetime_local"])
            if "det_forecast_execution_datetime_local" in df_forecast.columns
            else None
        )
        ens_exec = (
            _first_or_none(df_forecast["ens_forecast_execution_datetime_local"])
            if "ens_forecast_execution_datetime_local" in df_forecast.columns
            else None
        )

        actuals_hourly: dict[int, float] | None = None
        if not df_forecast.empty:
            with pl.timer(f"load settled DA LMP at {hub}"):
                lmps = loader.load_lmps_da(cache_dir=cache_dir)
            lmps_at_hub = lmps[lmps["region"].astype(str) == hub]
            actuals_hourly = actuals_from_pool(lmps_at_hub, resolved_date)

        bands_table = build_bands_table(resolved_date, df_forecast)
        forecast_vs_actuals = build_forecast_vs_actuals(
            resolved_date, df_forecast, actuals_hourly
        )
        bands_vs_actuals = build_bands_vs_actuals(
            resolved_date, df_forecast, actuals_hourly
        )
        members_table = build_members_table(resolved_date, df_forecast)
        dispersion = (
            compute_dispersion_metrics(df_forecast) if not df_forecast.empty else None
        )

        if publish and not df_forecast.empty:
            from backend.modelling.da_models.baseline_meteo_da_price.publish import (  # noqa: PLC0415
                build_payload,
                extract_onpeak_forecast,
            )

            # Unanchored variant: no ICE anchor, so build_payload gets the raw
            # bands for both scaled/raw slots, no trades, no VWAP -- it emits a
            # valid IcePayload with ice_anchor.applied=False.
            payload = build_payload(
                df_for_fan=df_forecast,
                bands_table_scaled=bands_table,
                bands_table_raw=bands_table,
                actuals_hourly=actuals_hourly,
                trades=pd.DataFrame(),
                vwap_result=None,
                target_date=resolved_date,
                run_date=resolved_run_date,
                model_name=PUBLISHED_MODEL_NAME,
                model_family=PUBLISHED_MODEL_FAMILY,
                run_id=run_id,
                hub=hub,
                lead_days=lead_days,
                det_exec=det_exec,
                ens_exec=ens_exec,
                ice_symbol="",
                ice_cutoff=None,
                shared_scale=None,
                anchor_label=None,
                implied_multipliers=None,
            )
            publish_forecast_run(
                model_name=PUBLISHED_MODEL_NAME,
                model_family=PUBLISHED_MODEL_FAMILY,
                target_date=resolved_date,
                run_date=resolved_run_date,
                run_id=run_id,
                payload=payload,
                da_lmp_total_onpeak_forecast=extract_onpeak_forecast(payload),
            )

        if not quiet:
            print_header(
                f"BASELINE METEO DA-PRICE — {hub} ($/MWh)  |  {resolved_date}",
                "=",
                120,
            )
            print_config(resolved_date, hub, lead_days, det_exec, ens_exec)

            if df_forecast.empty:
                pl.warning(
                    f"No Meteologica DA-price forecast for {resolved_date} "
                    f"(lead_days={lead_days}). Tables are empty."
                )
            else:
                pl.info(
                    f"forecast rows: {len(df_forecast)} | "
                    f"actuals: {'yes' if actuals_hourly else 'no'}"
                )

            # 1) ENS Bands ($/MWh) — bands + dispersion footer.
            print_bands_section(resolved_date, bands_table, dispersion)

            # 2) Forecast vs Actuals — only when settled DA LMP exists.
            if actuals_hourly is not None:
                print_forecast_vs_actuals_section(resolved_date, forecast_vs_actuals)

                # 3) ENS Bands vs Actuals — only when actuals exist.
                print_bands_vs_actuals_section(resolved_date, bands_vs_actuals)

            # NOTE: members fan is built and returned in the dict for
            # programmatic / notebook callers, but not printed by default.
            print()
            print_divider("=", 120, dim=False)
            print()

        return {
            "forecast_date": str(resolved_date),
            "run_date": str(resolved_run_date),
            "hub": hub,
            "bands_table": bands_table,
            "forecast_vs_actuals": forecast_vs_actuals,
            "bands_vs_actuals": bands_vs_actuals,
            "members_table": members_table,
            "dispersion_metrics": dispersion,
            "has_actuals": actuals_hourly is not None,
            "det_forecast_executed": det_exec,
            "ens_forecast_executed": ens_exec,
            "df_forecast": df_forecast,
            "run_id": run_id,
        }
    finally:
        pl.close()


if __name__ == "__main__":
    run()
