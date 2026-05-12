"""Next-N-days Meteologica DA-price baseline -- horizon summary + multi-day upsert.

Loads the most-recent Meteologica DA-price vintage (which covers ~7-14 future
delivery days), computes the OnPeak HE8-23 forecast for each forward delivery
date, prints a one-row-per-day summary, and -- by default -- upserts one row
per delivery date into ``pjm_model_outputs.forecast_runs``: all rows share one
``run_id``, with ``run_date`` = the vintage and ``target_date`` = each delivery
date, so each payload's ``lead_days`` = ``target_date - run_date`` (1..N).

Sibling of ``forecast_single_day.py`` (which publishes just tomorrow / lead 1
and keeps the rich per-hour detail tables); both publish
``model_name = baseline_meteo_da_price``. The lead-1 row therefore gets written
by both pipelines with distinct ``run_id`` values -- "latest by created_at"
dedupes for readers and the content is identical (same Meteo vintage).

Each published row carries an ``IcePayload`` with ``ice_anchor.applied = false``
and ``ice_trades = []`` -- no ICE anchor for the multi-day horizon (the ICE
PDA D1-IUS product only covers the next day; anchoring is the single-day
pipeline's concern). Per-hour ``actual_lmp`` is null: this is a forward-forecast
pipeline (a backfill mode that fills actuals for past target dates is a follow-up).

Usage::

    python -m backend.modelling.da_models.baseline_meteo_da_price.pipelines.forecast_next_7_days
    python modelling/da_models/baseline_meteo_da_price/pipelines/forecast_next_7_days.py
"""

from __future__ import annotations

import sys
import uuid
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[5]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402

from backend.modelling.da_models.baseline_meteo_da_price.printers import build_bands_table  # noqa: E402
from backend.modelling.da_models.common.data import loader  # noqa: E402
from backend.modelling.da_models.common.publish import publish_forecast_run  # noqa: E402
from backend.utils.logging_utils import init_logging, print_divider, print_header  # noqa: E402

# ── Defaults (edit here instead of using CLI flags) ────────────────────────
# Forecast vintage -- the date the run is produced (None -> date.today()).
RUN_DATE: date | None = None
# How many forward delivery days to publish (None -> the whole latest vintage).
HORIZON_DAYS: int | None = 7
HUB: str = "WESTERN HUB"
CACHE_DIR: Path | None = None
LOG_DIR: Path = _REPO_ROOT / "backend" / "modelling" / "logs"
# Frontend ingestion identity for pjm_model_outputs.forecast_runs. Same model
# as forecast_single_day.py (which publishes lead 1 only); this pipeline
# publishes leads 1..HORIZON_DAYS, one run_id per batch.
PUBLISHED_MODEL_NAME: str = "baseline_meteo_da_price"
PUBLISHED_MODEL_FAMILY: str = "baseline"
# The pipeline always publishes (one row per forward delivery date, all sharing
# one run_id) so the frontend can read it. Batch/backtest callers pass
# publish=False to run() to skip the writes.
PUBLISH: bool = True


def _first_or_none(s: pd.Series) -> pd.Timestamp | None:
    s = s.dropna()
    return None if s.empty else pd.Timestamp(s.iloc[0])


def _onpeak(bands: pd.DataFrame, series: str) -> float | None:
    """OnPeak HE8-23 value for a named series in a build_bands_table frame."""
    if bands is None or bands.empty or "Type" not in bands.columns:
        return None
    rows = bands[bands["Type"] == series]
    if rows.empty:
        return None
    v = rows.iloc[0].get("OnPeak")
    return None if pd.isna(v) else float(v)


def run(
    run_date: date | None = RUN_DATE,
    horizon_days: int | None = HORIZON_DAYS,
    hub: str = HUB,
    cache_dir: Path | None = CACHE_DIR,
    publish: bool = PUBLISH,
    quiet: bool = False,
) -> dict:
    """Run the next-N-days Meteologica DA-price baseline.

    Loads the latest Meteologica DA-price vintage, computes the OnPeak
    HE8-23 forecast for each forward delivery date (capped at
    ``horizon_days``; ``None`` -> the whole vintage), and -- when
    ``publish`` -- upserts one ``forecast_runs`` row per delivery date,
    all sharing one ``run_id`` (``run_date`` = the vintage,
    ``target_date`` = each delivery date, ``lead_days`` = the gap).

    Returns a dict: ``run_date``, ``run_id``, ``model_name``,
    ``horizon_days_used``, ``target_dates`` (ISO date strings), ``rows``
    (one dict per delivery date: ``target_date``, ``lead``,
    ``onpeak_det``, ``onpeak_ens_avg``, ``onpeak_ens_bottom``,
    ``onpeak_ens_top``, ``det_executed_local``, ``ens_executed_local``,
    ``bands_table``), ``summary_table`` (DataFrame), ``published`` (count
    of rows upserted). ``quiet`` suppresses printing while keeping the
    return dict populated (the python-scripts harness contract).
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="baseline_meteo_da_price_next_7", log_dir=LOG_DIR)
    try:
        resolved_run_date = run_date if run_date is not None else date.today()
        run_id = str(uuid.uuid4())

        with pl.timer("load Meteologica DA-price forecast (latest vintage)"):
            full = loader.load_meteologica_da_price_forecast(
                cache_dir=cache_dir, latest_only=True
            )

        full_dates = (
            pd.to_datetime(full["date"], errors="coerce").dt.date
            if not full.empty
            else pd.Series([], dtype=object)
        )
        all_dates = sorted({d for d in full_dates if d is not None and pd.notna(d)})
        target_dates = [d for d in all_dates if d > resolved_run_date]
        if horizon_days is not None:
            target_dates = target_dates[: int(horizon_days)]

        det_col = "det_forecast_execution_datetime_local"
        ens_col = "ens_forecast_execution_datetime_local"

        if publish and target_dates:
            from backend.modelling.da_models.baseline_meteo_da_price.publish import (  # noqa: PLC0415
                build_payload,
                extract_onpeak_forecast,
            )

        rows: list[dict] = []
        published = 0
        for delivery_date in target_dates:
            df_d = full[full_dates == delivery_date].copy()
            if df_d.empty:
                continue
            lead = (delivery_date - resolved_run_date).days
            det_exec_d = (
                _first_or_none(df_d[det_col]) if det_col in df_d.columns else None
            )
            ens_exec_d = (
                _first_or_none(df_d[ens_col]) if ens_col in df_d.columns else None
            )
            bands_d = build_bands_table(delivery_date, df_d)

            rows.append(
                {
                    "target_date": delivery_date.isoformat(),
                    "lead": lead,
                    "onpeak_det": _onpeak(bands_d, "Det"),
                    "onpeak_ens_avg": _onpeak(bands_d, "ENS Avg"),
                    "onpeak_ens_bottom": _onpeak(bands_d, "ENS Bottom"),
                    "onpeak_ens_top": _onpeak(bands_d, "ENS Top"),
                    "det_executed_local": (
                        det_exec_d.isoformat() if det_exec_d is not None else None
                    ),
                    "ens_executed_local": (
                        ens_exec_d.isoformat() if ens_exec_d is not None else None
                    ),
                    "bands_table": bands_d,
                }
            )

            if publish:
                payload = build_payload(
                    df_for_fan=df_d,
                    bands_table_scaled=bands_d,
                    bands_table_raw=bands_d,
                    actuals_hourly=None,
                    trades=pd.DataFrame(),
                    vwap_result=None,
                    target_date=delivery_date,
                    run_date=resolved_run_date,
                    model_name=PUBLISHED_MODEL_NAME,
                    model_family=PUBLISHED_MODEL_FAMILY,
                    run_id=run_id,
                    hub=hub,
                    lead_days=lead,
                    det_exec=det_exec_d,
                    ens_exec=ens_exec_d,
                    ice_symbol="",
                    ice_cutoff=None,
                    shared_scale=None,
                    anchor_label=None,
                    implied_multipliers=None,
                )
                publish_forecast_run(
                    model_name=PUBLISHED_MODEL_NAME,
                    model_family=PUBLISHED_MODEL_FAMILY,
                    target_date=delivery_date,
                    run_date=resolved_run_date,
                    run_id=run_id,
                    payload=payload,
                    da_lmp_total_onpeak_forecast=extract_onpeak_forecast(payload),
                )
                published += 1

        summary_table = pd.DataFrame(
            [
                {
                    "delivery_date": r["target_date"],
                    "lead": r["lead"],
                    "det_onpk": r["onpeak_det"],
                    "ens_avg_onpk": r["onpeak_ens_avg"],
                    "ens_bot_onpk": r["onpeak_ens_bottom"],
                    "ens_top_onpk": r["onpeak_ens_top"],
                }
                for r in rows
            ]
        )

        if not quiet:
            print_header(
                f"BASELINE METEO DA-PRICE -- NEXT {len(rows)} DAYS "
                f"-- {hub} ($/MWh)  |  run_date {resolved_run_date}",
                "=",
                100,
            )
            covers = f"{all_dates[0]}..{all_dates[-1]}" if all_dates else "(empty)"
            leads = f"{rows[0]['lead']}..{rows[-1]['lead']}" if rows else "-"
            pl.info(
                f"vintage covers {covers} | publishing {len(rows)} forward days "
                f"(leads {leads})"
                f"{' | publish=False (no DB writes)' if not publish else ''}"
            )
            if summary_table.empty:
                pl.warning(
                    f"No forward Meteologica delivery dates after {resolved_run_date} "
                    f"-- nothing to publish."
                )
            else:
                print(summary_table.to_string(index=False))
            print()
            print_divider("=", 100, dim=False)
            print()

        return {
            "run_date": str(resolved_run_date),
            "run_id": run_id,
            "model_name": PUBLISHED_MODEL_NAME,
            "horizon_days_used": len(rows),
            "target_dates": [r["target_date"] for r in rows],
            "rows": rows,
            "summary_table": summary_table,
            "published": published,
        }
    finally:
        pl.close()


if __name__ == "__main__":
    run()
