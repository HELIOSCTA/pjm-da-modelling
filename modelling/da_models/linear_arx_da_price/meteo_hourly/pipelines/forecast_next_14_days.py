"""Next-14-days ``meteo_hourly`` linear ARX DA-price forecast strip.

Forecasts delivery dates D+1 .. D+``HORIZON_DAYS`` for Western Hub in one
pass: trains the 24 per-hour ridge models once (lead-1 historical
features, as in the single-day pipeline), then predicts each forward day
from the best-available forward fundamentals. Prints a one-row-per-day
forward strip, then -- when ``PER_DAY_DETAIL`` -- the full per-HE
``Quantile Bands`` table for each forecast day (the same layout as the
single-day pipeline's banded section).

The "one-off horizon config" lives in the module-level constants below:

  - Demand (load/solar/wind/net-load): the *latest published* Meteologica
    regional vintage, which spans ~7-14 forward delivery days.
  - Weather: the latest WSI forecast horizon (``load_weather_coalesced``
    falls back to it for future dates); days past that horizon get a
    partial forecast (the missing-feature hours are skipped, flagged in
    the strip's ``n_he`` column).
  - Outages and ICE next-day gas: their feeds only reach ~D+1, so
    ``FORWARD_FILL_COLS`` carries the last known value forward onto the
    D+2 .. horizon-end rows (and the ``load_x_gas`` / ``outage_sq``
    interactions inherit the filled values).

Caveat (documented, not fixed here): training rows use the lead-1 forecast
vintage while the late-horizon target rows use the latest vintage / a
forward-filled constant -- a vintage asymmetry the design memo's Tier-2
list (``historical_forecasts.md`` vintage-matching) addresses. Treat the
late-lead numbers as indicative, not calibrated.

Research / standalone -- ``run(...)`` computes, prints, returns a dict;
nothing here writes Postgres.

Usage::

    python -m da_models.linear_arx_da_price.meteo_hourly.pipelines.forecast_next_14_days
    python modelling/da_models/linear_arx_da_price/meteo_hourly/pipelines/forecast_next_14_days.py
"""

from __future__ import annotations

import sys
import uuid
from datetime import date
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from da_models.linear_arx_da_price import configs as C  # noqa: E402
from da_models.linear_arx_da_price import printers  # noqa: E402
from da_models.linear_arx_da_price.forecast import (  # noqa: E402
    build_quantiles_table,
    forecast_target_date,
)
from da_models.linear_arx_da_price.meteo_hourly import config as V  # noqa: E402
from da_models.linear_arx_da_price.meteo_hourly.builder import (  # noqa: E402
    HORIZON_FORWARD_FILL_COLS,
    build_panel_horizon,
)
from da_models.linear_arx_da_price.trainer import train  # noqa: E402
from utils.logging_utils import (  # noqa: E402
    init_logging,
    print_divider,
    print_header,
    print_section,
)

# -- One-off horizon config (edit here instead of using CLI flags) ---------
RUN_DATE: date | None = (
    None  # forecast vintage; None -> today. Targets D+1..D+HORIZON_DAYS.
)
HORIZON_DAYS: int = 14
HUB: str = C.HUB
# Daily feeds that don't reach the horizon end -> carry the last known value forward.
FORWARD_FILL_COLS: tuple[str, ...] = HORIZON_FORWARD_FILL_COLS
# Print the full per-HE Quantile Bands table for each forecast day after the strip.
PER_DAY_DETAIL: bool = True
CACHE_DIR: Path | None = None
LOG_DIR: Path = _MODELLING_ROOT / "logs"

_DAY_ABBR = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_ONPEAK_HOURS = list(range(8, 24))  # HE8..HE23
_OFFPEAK_HOURS = list(range(1, 8)) + [24]
_Q_LO, _Q_HI = min(C.QUANTILES), max(C.QUANTILES)


def _block_mean(by_he: dict[int, float], hours: list[int]) -> float:
    vals = [by_he[h] for h in hours if h in by_he and pd.notna(by_he[h])]
    return float(np.mean(vals)) if vals else float("nan")


def run(
    run_date: date | None = RUN_DATE,
    horizon_days: int = HORIZON_DAYS,
    hub: str = HUB,
    forward_fill_cols: tuple[str, ...] = FORWARD_FILL_COLS,
    per_day_detail: bool = PER_DAY_DETAIL,
    cache_dir: Path | None = CACHE_DIR,
    quiet: bool = False,
) -> dict:
    """Run the next-N-days ``meteo_hourly`` forecast.

    Returns a dict: ``run_date``, ``horizon_days``, ``hub``,
    ``forward_filled_cols``, ``strip_table`` (one row per delivery date:
    ``target_date``, ``lead``, ``dow``, ``onpeak``, ``offpeak``, ``flat``,
    ``p10_onpeak``, ``p90_onpeak``, ``n_he``, ``features_complete``),
    ``forecasts_by_date`` ({date_iso: per-HE forecast frame}),
    ``bands_by_date`` ({date_iso: P10..P90 quantile-bands table}),
    ``skipped_hours``, ``backward_coef_share``, ``dropped_groups``,
    ``n_features``, ``run_id``. ``quiet`` suppresses printing;
    ``per_day_detail`` toggles the per-day Quantile Bands tables.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="linear_arx_meteo_hourly_next_14", log_dir=LOG_DIR)
    try:
        resolved_run_date = run_date if run_date is not None else date.today()
        run_id = str(uuid.uuid4())

        with pl.timer(f"build horizon feature panel (D+1..D+{horizon_days})"):
            built = build_panel_horizon(
                run_date=resolved_run_date,
                horizon_days=horizon_days,
                cache_dir=cache_dir,
                hub=hub,
                forward_fill_cols=tuple(forward_fill_cols),
            )
        panel = built["panel"]
        feature_cols = built["feature_cols"]
        target_dates = built["target_dates"]
        feat_ok = built["has_target_features_by_date"]

        with pl.timer("train 24 per-hour ridge models"):
            models = train(panel, feature_cols, target_dates[0])

        rows: list[dict] = []
        forecasts_by_date: dict[str, pd.DataFrame] = {}
        bands_by_date: dict[str, pd.DataFrame] = {}
        for d in target_dates:
            fc_d = forecast_target_date(models, panel, d)
            qt_d = build_quantiles_table(d, fc_d, C.DISPLAY_QUANTILES)
            forecasts_by_date[d.isoformat()] = fc_d
            bands_by_date[d.isoformat()] = qt_d
            he = fc_d["hour_ending"].astype(int)
            point = dict(zip(he, fc_d["point_forecast"].astype(float)))
            q_lo = dict(zip(he, fc_d[f"q_{_Q_LO:.2f}"].astype(float)))
            q_hi = dict(zip(he, fc_d[f"q_{_Q_HI:.2f}"].astype(float)))
            rows.append(
                {
                    "target_date": d.isoformat(),
                    "lead": (d - resolved_run_date).days,
                    "dow": _DAY_ABBR[d.weekday()],
                    "onpeak": _block_mean(point, _ONPEAK_HOURS),
                    "offpeak": _block_mean(point, _OFFPEAK_HOURS),
                    "flat": _block_mean(point, list(range(1, 25))),
                    "p10_onpeak": _block_mean(q_lo, _ONPEAK_HOURS),
                    "p90_onpeak": _block_mean(q_hi, _ONPEAK_HOURS),
                    "n_he": int(sum(1 for v in point.values() if pd.notna(v))),
                    "features_complete": bool(feat_ok.get(d, False)),
                }
            )
        strip_table = pd.DataFrame(rows)

        if not quiet:
            print_header(
                f"LINEAR ARX -- NEXT {len(rows)} DAYS -- {hub} ($/MWh)  |  "
                f"meteo_hourly  |  run_date {resolved_run_date}",
                "=",
                120,
            )
            print(f"  Model              {V.MODEL_NAME}")
            print(
                f"  Targets            {target_dates[0]} .. {target_dates[-1]}  (leads 1..{horizon_days})"
            )
            print("  Demand (D+2..end)  latest published Meteologica regional vintage")
            print(
                f"  Forward-filled     {', '.join(forward_fill_cols)}  (carried from last known value)"
            )
            if models.skipped_hours:
                print(
                    f"  Hours skipped      {models.skipped_hours}  (insufficient training rows)"
                )
            if built["dropped_groups"]:
                print(f"  Dropped groups     {', '.join(built['dropped_groups'])}")
            partial = [r["target_date"] for r in rows if not r["features_complete"]]
            if partial:
                print(
                    "  Partial-feature days (e.g. past the weather-forecast horizon): "
                    + ", ".join(partial)
                )
            print()

            print_section("Forward Strip ($/MWh)")
            disp = strip_table.copy()
            for c in ("onpeak", "offpeak", "flat", "p10_onpeak", "p90_onpeak"):
                disp[c] = disp[c].map(lambda v: "" if pd.isna(v) else f"{v:>9.2f}")
            disp = disp.rename(
                columns={
                    "target_date": "delivery_date",
                    "onpeak": "OnPk",
                    "offpeak": "OffPk",
                    "flat": "Flat",
                    "p10_onpeak": "P10_OnPk",
                    "p90_onpeak": "P90_OnPk",
                    "features_complete": "feat_ok",
                }
            )[
                [
                    "delivery_date",
                    "lead",
                    "dow",
                    "OnPk",
                    "OffPk",
                    "Flat",
                    "P10_OnPk",
                    "P90_OnPk",
                    "n_he",
                    "feat_ok",
                ]
            ]
            print(disp.to_string(index=False))
            print()
            print_divider("=", 120, dim=False)
            print()

            # Per-day Quantile Bands tables (same layout as the single-day report).
            if per_day_detail:
                for d in target_dates:
                    if (
                        int(
                            strip_table.loc[
                                strip_table["target_date"] == d.isoformat(), "n_he"
                            ].iloc[0]
                        )
                        == 0
                    ):
                        continue  # no features for this day -> nothing to show
                    print_header(
                        f"LINEAR ARX FORECAST -- {hub} ($/MWh)  |  {d}  (meteo_hourly)",
                        "=",
                        120,
                    )
                    printers.print_quantiles(bands_by_date[d.isoformat()])
                    print_divider("=", 120, dim=False)
                    print()

        return {
            "run_date": str(resolved_run_date),
            "horizon_days": int(horizon_days),
            "hub": hub,
            "forward_filled_cols": list(forward_fill_cols),
            "strip_table": strip_table,
            "forecasts_by_date": forecasts_by_date,
            "bands_by_date": bands_by_date,
            "skipped_hours": models.skipped_hours,
            "backward_coef_share": models.backward_coef_share,
            "dropped_groups": built["dropped_groups"],
            "n_features": len(feature_cols),
            "run_id": run_id,
        }
    finally:
        pl.close()


if __name__ == "__main__":
    run()
