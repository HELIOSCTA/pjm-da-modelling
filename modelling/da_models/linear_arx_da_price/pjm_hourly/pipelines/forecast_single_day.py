"""Single-day ``pjm_hourly`` linear ARX DA-price forecast -- terminal output.

LEAR-style 24-per-hour ridge ARX on PJM forward fundamentals (RTO
supply-demand + sub-zonal load), target in asinh space, recency-weighted;
residual-quantile bands. Print layout mirrors the like-day
``pjm_rto_hourly`` report (FORECAST CONFIGURATION / Model Diagnostics /
Quantile Bands / Forecast vs Actuals / Quantile Bands vs Actuals).

Research / standalone: ``run(...)`` computes, prints, and returns a dict;
nothing here writes Postgres (publishing is owned by the scheduled twin
under ``backend/modelling/da_models/``). ``quiet=True`` suppresses the
printed report. The sibling ``meteo_hourly`` variant swaps the demand
block for Meteologica regional supply-demand.

Usage::

    python -m da_models.linear_arx_da_price.pjm_hourly.pipelines.forecast_single_day
    python modelling/da_models/linear_arx_da_price/pjm_hourly/pipelines/forecast_single_day.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from da_models.linear_arx_da_price import configs as C  # noqa: E402
from da_models.linear_arx_da_price import run as _run  # noqa: E402
from da_models.linear_arx_da_price.pjm_hourly import config as V  # noqa: E402
from da_models.linear_arx_da_price.pjm_hourly.builder import build_panel  # noqa: E402

# -- Defaults (edit here instead of using CLI flags) -----------------------
TARGET_DATE: date | None = None  # None -> tomorrow
RUN_DATE: date | None = None  # forecast vintage; None -> today
HUB: str = C.HUB
CACHE_DIR: Path | None = None


def run(
    target_date: date | None = TARGET_DATE,
    run_date: date | None = RUN_DATE,
    hub: str = HUB,
    cache_dir: Path | None = CACHE_DIR,
    quiet: bool = False,
) -> dict:
    return _run.run_single_day(
        build_panel=build_panel,
        variant_cfg=V,
        target_date=target_date,
        run_date=run_date,
        hub=hub,
        cache_dir=cache_dir,
        quiet=quiet,
    )


if __name__ == "__main__":
    run()
