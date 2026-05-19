"""Single-day supply-stack DA-LMP forecast (Western Hub) -- terminal output.

Structural merit-order dispatch: builds the outage-derated PJM supply
stack, dispatches each hour's net load, sets the clearing price =
marginal variable cost + congestion adder + a reserve-utilization
scarcity adder. Forward-looking and extrapolates by construction, so it
works for a next-week delivery date (uses the latest load/renewable
vintage) and stays sane on extreme heat days where the data-driven
models under-react. Monte-Carlo bands from load / forced-outage / gas
perturbations.

Research / standalone -- ``run(...)`` computes, prints, returns a dict;
nothing here writes Postgres. ``quiet=True`` suppresses the report.

Usage::

    python -m da_models.supply_stack.pipelines.forecast_single_day
    python modelling/da_models/supply_stack/pipelines/forecast_single_day.py
"""

from __future__ import annotations

import sys
import uuid
from datetime import date, timedelta
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[4]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))


from da_models.common.data import loader  # noqa: E402
from da_models.common.forecast.output import actuals_from_pool, build_output_table  # noqa: E402
from da_models.common.data.lmp_pool import build_lmp_labels  # noqa: E402
from da_models.supply_stack import configs as C  # noqa: E402
from da_models.supply_stack import printers  # noqa: E402
from da_models.supply_stack.forecast import forecast_day  # noqa: E402
from utils.logging_utils import init_logging, print_divider, print_header  # noqa: E402

# -- Defaults (edit here instead of using CLI flags) -----------------------
TARGET_DATE: date | None = None  # None -> tomorrow
RUN_DATE: date | None = None  # forecast vintage; None -> today
HUB: str = C.HUB
LATEST_ONLY: bool = True  # use latest load/renewable vintage (works for next-week dates); False -> lead-1
WITH_BANDS: bool = True
CACHE_DIR: Path | None = None
LOG_DIR: Path = _MODELLING_ROOT / "logs"


def run(
    target_date: date | None = TARGET_DATE,
    run_date: date | None = RUN_DATE,
    hub: str = HUB,
    latest_only: bool = LATEST_ONLY,
    with_bands: bool = WITH_BANDS,
    cache_dir: Path | None = CACHE_DIR,
    quiet: bool = False,
) -> dict:
    """Run the supply-stack forecast. Returns a dict: ``forecast_table``
    (per-HE clearing price + ``q_*`` bands + structural metadata),
    ``output_table`` (Actual/Forecast/Error), ``hourly_table`` (display),
    ``fleet``, ``fleet_meta``, ``inputs``, ``has_inputs``, ``has_actuals``,
    ``forecast_date``, ``run_date``, ``hub``, ``run_id``."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name="supply_stack", log_dir=LOG_DIR)
    try:
        resolved_date = (
            target_date if target_date is not None else date.today() + timedelta(days=1)
        )
        resolved_run_date = run_date if run_date is not None else date.today()
        run_id = str(uuid.uuid4())

        with pl.timer("build fleet + assemble inputs + dispatch 24 HE"):
            fc = forecast_day(
                resolved_date,
                cache_dir=cache_dir,
                hub=hub,
                latest_only=latest_only,
                with_bands=with_bands,
            )
        table = fc["forecast_table"]
        if not fc["has_inputs"]:
            pl.warning(
                f"Required inputs missing for {resolved_date} (load/renewables or gas). Forecast empty."
            )

        # Settled DA LMP at the hub, if it exists yet.
        actuals_hourly = None
        try:
            label_wide = build_lmp_labels(loader.load_lmps_da(cache_dir=cache_dir), hub)
            actuals_hourly = actuals_from_pool(label_wide, resolved_date)
        except Exception as exc:  # noqa: BLE001
            pl.warning(f"could not load settled DA LMP: {exc}")

        output_table = (
            build_output_table(resolved_date, table, actuals_hourly)
            if fc["has_inputs"]
            else None
        )
        hourly_table = (
            printers.build_hourly_table(resolved_date, table, actuals_hourly)
            if fc["has_inputs"]
            else None
        )

        if not quiet:
            printers.print_config(
                resolved_date, hub, fc["fleet"], fc["fleet_meta"], fc["inputs"]
            )
            print_header(
                f"SUPPLY STACK FORECAST -- {hub} ($/MWh)  |  {resolved_date}", "=", 120
            )
            if not fc["has_inputs"]:
                pl.warning("no forecast to print -- inputs unavailable.")
            else:
                printers.print_hourly(hourly_table, table)
                if actuals_hourly is not None and output_table is not None:
                    print(output_table.to_string(index=False))
                    print()
                printers.print_sanity(table, fc["inputs"], actuals_hourly)
            print_divider("=", 120, dim=False)
            print()

        return {
            "forecast_table": table,
            "output_table": output_table,
            "hourly_table": hourly_table,
            "fleet": fc["fleet"],
            "fleet_meta": fc["fleet_meta"],
            "inputs": fc["inputs"],
            "has_inputs": fc["has_inputs"],
            "has_actuals": actuals_hourly is not None,
            "forecast_date": str(resolved_date),
            "run_date": str(resolved_run_date),
            "hub": hub,
            "run_id": run_id,
        }
    finally:
        pl.close()


if __name__ == "__main__":
    run()
