"""Shared single-day run logic for every linear ARX variant.

A variant's ``pipelines/forecast_single_day.py`` is a thin wrapper: it
defines its module-level defaults and calls ``run_single_day`` with its
own ``build_panel`` function and ``config`` module. Output layout mirrors
``da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day``.
"""

from __future__ import annotations

import sys
import uuid
from datetime import date, timedelta
from pathlib import Path
from types import ModuleType
from typing import Callable

import numpy as np
import pandas as pd

from da_models.common.configs import HOURS
from da_models.common.forecast.output import actuals_from_pool, build_output_table
from da_models.linear_arx_da_price import configs as C
from da_models.linear_arx_da_price import printers
from da_models.linear_arx_da_price.forecast import (
    build_quantiles_table,
    forecast_target_date,
)
from da_models.linear_arx_da_price.trainer import train
from utils.logging_utils import init_logging, print_divider, print_header

_LOG_DIR: Path = Path(__file__).resolve().parents[2] / "logs"  # modelling/logs


def _naive_d7(label_wide: pd.DataFrame, target_date: date) -> dict[int, float] | None:
    rows = label_wide[label_wide["date"] == target_date - timedelta(days=7)]
    if rows.empty:
        return None
    rec = rows.iloc[0]
    out = {
        h: float(rec[f"lmp_h{h}"])
        for h in range(1, 25)
        if pd.notna(rec.get(f"lmp_h{h}"))
    }
    return out or None


def run_single_day(
    *,
    build_panel: Callable[..., dict],
    variant_cfg: ModuleType,
    target_date: date | None,
    run_date: date | None,
    hub: str,
    cache_dir: Path | None,
    quiet: bool,
) -> dict:
    """Run the variant and print the FORECAST CONFIGURATION / Model
    Diagnostics / LINEAR ARX FORECAST report. Returns the artefact dict
    (``output_table``, ``quantiles_table``, ``df_forecast``, ``metrics``,
    ``block_level``, ``backward_coef_share``, ``skipped_hours``,
    ``dropped_groups``, ``variant``, ``forecast_date``, ``run_date``,
    ``hub``, ``has_actuals``, ``n_features``, ``run_id``)."""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    pl = init_logging(name=variant_cfg.MODEL_NAME, log_dir=_LOG_DIR)
    try:
        resolved_date = (
            target_date if target_date is not None else date.today() + timedelta(days=1)
        )
        resolved_run_date = run_date if run_date is not None else date.today()
        run_id = str(uuid.uuid4())

        with pl.timer("build feature panel"):
            built = build_panel(target_date=resolved_date, cache_dir=cache_dir, hub=hub)
        panel = built["panel"]
        feature_cols = built["feature_cols"]
        if not built["has_target_features"]:
            pl.warning(
                f"Target-date features incomplete for {resolved_date} (lead {C.LEAD_DAYS}). "
                "Forecast rows will be empty."
            )

        with pl.timer("train 24 per-hour ridge models"):
            models = train(panel, feature_cols, resolved_date)
        with pl.timer("forecast target date"):
            df_forecast = forecast_target_date(models, panel, resolved_date)

        label_wide = built["label_wide"]
        actuals_hourly = actuals_from_pool(label_wide, resolved_date)
        naive = _naive_d7(label_wide, resolved_date)

        output_table = build_output_table(resolved_date, df_forecast, actuals_hourly)
        quantiles_table = build_quantiles_table(
            resolved_date, df_forecast, C.DISPLAY_QUANTILES
        )
        metrics = printers.compute_metrics(df_forecast, actuals_hourly, naive)

        block_level: dict = {}
        in_band_80: list[bool | None] = []
        crps_per_hour = np.full(24, np.nan)
        if actuals_hourly is not None and len(df_forecast) > 0:
            fc_by_he = dict(
                zip(
                    df_forecast["hour_ending"].astype(int),
                    df_forecast["point_forecast"].astype(float),
                )
            )
            actual_arr = np.array(
                [actuals_hourly.get(h, np.nan) for h in HOURS], dtype=float
            )
            forecast_arr = np.array(
                [fc_by_he.get(h, np.nan) for h in HOURS], dtype=float
            )
            naive_full = (
                np.array([naive[h] for h in HOURS], dtype=float)
                if naive is not None
                else None
            )
            block_level = printers.compute_block_level(
                actual_arr, forecast_arr, naive_full
            )
            in_band_80 = printers.compute_in_band_80(quantiles_table, actuals_hourly)
            crps_per_hour = printers.compute_crps_per_hour(df_forecast, actuals_hourly)

        if not quiet:
            printers.print_config(
                resolved_date, hub, feature_cols, built["dropped_groups"], variant_cfg
            )
            if models.skipped_hours:
                pl.warning(f"hours skipped (insufficient data): {models.skipped_hours}")
            printers.print_model_diagnostics(models, feature_cols)
            print_header(
                f"LINEAR ARX FORECAST -- {hub} ($/MWh)  |  {resolved_date}  ({variant_cfg.VARIANT})",
                "=",
                120,
            )
            printers.print_quantiles(quantiles_table)
            printers.print_forecast(output_table, block_level=block_level or None)
            printers.print_band_calibration(
                output_table,
                quantiles_table,
                in_band_80=in_band_80 or None,
                crps_per_hour=crps_per_hour,
            )
            print()
            print_divider("=", 120, dim=False)
            print()

        return {
            "output_table": output_table,
            "quantiles_table": quantiles_table,
            "df_forecast": df_forecast,
            "metrics": metrics,
            "block_level": block_level,
            "backward_coef_share": models.backward_coef_share,
            "skipped_hours": models.skipped_hours,
            "dropped_groups": built["dropped_groups"],
            "variant": variant_cfg.VARIANT,
            "forecast_date": str(resolved_date),
            "run_date": str(resolved_run_date),
            "hub": hub,
            "has_actuals": actuals_hourly is not None,
            "n_features": len(feature_cols),
            "run_id": run_id,
        }
    finally:
        pl.close()
