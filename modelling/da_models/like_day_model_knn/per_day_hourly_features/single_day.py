"""Single-day backtest for per_day_hourly_features.

Usage::

    python -m da_models.like_day_model_knn.per_day_hourly_features.single_day --date 2024-08-06
"""
from __future__ import annotations

import argparse
import contextlib
import io
import sys
from datetime import date, datetime
from pathlib import Path

_MODELLING_ROOT = Path(__file__).resolve().parents[3]
if str(_MODELLING_ROOT) not in sys.path:
    sys.path.insert(0, str(_MODELLING_ROOT))

from da_models.like_day_model_knn import _shared, configs, diagnostics_common as dc  # noqa: E402
from da_models.like_day_model_knn.analog_store import (  # noqa: E402
    DEFAULT_STORE_DIR,
    write_analog_explainability,
)
from da_models.like_day_model_knn.per_day_hourly_features.builder import (  # noqa: E402
    build_pool, build_query_row,
)
from da_models.like_day_model_knn.per_day_hourly_features.engine import find_twins_day  # noqa: E402
from da_models.like_day_model_knn.per_day_hourly_features.forecast import (  # noqa: E402
    actuals_from_pool, hourly_forecast_from_day_analogs,
)
from html_reports.utils.html_dashboard import HTMLDashboardBuilder  # noqa: E402
from utils.logging_utils import init_logging  # noqa: E402

REPORT_OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def generate(
    target_date: date,
    output_dir: Path | None = None,
    n_analogs: int | None = None,
    season_window_days: int | None = None,
    min_pool_size: int | None = None,
    write_analog_store: bool = True,
    analog_store_dir: Path | None = None,
    pl=None,
) -> Path:
    output_dir = output_dir or REPORT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    base_config = configs.KnnModelConfig(
        forecast_date=str(target_date),
        model_name=configs.PER_DAY_HOURLY_FEATURES_SPEC.name,
        n_analogs=configs.DEFAULT_N_ANALOGS if n_analogs is None else int(n_analogs),
        season_window_days=(
            configs.SEASON_WINDOW_DAYS if season_window_days is None
            else int(season_window_days)
        ),
        min_pool_size=(
            configs.MIN_POOL_SIZE if min_pool_size is None else int(min_pool_size)
        ),
    )
    config, day_type = base_config.with_day_type_overrides(target_date)
    spec = config.resolved_spec()
    quantiles = config.resolved_quantiles()
    if pl:
        pl.info(f"day_type={day_type} (same_dow_group={config.same_dow_group}, season_window_days={config.season_window_days})")

    if pl:
        pl.header(f"per_day_hourly_features - {target_date}")
        pl.info(spec.description)

    with contextlib.redirect_stdout(io.StringIO()):
        pool = build_pool(schema=config.schema, hub=config.hub, cache_dir=configs.CACHE_DIR)
        query = build_query_row(
            target_date=target_date, schema=config.schema, cache_dir=configs.CACHE_DIR,
        )
        dates_meta = _shared.load_dates_daily(configs.CACHE_DIR)
        analogs = find_twins_day(
            query=query, pool=pool, target_date=target_date, spec=spec,
            n_analogs=config.n_analogs,
            season_window_days=config.season_window_days,
            min_pool_size=config.min_pool_size,
            dates_meta=dates_meta,
            same_dow_group=config.same_dow_group,
            exclude_holidays=config.exclude_holidays,
            exclude_dates=config.exclude_dates,
        )
        hourly_rto = _shared.load_hourly_rto(configs.CACHE_DIR)

    if write_analog_store:
        store_dir = analog_store_dir or DEFAULT_STORE_DIR
        run_id = write_analog_explainability(
            target_date=target_date,
            config=config,
            spec=spec,
            pool=pool,
            query=query,
            analogs=analogs,
            output_dir=store_dir,
        )
        if pl:
            pl.info(f"Analog explainability store run_id={run_id}: {store_dir}")

    if len(analogs) == 0:
        sections = [("Run Error", dc.empty_fragment("No analogs returned for target date."), None)]
    else:
        target_actuals = actuals_from_pool(pool, target_date)
        df_forecast = hourly_forecast_from_day_analogs(analogs, quantiles)
        forecast_table = dc.hourly_forecast_table(df_forecast, target_actuals)
        hourly_values = dc.hourly_load_table(target_date, hourly_rto)

        sections = [
            ("Run Summary", dc.summary_html(
                target_date=target_date,
                spec_name=spec.name, spec_description=spec.description,
                n_pool=len(pool), n_analogs_total=len(analogs),
                forecast_table=forecast_table, hub=config.hub,
                season_window_days=config.season_window_days,
            ), None),
            ("Hourly Values - Chart", dc.hourly_values_fig(hourly_values), None),
            ("Analogs - Selected Days", dc.analog_weights_fig_day(analogs), None),
            ("Analogs - Load Curve Overlay",
             dc.analog_load_overlay_fig_day(analogs, target_date, hourly_rto), None),
            ("Hourly Forecast - Chart", dc.forecast_fig(forecast_table, hub=config.hub), None),
            ("Hourly Errors - Chart", dc.hourly_error_fig(forecast_table), None),
        ]

    builder = HTMLDashboardBuilder(
        title=f"Per Day Hourly Features - {target_date}", theme="dark",
    )
    current_group = None
    for label, content, icon in sections:
        group = label.split(" - ", 1)[0]
        if group != current_group:
            builder.add_divider(group)
            current_group = group
        builder.add_content(label, content, icon=icon)

    output_path = output_dir / f"single_day_{target_date}.html"
    builder.save(str(output_path))
    if pl:
        pl.success(f"Saved: {output_path}")
    return output_path


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Per Day Hourly Features single-day backtest.",
    )
    parser.add_argument("--date", type=_parse_date, required=True, help="Target date YYYY-MM-DD.")
    parser.add_argument("--out-dir", type=Path, default=None,
                        help="Output directory (default: per_day_hourly_features/output).")
    parser.add_argument("--analog-store-dir", type=Path, default=None,
                        help=f"Parquet explainability store directory (default: {DEFAULT_STORE_DIR}).")
    parser.add_argument("--skip-analog-store", action="store_true",
                        help="Do not write DuckDB/Streamlit Parquet explainability tables.")
    return parser.parse_args()


def main() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")

    args = _parse_args()
    pl = init_logging(name="knn_per_day_hourly_features", log_dir=_MODELLING_ROOT / "logs")
    try:
        generate(
            target_date=args.date,
            output_dir=args.out_dir,
            write_analog_store=not args.skip_analog_store,
            analog_store_dir=args.analog_store_dir,
            pl=pl,
        )
    finally:
        pl.close()


if __name__ == "__main__":
    main()
