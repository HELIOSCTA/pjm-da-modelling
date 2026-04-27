"""
Backfill Meteologica historical forecasts to a vintage-stamped archive.

For each (content, year, month) in the requested grid, hits the Meteologica
`historical_data` endpoint, persists raw JSONs under
modelling/data/cache_archive/meteologica/pjm/raw/, and appends to the
long-table parquet at modelling/data/cache_archive/meteologica/pjm/long/.

Re-running is safe: PK dedup on
    (content_id, issue_date_utc, forecast_period_start_utc).

Usage:
    python -m backend.scrapes.meteologica.pjm.historical.backfill_historical
    python -m backend.scrapes.meteologica.pjm.historical.backfill_historical --start 2025-04 --end 2026-04
    python -m backend.scrapes.meteologica.pjm.historical.backfill_historical --contents usa_pjm_power_demand_forecast_hourly
    python -m backend.scrapes.meteologica.pjm.historical.backfill_historical --no-save-raw --sleep-seconds 0.5
"""

from __future__ import annotations

import argparse
import time
from datetime import date
from pathlib import Path

from backend.scrapes.meteologica.pjm.historical import _io
from backend.utils import logging_utils, pipeline_run_logger

API_SCRAPE_NAME = "meteologica_pjm_historical_backfill"


def _parse_yyyymm(s: str) -> tuple[int, int]:
    year_str, month_str = s.split("-")
    return int(year_str), int(month_str)


def _iter_months(start: tuple[int, int], end: tuple[int, int]):
    y, m = start
    while (y, m) <= end:
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


def _default_start() -> tuple[int, int]:
    today = date.today()
    y, m = today.year, today.month
    m -= 24
    while m <= 0:
        m += 12
        y -= 1
    return y, m


def _default_end() -> tuple[int, int]:
    today = date.today()
    return today.year, today.month


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--start", default=None, help="YYYY-MM (default: 24 months ago)")
    p.add_argument("--end",   default=None, help="YYYY-MM (default: current month)")
    p.add_argument(
        "--contents", nargs="*", default=None,
        help="api_scrape_name(s). Default = all 12 PJM Meteologica contents.",
    )
    p.add_argument("--sleep-seconds", type=float, default=1.0, help="Throttle between API calls.")
    p.add_argument("--archive-root", type=Path, default=_io.ARCHIVE_ROOT_DEFAULT)
    p.add_argument("--save-raw", dest="save_raw", action="store_true", default=True)
    p.add_argument("--no-save-raw", dest="save_raw", action="store_false")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    start = _parse_yyyymm(args.start) if args.start else _default_start()
    end   = _parse_yyyymm(args.end)   if args.end   else _default_end()
    contents = args.contents or _io.all_api_scrape_names()

    logger = logging_utils.init_logging(
        name=API_SCRAPE_NAME,
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )

    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name=API_SCRAPE_NAME,
        source="power",
        target_table=str(args.archive_root / "long"),
        operation_type="upsert",
        log_file_path=logger.log_file_path,
    )
    run.start()

    total_payloads = 0
    total_rows = 0

    try:
        logger.header(f"{API_SCRAPE_NAME}")
        logger.info(f"Range: {start[0]:04d}-{start[1]:02d} -> {end[0]:04d}-{end[1]:02d}")
        logger.info(f"Contents: {len(contents)} ({', '.join(contents)})")
        logger.info(f"Archive root: {args.archive_root}")

        for api_scrape_name in contents:
            entry = _io.get_registry_entry(api_scrape_name)
            content_id = entry["content_id"]
            long_path = _io.long_parquet_path(api_scrape_name, args.archive_root)

            logger.section(f"{api_scrape_name} (content_id={content_id})")

            for year, month in _iter_months(start, end):
                try:
                    payloads, raw_by_filename = _io.pull_historical_month(content_id, year, month)
                except Exception as e:
                    logger.error(f"  {year:04d}-{month:02d}: pull failed: {e}")
                    continue

                if not payloads:
                    logger.info(f"  {year:04d}-{month:02d}: no data (404 or empty)")
                    time.sleep(args.sleep_seconds)
                    continue

                if args.save_raw:
                    saved = _io.save_raw_payloads(
                        raw_by_filename, content_id, year, month, args.archive_root,
                    )
                else:
                    saved = 0

                df_new = _io.payloads_to_long_df(payloads, entry)
                _, added_net = _io.write_long_parquet(df_new, long_path)

                logger.info(
                    f"  {year:04d}-{month:02d}: {len(payloads)} issues, "
                    f"{len(df_new)} rows pulled, +{added_net} net rows, raw_saved={saved}"
                )
                total_payloads += len(payloads)
                total_rows += len(df_new)

                time.sleep(args.sleep_seconds)

        logger.success(
            f"Backfill complete: {total_payloads} payloads, {total_rows} forecast rows.",
        )
        run.success(rows_processed=total_rows, files_processed=total_payloads)

    except Exception as e:
        logger.exception(f"Backfill failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()
