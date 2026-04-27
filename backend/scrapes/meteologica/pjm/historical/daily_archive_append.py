"""
Daily forward-only append to the Meteologica historical archive.

Designed to run AFTER the existing 12 latest-snapshot scrapes in
`backend/scrapes/meteologica/pjm/`. For each registered content this fetches
the current `contents/{id}/data` snapshot, appends to the long-table parquet,
and rebuilds the DA-cutoff parquet.

The append is idempotent: PK dedup on
    (content_id, issue_date_utc, forecast_period_start_utc).

Usage:
    python -m backend.scrapes.meteologica.pjm.historical.daily_archive_append
    python -m backend.scrapes.meteologica.pjm.historical.daily_archive_append --contents usa_pjm_power_demand_forecast_hourly
    python -m backend.scrapes.meteologica.pjm.historical.daily_archive_append --no-derive-cutoff
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from backend.scrapes.meteologica.auth import make_get_request
from backend.scrapes.meteologica.pjm.historical import _io
from backend.scrapes.meteologica.pjm.historical.derive_da_cutoff import derive_for_content
from backend.utils import logging_utils, pipeline_run_logger

API_SCRAPE_NAME = "meteologica_pjm_historical_daily_append"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--contents", nargs="*", default=None)
    p.add_argument("--sleep-seconds", type=float, default=0.5)
    p.add_argument("--archive-root", type=Path, default=_io.ARCHIVE_ROOT_DEFAULT)
    p.add_argument(
        "--no-derive-cutoff",
        dest="derive_cutoff", action="store_false", default=True,
    )
    return p.parse_args()


def _pull_latest(content_id: int) -> dict:
    response = make_get_request(f"contents/{content_id}/data", account="iso")
    payload = response.json()
    payload["_origin_filename"] = (
        f"{content_id}_{payload.get('update_id', 'snapshot')}.json"
    )
    return payload


def main() -> None:
    args = _parse_args()
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

    total_rows = 0

    try:
        logger.header(f"{API_SCRAPE_NAME}")

        for api_scrape_name in contents:
            entry = _io.get_registry_entry(api_scrape_name)
            content_id = entry["content_id"]

            logger.section(f"{api_scrape_name} (content_id={content_id})")

            try:
                payload = _pull_latest(content_id)
            except Exception as e:
                logger.error(f"  Pull failed: {e}")
                continue

            df_new = _io.payloads_to_long_df([payload], entry)
            long_path = _io.long_parquet_path(api_scrape_name, args.archive_root)
            _, added_net = _io.write_long_parquet(df_new, long_path)

            logger.info(
                f"  Appended {len(df_new)} rows, +{added_net} net "
                f"(issue_date={payload.get('issue_date')})"
            )
            total_rows += len(df_new)

            if args.derive_cutoff:
                cutoff_rows = derive_for_content(api_scrape_name, args.archive_root)
                logger.info(f"  Derived DA-cutoff parquet: {cutoff_rows} rows")

            time.sleep(args.sleep_seconds)

        logger.success(
            f"Daily append complete: {total_rows} rows pulled across "
            f"{len(contents)} contents.",
        )
        run.success(rows_processed=total_rows)

    except Exception as e:
        logger.exception(f"Daily append failed: {e}")
        run.failure(error=e)
        raise

    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()
