"""
Derive the DA-cutoff parquet from the long-table archive.

Replicates the dbt rule from
`backend/dbt/.../staging_v1_meteologica_pjm_<x>_forecast_da_cutoff.sql`:
for each delivery date D, keep the latest issue with
    issue_date_local in (D 10:00 ET - 48h, D 10:00 ET]
partitioned by (forecast_date, hour_ending, region).

Output schema is column-aligned with the existing
    modelling/data/cache/meteologica_pjm_*_da_cutoff.parquet
so the derived parquet is a drop-in replacement.

Usage:
    python -m backend.scrapes.meteologica.pjm.historical.derive_da_cutoff
    python -m backend.scrapes.meteologica.pjm.historical.derive_da_cutoff --contents usa_pjm_power_demand_forecast_hourly
"""

from __future__ import annotations

import argparse
from datetime import time as dtime, timedelta
from pathlib import Path

import pandas as pd

from backend.scrapes.meteologica.pjm.historical import _io
from backend.utils import logging_utils

CUTOFF_LOCAL_TIME = dtime(10, 0)        # 10:00 America/New_York
LOOKBACK = timedelta(hours=48)


def da_cutoff_parquet_path(
    api_scrape_name: str, archive_root: Path = _io.ARCHIVE_ROOT_DEFAULT,
) -> Path:
    return archive_root / "da_cutoff" / f"{api_scrape_name}__vintaged_da_cutoff.parquet"


def _build_da_cutoff(df_long: pd.DataFrame, value_col_out: str) -> pd.DataFrame:
    """Apply the (D 10am ET, 48h lookback, latest issue) rule to a long DF."""
    if df_long.empty:
        return pd.DataFrame()

    df = df_long.copy()
    df["forecast_date"] = df["forecast_period_start_local"].dt.normalize()
    df["forecast_datetime"] = df["forecast_period_start_local"]
    df["cutoff_local"] = (
        df["forecast_date"]
        + pd.Timedelta(hours=CUTOFF_LOCAL_TIME.hour, minutes=CUTOFF_LOCAL_TIME.minute)
    )

    in_window = (
        (df["issue_date_local"] <= df["cutoff_local"])
        & (df["issue_date_local"] > df["cutoff_local"] - LOOKBACK)
    )
    df = df[in_window].copy()
    if df.empty:
        return pd.DataFrame()

    df = df.sort_values(["forecast_date", "hour_ending", "region", "issue_date_local"])
    df = df.drop_duplicates(
        subset=["forecast_date", "hour_ending", "region"], keep="last",
    )

    out = pd.DataFrame({
        "forecast_execution_datetime_utc":   df["issue_date_utc"].values,
        "timezone":                          "America/New_York",
        "forecast_execution_datetime_local": df["issue_date_local"].values,
        "forecast_rank":                     1,
        "forecast_execution_date":           df["issue_date_local"].dt.normalize().values,
        "forecast_datetime":                 df["forecast_datetime"].values,
        "forecast_date":                     df["forecast_date"].values,
        "hour_ending":                       df["hour_ending"].astype("Int64").values,
        "region":                            df["region"].values,
        value_col_out:                       df["forecast_value_mw"].astype(float).values,
    })
    return (
        out.sort_values(["region", "forecast_date", "hour_ending"])
           .reset_index(drop=True)
    )


def derive_for_content(
    api_scrape_name: str, archive_root: Path = _io.ARCHIVE_ROOT_DEFAULT,
) -> int:
    """Materialize the DA-cutoff parquet for one content. Returns row count."""
    entry = _io.get_registry_entry(api_scrape_name)
    long_path = _io.long_parquet_path(api_scrape_name, archive_root)
    if not long_path.exists():
        return 0

    df_long = pd.read_parquet(long_path)
    df_cut = _build_da_cutoff(df_long, value_col_out=entry["value_col"])

    out_path = da_cutoff_parquet_path(api_scrape_name, archive_root)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_cut.to_parquet(out_path, index=False)
    return len(df_cut)


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--contents", nargs="*", default=None)
    p.add_argument("--archive-root", type=Path, default=_io.ARCHIVE_ROOT_DEFAULT)
    args = p.parse_args()

    logger = logging_utils.init_logging(
        name="meteologica_pjm_historical_derive_da_cutoff",
        log_dir=Path(__file__).parent / "logs",
        log_to_file=True,
        delete_if_no_errors=True,
    )

    contents = args.contents or _io.all_api_scrape_names()

    try:
        logger.header("derive_da_cutoff")
        for api_scrape_name in contents:
            n = derive_for_content(api_scrape_name, args.archive_root)
            logger.info(f"  {api_scrape_name}: {n} cutoff rows")
        logger.success(f"Derived {len(contents)} DA-cutoff parquets")
    finally:
        logging_utils.close_logging()


if __name__ == "__main__":
    main()
