"""
Aggregate the historical backfill parquet by issue date for verification.

Mirrors the dbt audit view at
    backend/dbt/.../models/meteologica/pjm/marts/audit/audit_meteologica_pjm_vintage_coverage.sql
so the two outputs can be diffed side-by-side: the parquet is the API's
ground-truth archive, and the dbt view exposes whatever the live
latest-snapshot scrape captured into Postgres.

Usage:
    python -m backend.scrapes.meteologica.pjm.historical.verify_backfill
    python -m backend.scrapes.meteologica.pjm.historical.verify_backfill \\
        --contents usa_pjm_power_demand_forecast_hourly
    python -m backend.scrapes.meteologica.pjm.historical.verify_backfill \\
        --contents usa_pjm_power_demand_forecast_hourly \\
        --start 2026-03-01 --end 2026-04-01
    python -m backend.scrapes.meteologica.pjm.historical.verify_backfill \\
        --csv-out /tmp/parquet_summary.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from backend.scrapes.meteologica.pjm.historical import _io


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    """Per (region, variable, content_id, issue_local_date) aggregate.

    Column names match the dbt audit view exactly.
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["issue_local_date"] = df["issue_date_local"].dt.normalize()
    df["forecast_date"] = df["forecast_period_start_local"].dt.normalize()

    grouped = df.groupby(
        ["region", "variable", "content_id", "issue_local_date"],
        as_index=False,
    ).agg(
        n_issues=("issue_date_utc", "nunique"),
        n_rows=("forecast_value_mw", "size"),
        min_issue_utc=("issue_date_utc", "min"),
        max_issue_utc=("issue_date_utc", "max"),
        min_forecast_period_start=("forecast_period_start_local", "min"),
        max_forecast_period_start=("forecast_period_start_local", "max"),
        n_forecast_dates=("forecast_date", "nunique"),
        n_null_forecast_mw=("forecast_value_mw", lambda s: int(s.isna().sum())),
        avg_forecast_mw=("forecast_value_mw", "mean"),
        min_forecast_mw=("forecast_value_mw", "min"),
        max_forecast_mw=("forecast_value_mw", "max"),
    )
    grouped[["avg_forecast_mw", "min_forecast_mw", "max_forecast_mw"]] = (
        grouped[["avg_forecast_mw", "min_forecast_mw", "max_forecast_mw"]].round(2)
    )
    return grouped.sort_values(
        ["content_id", "issue_local_date"], ascending=[True, False],
    )


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--contents", nargs="*", default=None)
    p.add_argument("--archive-root", type=Path, default=_io.ARCHIVE_ROOT_DEFAULT)
    p.add_argument(
        "--start", default=None,
        help="YYYY-MM-DD lower bound (inclusive) on issue_local_date",
    )
    p.add_argument(
        "--end", default=None,
        help="YYYY-MM-DD upper bound (exclusive) on issue_local_date",
    )
    p.add_argument(
        "--csv-out", type=Path, default=None,
        help="Write summary to CSV instead of printing.",
    )
    args = p.parse_args()

    contents = args.contents or _io.all_api_scrape_names()
    summaries = []

    for name in contents:
        path = _io.long_parquet_path(name, args.archive_root)
        if not path.exists():
            print(f"[skip] {name}: no parquet at {path}")
            continue
        df = pd.read_parquet(path)
        summary = summarize(df)
        if args.start:
            summary = summary[summary["issue_local_date"] >= pd.Timestamp(args.start)]
        if args.end:
            summary = summary[summary["issue_local_date"] < pd.Timestamp(args.end)]
        summary.insert(0, "api_scrape_name", name)
        summaries.append(summary)

    if not summaries:
        print("No summaries produced.")
        return

    out = pd.concat(summaries, ignore_index=True)

    if args.csv_out:
        args.csv_out.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(args.csv_out, index=False)
        print(f"Wrote {len(out)} rows to {args.csv_out}")
    else:
        with pd.option_context(
            "display.max_rows", 200,
            "display.max_columns", None,
            "display.width", 200,
        ):
            print(out.to_string(index=False))


if __name__ == "__main__":
    main()
