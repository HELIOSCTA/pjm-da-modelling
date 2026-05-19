"""CLI entrypoint: `python -m backend.reports.forecast_differences`."""
from __future__ import annotations

import argparse

from backend.reports.forecast_differences import DEFAULT_AREAS, run


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="forecast_differences",
        description="Render the 7-day load forecast diff HTML report.",
    )
    parser.add_argument(
        "--areas",
        nargs="+",
        default=list(DEFAULT_AREAS),
        help=f"Forecast areas to include (default: {' '.join(DEFAULT_AREAS)})",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=2,
        help="evaluated_at_utc lookback window in days (default: 2)",
    )
    args = parser.parse_args()

    run(forecast_areas=args.areas, eval_lookback_days=args.lookback)


if __name__ == "__main__":
    main()
