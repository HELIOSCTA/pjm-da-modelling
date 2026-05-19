"""Backtest configuration -- date range, hub, output paths, default model set.

Edit constants here (or override via the ``run(...)`` kwargs on the
pipelines). The first-run defaults are deliberately small: last 7
*settled* delivery dates, four models (the three under evaluation +
``baseline_meteo`` as the comparison anchor), parquet artefacts only.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

HUB: str = "WESTERN HUB"
HOURS: tuple[int, ...] = tuple(range(1, 25))
QUANTILES: tuple[float, ...] = (0.10, 0.25, 0.50, 0.75, 0.90)

# Last N *settled* delivery dates ending at the most recent settled date.
# v1 default: 7 days. Switch this number to widen the window once the
# scaffold is validated.
DEFAULT_LOOKBACK_DAYS: int = 7

# Models pulled into the v1 run. ``baseline_meteo`` is the comparison
# anchor (rMAE denominator + Diebold-Mariano pairwise pivot); the rest
# are the "models under test".
DEFAULT_MODEL_NAMES: tuple[str, ...] = (
    "supply_stack",
    "linear_arx_pjm_hourly",
    "linear_arx_meteo_hourly",
    "baseline_meteo",
)
BASELINE_MODEL_NAME: str = "baseline_meteo"

# Output directory for the parquet artefacts. Per the python-scripts
# skill, run_id = "{utc_ts}_{uuid_hex[:6]}".
OUTPUT_DIR: Path = Path(__file__).resolve().parent / "output"


def default_target_dates(
    *, end_date: date | None = None, lookback_days: int = DEFAULT_LOOKBACK_DAYS
) -> list[date]:
    """Last ``lookback_days`` delivery dates ending at ``end_date`` (inclusive).

    Does not filter for settled-LMP presence -- the replay loop drops
    dates with no settled actuals when it joins to the LMP feed. Default
    ``end_date`` is yesterday (D-1), which is the latest fully-settled
    delivery date in the DA LMP feed by mid-afternoon EPT.
    """
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    return [end_date - timedelta(days=i) for i in range(lookback_days - 1, -1, -1)]
