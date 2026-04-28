"""Configuration for hourly KNN PJM DA forecasting.

Same conceptual feature groups as forward_only_knn (load, weather, renewables,
gas, outages, calendar) but evaluated at hour resolution. Each row in the pool
is one (date, hour_ending) pair instead of one date.

The dict-of-feature-group-weights is the primary tuning surface — it's what
the weight optimizer will sweep over.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

from da_models.common.configs import CACHE_DIR as DEFAULT_SHARED_CACHE_DIR

SCHEMA: str = "pjm_cleaned"
HUB: str = "WESTERN HUB"
LOAD_REGION: str = "RTO"

CACHE_DIR: Path = Path(os.getenv("DA_MODELS_CACHE_DIR", str(DEFAULT_SHARED_CACHE_DIR)))

DEFAULT_TARGET_DATE: date = date.today() + timedelta(days=1)
DEFAULT_N_ANALOGS: int = 20
MIN_POOL_SIZE: int = 30  # smaller than daily because per-hour pool is naturally thinner

HOURS: list[int] = list(range(1, 25))
QUANTILES: list[float] = [0.10, 0.25, 0.50, 0.75, 0.90]

# Per-(date, hour) feature columns grouped by source.
# Hourly columns end in `_at_hour`; daily columns are reused as constants across
# all 24 rows of a given date.
FEATURE_GROUPS: dict[str, list[str]] = {
    "load_at_hour":      ["load_mw_at_hour"],
    "weather_at_hour":   ["temp_at_hour"],
    "renewable_at_hour": ["solar_at_hour", "wind_at_hour"],
    "gas_daily":         ["gas_m3_daily_avg"],
    "outage_daily":      ["outage_total_mw"],
    "calendar":          ["is_weekend", "dow_sin", "dow_cos"],
}

# Default group weights — same shape as forward_only_knn so the comparison is
# apples-to-apples. The weight optimizer will mutate these.
FEATURE_GROUP_WEIGHTS: dict[str, float] = {
    "load_at_hour":      3.0,
    "weather_at_hour":   2.0,
    "renewable_at_hour": 1.5,
    "gas_daily":         2.0,
    "outage_daily":      1.5,
    "calendar":          1.0,
}

# Filtering
FILTER_SAME_DOW_GROUP: bool = True
FILTER_EXCLUDE_HOLIDAYS: bool = True
FILTER_SEASON_WINDOW_DAYS: int = 60

# Recency penalty (linear ageing — same convention as forward_only_knn).
RECENCY_HALF_LIFE_DAYS: int = 730


@dataclass
class HourlyKNNConfig:
    """Run-level configuration for hourly KNN."""

    forecast_date: str | None = None
    n_analogs: int = DEFAULT_N_ANALOGS
    min_pool_size: int = MIN_POOL_SIZE
    same_dow_group: bool = FILTER_SAME_DOW_GROUP
    exclude_holidays: bool = FILTER_EXCLUDE_HOLIDAYS
    season_window_days: int = FILTER_SEASON_WINDOW_DAYS
    recency_half_life_days: int = RECENCY_HALF_LIFE_DAYS
    quantiles: list[float] = field(default_factory=lambda: list(QUANTILES))
    feature_group_weights: dict[str, float] | None = None
    schema: str = SCHEMA
    hub: str = HUB

    def resolved_target_date(self) -> date:
        if self.forecast_date:
            return date.fromisoformat(self.forecast_date)
        return DEFAULT_TARGET_DATE

    def resolved_weights(self) -> dict[str, float]:
        return dict(self.feature_group_weights) if self.feature_group_weights else dict(FEATURE_GROUP_WEIGHTS)
