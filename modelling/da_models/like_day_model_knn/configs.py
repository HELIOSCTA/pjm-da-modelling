"""Shared configuration for like_day_model_knn.

Three models live in their own subfolders, each with its own builder, engine,
forecast, and single_day backtest:

  per_day_daily_features/   - 6 daily summary features          x day-level matching
  per_day_hourly_features/  - 24 hourly load features           x day-level matching
  per_hour/                 - 3-hour window per target HE       x per-hour matching

This module owns ONLY shared values and the per-model ``ModelSpec`` registry.
Each per-model builder produces its own features; there is no superset builder.
Truly shared parquet/region/label helpers live in ``_shared.py``.
"""
from __future__ import annotations

import copy
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from da_models.common.configs import CACHE_DIR as DEFAULT_SHARED_CACHE_DIR

# ── Database / market ──────────────────────────────────────────────────
SCHEMA: str = "pjm_cleaned"
HUB: str = "WESTERN HUB"
LOAD_REGION: str = "RTO"

# ── Cache ──────────────────────────────────────────────────────────────
CACHE_DIR: Path = Path(os.getenv("DA_MODELS_CACHE_DIR", str(DEFAULT_SHARED_CACHE_DIR)))

# ── Data source parquets ───────────────────────────────────────────────
LOAD_FORECAST_PARQUETS: list[str] = [
    "pjm_load_forecast_hourly_da_cutoff_historical.parquet",
]
LMP_DA_PARQUET: str = "pjm_lmps_hourly.parquet"
PJM_DATES_DAILY_PARQUET: str = "pjm_dates_daily.parquet"

# ── Forecast defaults ──────────────────────────────────────────────────
DEFAULT_TARGET_DATE: date = date.today() + timedelta(days=1)
DEFAULT_N_ANALOGS: int = 20
MIN_POOL_SIZE: int = 100
SEASON_WINDOW_DAYS: int = 60

# ── Calendar / day-type pre-filtering ──────────────────────────────────
# Sun=0..Sat=6 numbering matches pjm_dates_daily.day_of_week_number.
DOW_GROUPS: dict[str, list[int]] = {
    "weekday": [1, 2, 3, 4, 5],
    "saturday": [6],
    "sunday": [0],
}

DAY_TYPE_WEEKDAY: str = "weekday"
DAY_TYPE_SATURDAY: str = "saturday"
DAY_TYPE_SUNDAY: str = "sunday"

FILTER_SAME_DOW_GROUP: bool = True
FILTER_EXCLUDE_HOLIDAYS: bool = True
EXCLUDE_DATES: list[str] = []  # add YYYY-MM-DD strings to drop from the pool

# Saturday/Sunday narrow the window and tighten DOW matching.
# Only knobs that exist on KnnModelConfig are listed here — no feature_group
# weight overrides, since this load-only package doesn't have non-load groups.
DAY_TYPE_SCENARIO_PROFILES: dict[str, dict[str, Any]] = {
    DAY_TYPE_WEEKDAY: {},
    DAY_TYPE_SATURDAY: {
        "same_dow_group": True,
        "season_window_days": 45,
        "n_analogs": 12,
    },
    DAY_TYPE_SUNDAY: {
        "same_dow_group": True,
        "season_window_days": 60,
        "n_analogs": 10,
    },
}

# ── Hours / quantiles ──────────────────────────────────────────────────
HOURS: list[int] = list(range(1, 25))
QUANTILES: list[float] = [0.10, 0.25, 0.50, 0.75, 0.90]
LMP_LABEL_COLUMNS: list[str] = [f"lmp_h{h}" for h in HOURS]


# ── Per-model spec ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class ModelSpec:
    """Per-model feature/weight/matching definition.

    For ``match_unit == "day"``: ``feature_groups`` is a static dict from
    group name to column list, exactly like ``forward_knn_load_forecast``.

    For ``match_unit == "hour"``: ``feature_groups`` is empty - the engine
    builds per-target-hour groups dynamically using ``flt_radius``.
    """
    name: str
    description: str
    match_unit: str  # "day" | "hour"
    feature_groups: dict[str, list[str]] = field(default_factory=dict)
    feature_group_weights: dict[str, float] = field(default_factory=dict)
    flt_radius: int = 0  # only used when match_unit == "hour"


# Daily summary features, day-level matching (baseline)
PER_DAY_DAILY_FEATURES_SPEC = ModelSpec(
    name="per_day_daily_features",
    description="Daily summary features (6) x day-level matching",
    match_unit="day",
    feature_groups={
        "load_level": [
            "fcst_load_daily_avg",
            "fcst_load_daily_peak",
            "fcst_load_daily_valley",
        ],
        "load_ramps": [
            "fcst_load_morning_ramp",
            "fcst_load_evening_ramp",
            "fcst_load_ramp_max",
        ],
    },
    feature_group_weights={
        "load_level": 3.0,
        "load_ramps": 1.0,
    },
)

# Hourly bucketed features, day-level matching
PER_DAY_HOURLY_FEATURES_SPEC = ModelSpec(
    name="per_day_hourly_features",
    description="Hourly bucketed features (24 in 5 blocks) x day-level matching",
    match_unit="day",
    feature_groups={
        "load_overnight": [f"fcst_load_h{h}" for h in range(1, 7)],   # HE1-6
        "load_morning":   [f"fcst_load_h{h}" for h in range(7, 12)],  # HE7-11
        "load_midday":    [f"fcst_load_h{h}" for h in range(12, 17)], # HE12-16
        "load_peak":      [f"fcst_load_h{h}" for h in range(17, 21)], # HE17-20
        "load_evening":   [f"fcst_load_h{h}" for h in range(21, 25)], # HE21-24
    },
    feature_group_weights={
        "load_overnight": 1.0,
        "load_morning":   1.5,
        "load_midday":    2.0,
        "load_peak":      3.5,
        "load_evening":   2.0,
    },
)

# 3-hour window, per-hour matching. The engine builds the window cols
# dynamically per target HE; feature_groups stays empty here.
PER_HOUR_SPEC = ModelSpec(
    name="per_hour",
    description="3-hour window features x per-hour matching (24 matches per day)",
    match_unit="hour",
    feature_groups={},
    feature_group_weights={},
    flt_radius=1,
)

MODEL_REGISTRY: dict[str, ModelSpec] = {
    PER_DAY_DAILY_FEATURES_SPEC.name: PER_DAY_DAILY_FEATURES_SPEC,
    PER_DAY_HOURLY_FEATURES_SPEC.name: PER_DAY_HOURLY_FEATURES_SPEC,
    PER_HOUR_SPEC.name: PER_HOUR_SPEC,
}

DEFAULT_MODEL: str = PER_DAY_DAILY_FEATURES_SPEC.name


def _day_type_for(d: date) -> str:
    """Sun=0..Sat=6 day-type bucket. Inlined here to avoid a circular import
    between configs.py and calendar.py."""
    wd = d.weekday()  # Mon=0..Sun=6
    if wd == 5:
        return DAY_TYPE_SATURDAY
    if wd == 6:
        return DAY_TYPE_SUNDAY
    return DAY_TYPE_WEEKDAY


@dataclass
class KnnModelConfig:
    """Run-level configuration for any of the three models."""

    forecast_date: str | None = None
    model_name: str = DEFAULT_MODEL
    n_analogs: int = DEFAULT_N_ANALOGS
    quantiles: list[float] | None = None
    season_window_days: int = SEASON_WINDOW_DAYS
    min_pool_size: int = MIN_POOL_SIZE
    hub: str = HUB
    schema: str = SCHEMA

    # Calendar / day-type pre-filter knobs
    same_dow_group: bool = FILTER_SAME_DOW_GROUP
    exclude_holidays: bool = FILTER_EXCLUDE_HOLIDAYS
    exclude_dates: list[str] = field(default_factory=lambda: list(EXCLUDE_DATES))
    use_day_type_profiles: bool = True
    day_type_profiles: dict[str, dict[str, Any]] | None = None

    def resolved_target_date(self) -> date:
        if self.forecast_date:
            return date.fromisoformat(self.forecast_date)
        return date.today() + timedelta(days=1)

    def resolved_quantiles(self) -> list[float]:
        return list(self.quantiles) if self.quantiles is not None else list(QUANTILES)

    def resolved_spec(self) -> ModelSpec:
        if self.model_name not in MODEL_REGISTRY:
            raise ValueError(
                f"Unknown model '{self.model_name}'. "
                f"Available: {sorted(MODEL_REGISTRY.keys())}"
            )
        return MODEL_REGISTRY[self.model_name]

    def resolved_day_type_profiles(self) -> dict[str, dict[str, Any]]:
        """Day-type override profiles with package defaults filled in."""
        base = copy.deepcopy(DAY_TYPE_SCENARIO_PROFILES)
        if not self.day_type_profiles:
            return base
        for k, v in self.day_type_profiles.items():
            if k not in base:
                base[k] = {}
            if isinstance(v, dict):
                base[k].update(copy.deepcopy(v))
        return base

    def with_day_type_overrides(
        self, target_date: date,
    ) -> tuple["KnnModelConfig", str]:
        """Return a config copy with the Saturday/Sunday profile applied.

        Only fields that exist on this dataclass are overridden; unknown
        keys in a profile are silently ignored so profiles can carry
        forward without breaking.
        """
        day_type = _day_type_for(target_date)
        if not self.use_day_type_profiles:
            return self, day_type

        profile = self.resolved_day_type_profiles().get(day_type, {})
        if not profile:
            return self, day_type

        cfg = copy.deepcopy(self)
        for key, value in profile.items():
            if hasattr(cfg, key):
                setattr(cfg, key, copy.deepcopy(value))
        return cfg, day_type
