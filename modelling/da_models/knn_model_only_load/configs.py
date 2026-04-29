"""Shared configuration for knn_model_only_load.

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

# ── Forecast defaults ──────────────────────────────────────────────────
DEFAULT_TARGET_DATE: date = date.today() + timedelta(days=1)
DEFAULT_N_ANALOGS: int = 20
MIN_POOL_SIZE: int = 100
SEASON_WINDOW_DAYS: int = 60

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
