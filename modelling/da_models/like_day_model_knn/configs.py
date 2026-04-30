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
PJM_DATES_DAILY_PARQUET: str = "pjm_dates_daily.parquet"
LMP_DA_PARQUET: str = "pjm_lmps_hourly.parquet"
LOAD_FORECAST_PARQUETS: list[str] = ["pjm_load_forecast_hourly_da_cutoff.parquet"]

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

# Recency controls. Both default to None (no recency adjustment) so that
# behavior is unchanged unless a config explicitly opts in.
#   - MAX_AGE_YEARS: hard cap on candidate age. Drops candidates older than
#     ``target_date - N years``. Use as a structural-break ablation; below
#     ~3 years the pool starves on weekends.
#   - RECENCY_HALF_LIFE_YEARS: soft exponential decay on the analog weight
#     post-selection. ``weight *= 0.5 ** (age_years / half_life)``.
#     Doesn't change pool composition; only how analogs blend in the forecast.
MAX_AGE_YEARS: int | None = None
RECENCY_HALF_LIFE_YEARS: float | None = None

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
    """Per-model definition. Composes one or more registered FeatureDomains.

    ``feature_groups`` and ``feature_group_weights`` are DERIVED from the
    enabled domains; weights are renormalized to sum to 1.0. ``per_hour``
    additionally uses ``flt_radius`` for the dynamic load window.
    """
    name: str
    description: str
    match_unit: str  # "day" | "hour"
    domains: tuple[str, ...]
    flt_radius: int = 0

    @property
    def feature_groups(self) -> dict[str, list[str]]:
        from da_models.like_day_model_knn.domains import resolved_feature_groups
        return resolved_feature_groups(self.domains)

    @property
    def feature_group_weights(self) -> dict[str, float]:
        from da_models.like_day_model_knn.domains import resolved_feature_group_weights
        return resolved_feature_group_weights(self.domains)


# ── Baseline (load-only) specs ─────────────────────────────────────────

PER_DAY_DAILY_FEATURES_SPEC = ModelSpec(
    name="per_day_daily_features",
    description="RTO load daily summaries x day-level matching",
    match_unit="day",
    domains=("rto_load_summary",),
)

PER_DAY_HOURLY_FEATURES_SPEC = ModelSpec(
    name="per_day_hourly_features",
    description="RTO load 24-hour profile (5 zones) x day-level matching",
    match_unit="day",
    domains=("rto_load_profile",),
)

PER_HOUR_SPEC = ModelSpec(
    name="per_hour",
    description="RTO load 3-hour window x per-hour matching (24 matches/day)",
    match_unit="hour",
    domains=("rto_load_profile",),
    flt_radius=1,
)

# ── All-domains-on specs (RTO load + renewables + outages + gas) ──────

PER_DAY_DAILY_FEATURES_ALL_SPEC = ModelSpec(
    name="per_day_daily_features__all",
    description="Daily summaries + renewables + outages + gas x day matching",
    match_unit="day",
    domains=("rto_load_summary", "renewables", "outages", "gas"),
)

PER_DAY_HOURLY_FEATURES_ALL_SPEC = ModelSpec(
    name="per_day_hourly_features__all",
    description="Hourly profile + renewables + outages + gas x day matching",
    match_unit="day",
    domains=("rto_load_profile", "renewables", "outages", "gas"),
)

PER_HOUR_ALL_SPEC = ModelSpec(
    name="per_hour__all",
    description="Load window + renewables + outages + gas x per-hour matching",
    match_unit="hour",
    domains=("rto_load_profile", "renewables", "outages", "gas"),
    flt_radius=1,
)

MODEL_REGISTRY: dict[str, ModelSpec] = {
    PER_DAY_DAILY_FEATURES_SPEC.name: PER_DAY_DAILY_FEATURES_SPEC,
    PER_DAY_HOURLY_FEATURES_SPEC.name: PER_DAY_HOURLY_FEATURES_SPEC,
    PER_HOUR_SPEC.name: PER_HOUR_SPEC,
    PER_DAY_DAILY_FEATURES_ALL_SPEC.name: PER_DAY_DAILY_FEATURES_ALL_SPEC,
    PER_DAY_HOURLY_FEATURES_ALL_SPEC.name: PER_DAY_HOURLY_FEATURES_ALL_SPEC,
    PER_HOUR_ALL_SPEC.name: PER_HOUR_ALL_SPEC,
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

    # Recency knobs (default None = unchanged behavior)
    max_age_years: int | None = MAX_AGE_YEARS
    recency_half_life_years: float | None = RECENCY_HALF_LIFE_YEARS

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
