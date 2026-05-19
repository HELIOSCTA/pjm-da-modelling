"""Shared configuration for like_day_model_knn_sunny.

Mirrors ``like_day_model_knn.configs`` in shape so backtest/sweep tooling
stays uniform, but encodes Sunny's defaults: scalar per-HE matching (no
``flt_radius``), linear days-based recency, and a run-level
``label_source`` switch for hub LMP vs RTO system-energy price.
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
LOAD_FORECAST_PARQUETS: list[str] = ["pjm_load_forecast_hourly_da_cutoff.parquet"]

# ── Forecast defaults ──────────────────────────────────────────────────
DEFAULT_TARGET_DATE: date = date.today() + timedelta(days=1)
DEFAULT_N_ANALOGS: int = 20
MIN_POOL_SIZE: int = (
    30  # smaller than daily because per-hour pool is naturally thinner (matches Sunny)
)
SEASON_WINDOW_DAYS: int = 60

# ── Calendar / day-type pre-filtering ──────────────────────────────────
# Sunny's filter ladder uses *exact* day-of-week match (not DOW groups)
# plus an ``is_weekend`` fallback. Numbering convention is Sun=0..Sat=6
# to match ``calendar.compute_sunny_calendar_row``.
#
# Both hard calendar filters default OFF: the calendar feature group
# (dow_sin, dow_cos, is_weekend) keeps DOW similarity as a soft signal
# in the distance metric. Per Sunny's empirical note, no-filter beats
# both exact-DOW and weekend-group filters on every metric (MAE, bias,
# coverage, pinball) over the 2025-04 to 2025-05 window — spike-day
# analogs need to be reachable across DOWs.
FILTER_SAME_DOW_GROUP: bool = False
FILTER_SAME_WEEKEND_GROUP: bool = False
FILTER_SAME_WEEKEND_GROUP_FOR_WEEKENDS: bool = False
FILTER_EXCLUDE_HOLIDAYS: bool = True
EXCLUDE_DATES: list[str] = []  # add YYYY-MM-DD strings to drop from the pool

# Recency knob: linear pre-selection multiplier ``d *= 1 + age_days / max(half_life, 1)``.
# Days-based and applied BEFORE the top-N selection — changes which
# analogs are picked, not just how they blend. Faithful to Sunny's
# original implementation; do not migrate to exponential half-life here.
RECENCY_HALF_LIFE_DAYS: float = 730.0  # ~2y, matches Sunny

# Optional hard age cap (mirrors yours). None = no cap.
MAX_AGE_YEARS: int | None = None

# Label source: "hub_lmp" uses the hub's total DA LMP; "system_energy"
# uses the RTO system-energy price (LMP minus congestion + loss). System
# energy is identical across hubs so the hub filter still works.
LABEL_SOURCE: str = "hub_lmp"

DAY_TYPE_WEEKDAY: str = "weekday"
DAY_TYPE_SATURDAY: str = "saturday"
DAY_TYPE_SUNDAY: str = "sunday"

# Saturday/Sunday narrow the season window and tighten DOW matching —
# mirrors the structure of like_day_model_knn but with knobs that exist
# on this package's ``KnnModelConfig``.
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

# ── Quantiles ──────────────────────────────────────────────────────────
QUANTILES: list[float] = [0.10, 0.25, 0.50, 0.75, 0.90]

# ── On/off-peak hour blocks (PJM convention, HE) ───────────────────────
ONPEAK_HOURS: list[int] = list(range(8, 24))
OFFPEAK_HOURS: list[int] = list(range(1, 8)) + [24]


# ── Per-model spec ─────────────────────────────────────────────────────


@dataclass(frozen=True)
class ModelSpec:
    """Per-model definition. Composes one or more registered FeatureDomains.

    ``feature_groups`` and ``feature_group_weights`` are DERIVED from the
    enabled domains; weights are renormalized to sum to 1.0. No
    ``flt_radius`` — Sunny's engine uses scalar features at the target HE
    only, no sliding window.
    """

    name: str
    description: str
    match_unit: str  # "hour" only for this package
    domains: tuple[str, ...]

    @property
    def feature_groups(self) -> dict[str, list[str]]:
        from da_models.like_day_model_knn_sunny.domains import resolved_feature_groups

        return resolved_feature_groups(self.domains)

    @property
    def feature_group_weights(self) -> dict[str, float]:
        from da_models.like_day_model_knn_sunny.domains import (
            resolved_feature_group_weights,
        )

        return resolved_feature_group_weights(self.domains)

    @property
    def raw_feature_group_weights(self) -> dict[str, float]:
        from da_models.like_day_model_knn_sunny.domains import (
            resolved_raw_feature_group_weights,
        )

        return resolved_raw_feature_group_weights(self.domains)


# ── Specs ──────────────────────────────────────────────────────────────

# Sunny's default: load + temp + solar + wind scalars at target HE,
# load 1h/3h ramps, plus daily-broadcast outage and gas. Net load is a
# separate optional spec because the original uses a non-identity-safe
# derivation (load - solar.fillna(0) - wind.fillna(0)) and we want both
# behaviors available without forcing one.
PJM_RTO_HOURLY_SUNNY_SPEC = ModelSpec(
    name="pjm_rto_hourly_sunny",
    description=(
        "Scalar per-HE features (load, temp, renewable, ramps, net_load) "
        "+ daily-broadcast outage / gas / calendar. Sum-Euclidean over "
        "z-scored dims; weights mirror Sunny's FEATURE_GROUP_WEIGHTS."
    ),
    match_unit="hour",
    domains=(
        "rto_load_scalar",
        "load_ramps_scalar",
        "temperature_scalar",
        "renewable_at_hour_scalar",
        "rto_net_load_scalar",
        "outages_scalar",
        "gas_scalar",
        "calendar_scalar",
    ),
)

# Ablation variant: solar and wind as separate distance groups instead
# of the combined renewable_at_hour. Useful for isolating which of the
# two renewables carries more signal.
PJM_RTO_HOURLY_SUNNY_FULL_SPEC = ModelSpec(
    name="pjm_rto_hourly_sunny_full",
    description=(
        "Ablation variant: solar and wind as separate distance groups "
        "(instead of combined renewable_at_hour). All other groups match "
        "the default sunny spec."
    ),
    match_unit="hour",
    domains=(
        "rto_load_scalar",
        "load_ramps_scalar",
        "temperature_scalar",
        "solar_scalar",
        "wind_scalar",
        "rto_net_load_scalar",
        "outages_scalar",
        "gas_scalar",
        "calendar_scalar",
    ),
)

# Meteologica-fed sibling of the default sunny spec. Uses the same scalar
# domains *minus* temperature_scalar — the Meteologica 14-day horizon
# outruns the weather-forecast mart, so the variant drops temperature
# rather than carrying a stale signal. Pool is unchanged (historical
# RT/lead-1 sources); only the target-date query is sourced from
# Meteologica latest_only + forward-filled outage / gas. See
# ``meteo_rto_hourly/`` for the variant pipelines.
METEO_RTO_HOURLY_SUNNY_SPEC = ModelSpec(
    name="meteo_rto_hourly_sunny",
    description=(
        "Sunny scalar features sourced from Meteologica's latest published "
        "regional vintage for D+1..D+14 (load + load ramps + solar + wind + "
        "net_load) plus WSI hourly weather forecast (temperature) + "
        "daily-broadcast outage / gas / calendar. Temperature relies on the "
        "WSI forecast horizon (~14 days); horizon-edge days that fall off "
        "the WSI feed leave temp_at_hour NaN and the strip flags feat_ok=False."
    ),
    match_unit="hour",
    domains=(
        "rto_load_scalar",
        "load_ramps_scalar",
        "temperature_scalar",
        "renewable_at_hour_scalar",
        "rto_net_load_scalar",
        "outages_scalar",
        "gas_scalar",
        "calendar_scalar",
    ),
)

MODEL_REGISTRY: dict[str, ModelSpec] = {
    PJM_RTO_HOURLY_SUNNY_SPEC.name: PJM_RTO_HOURLY_SUNNY_SPEC,
    PJM_RTO_HOURLY_SUNNY_FULL_SPEC.name: PJM_RTO_HOURLY_SUNNY_FULL_SPEC,
    METEO_RTO_HOURLY_SUNNY_SPEC.name: METEO_RTO_HOURLY_SUNNY_SPEC,
}

DEFAULT_MODEL: str = PJM_RTO_HOURLY_SUNNY_SPEC.name


def _day_type_for(d: date) -> str:
    """Sun=0..Sat=6 day-type bucket. Inlined to avoid a circular import
    between configs.py and calendar.py."""
    wd = d.weekday()  # Mon=0..Sun=6 (Python)
    if wd == 5:
        return DAY_TYPE_SATURDAY
    if wd == 6:
        return DAY_TYPE_SUNDAY
    return DAY_TYPE_WEEKDAY


@dataclass
class KnnModelConfig:
    """Run-level configuration for the Sunny variant."""

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
    same_weekend_group: bool = FILTER_SAME_WEEKEND_GROUP
    same_weekend_group_for_weekends: bool = FILTER_SAME_WEEKEND_GROUP_FOR_WEEKENDS
    exclude_holidays: bool = FILTER_EXCLUDE_HOLIDAYS
    exclude_dates: list[str] = field(default_factory=lambda: list(EXCLUDE_DATES))
    use_day_type_profiles: bool = True
    day_type_profiles: dict[str, dict[str, Any]] | None = None

    # Recency / age
    max_age_years: int | None = MAX_AGE_YEARS
    recency_half_life_days: float = RECENCY_HALF_LIFE_DAYS

    # Label
    label_source: str = LABEL_SOURCE

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
        self,
        target_date: date,
    ) -> tuple["KnnModelConfig", str]:
        """Return a config copy with the Saturday/Sunday profile applied."""
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
