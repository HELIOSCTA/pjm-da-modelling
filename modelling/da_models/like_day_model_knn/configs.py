"""Shared configuration for like_day_model_knn.

The model lives in ``pjm_rto_hourly/`` with its own builder, engine,
forecast, and single_day backtest:

  pjm_rto_hourly/           - 3-hour window per target HE       x per-hour matching

This module owns ONLY shared values and the ``ModelSpec`` registry.
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
LOAD_FORECAST_PARQUETS: list[str] = ["pjm_load_forecast_hourly_da_cutoff.parquet"]

# ── Forecast defaults ──────────────────────────────────────────────────
# Defaults aligned with ``like_day_model_knn_sunny`` and the
# ``pjm_dashboard_handoff`` production config: smaller min-pool floor
# (per-HE matching produces a thinner candidate set), no hard age cap
# (recency handled via the linear pre-selection penalty below), DOW
# filtering OFF by default (calendar similarity stays in the distance
# metric as a soft signal — the no-filter setup beat exact-DOW and
# weekend-group filters across MAE/bias/coverage/pinball on the
# 2025-04 to 2025-05 window).
DEFAULT_TARGET_DATE: date = date.today() + timedelta(days=1)
DEFAULT_N_ANALOGS: int = 20
MIN_POOL_SIZE: int = 30
SEASON_WINDOW_DAYS: int = 60

# Label source for the KNN regression target.
#   "hub_lmp"        -> total LMP at HUB (default; current behavior)
#   "system_energy"  -> RTO system-energy price (LMP minus congestion + loss)
# System energy is identical across hubs for a given hour so the choice
# of ``hub`` is irrelevant when this is set; we keep filtering for safety.
# NOTE: plumbing into the loader/builder is staged — adding the constant
# here gives the configuration surface; loader support comes next.
LABEL_SOURCE: str = "hub_lmp"

# ── Calendar / day-type pre-filtering ──────────────────────────────────
# Sun=0..Sat=6 numbering matches pjm_dates_daily.day_of_week_number.
# DOW_GROUPS is retained for back-compat / explicit opt-in — production
# default below is ``FILTER_SAME_DOW_GROUP=False`` (sunny parity).
DOW_GROUPS: dict[str, list[int]] = {
    "early_week": [1, 2, 3],  # Mon, Tue, Wed
    "late_week": [4, 5],  # Thu, Fri (structural Thu/Fri price premium)
    "saturday": [6],
    "sunday": [0],
}

DAY_TYPE_WEEKDAY: str = "weekday"
DAY_TYPE_SATURDAY: str = "saturday"
DAY_TYPE_SUNDAY: str = "sunday"

FILTER_SAME_DOW_GROUP: bool = False
FILTER_SAME_WEEKEND_GROUP: bool = False
FILTER_SAME_WEEKEND_GROUP_FOR_WEEKENDS: bool = False
FILTER_EXCLUDE_HOLIDAYS: bool = True
EXCLUDE_DATES: list[str] = []  # add YYYY-MM-DD strings to drop from the pool

# Recency control: linear pre-selection multiplier
# ``distance *= 1 + age_days / max(half_life_days, 1)``.
# Days-based and applied BEFORE top-N selection — older candidates get
# penalized distances and so are less likely to be picked at all
# (matches sunny's ``calendar.linear_age_penalty``). The previous
# exponential post-selection weight decay (``RECENCY_HALF_LIFE_YEARS``)
# is removed: it changed only how analogs blend, not which were
# selected, which under-weighted the recency signal.
RECENCY_HALF_LIFE_DAYS: float = 730.0  # ~2y, matches sunny

# Optional hard age cap. ``None`` = no cap (default; recency handled by
# the linear penalty above). Set to an integer to drop anything older.
MAX_AGE_YEARS: int | None = None

# Saturday/Sunday narrow the season window and reduce K. The sat/sun
# profiles previously also enabled ``same_dow_group`` — now switched to
# ``same_weekend_group_for_weekends`` for sunny parity.
DAY_TYPE_SCENARIO_PROFILES: dict[str, dict[str, Any]] = {
    DAY_TYPE_WEEKDAY: {},
    DAY_TYPE_SATURDAY: {
        "same_weekend_group_for_weekends": True,
        "season_window_days": 45,
        "n_analogs": 12,
    },
    DAY_TYPE_SUNDAY: {
        "same_weekend_group_for_weekends": True,
        "season_window_days": 60,
        "n_analogs": 10,
    },
}

# ── Quantiles ──────────────────────────────────────────────────────────
QUANTILES: list[float] = [0.10, 0.25, 0.50, 0.75, 0.90]


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

    @property
    def raw_feature_group_weights(self) -> dict[str, float]:
        from da_models.like_day_model_knn.domains import (
            resolved_raw_feature_group_weights,
        )

        return resolved_raw_feature_group_weights(self.domains)


# ── Specs ──────────────────────────────────────────────────────────────

# Load + solar + wind on a 3-hour per-HE window; outages and gas as
# daily-broadcast date-level filters. Solar/wind go through the engine's
# dynamic-window path (engine.py treats load_/solar_/wind_ group prefixes
# as windowed); outages/gas go through the broadcast path. Pool spans
# 2010+ (load) but solar/wind only fill ~2019+ — pool merge in
# _shared.build_pool_from_spec uses outer-join so older dates compete on
# load alone via the engine's NaN-aware RMS-z.
PJM_RTO_HOURLY_SPEC = ModelSpec(
    name="pjm_rto_hourly",
    description="Load + solar + wind per-HE levels; outages + M3 gas as daily filters.",
    match_unit="hour",
    domains=(
        "rto_load_profile",
        "solar_profile",
        "wind_profile",
        "outages_level",
        "gas_level",
    ),
    flt_radius=1,
)

# All six features (the five above plus net_load) for ablation experiments.
# Net_load reads from the unified supply-demand coalescer, so the identity
# `net_load = load - solar - wind` holds within each pool row by
# construction. Default weights mirror rto_load_profile so net_load and the
# component triple contribute equally — natural baseline for Method-B
# ablation backtests where weight overrides isolate each feature in turn.
# For production forecasts continue to use ``pjm_rto_hourly`` (the 5-feature
# spec) until ablation results justify a default change.
PJM_RTO_HOURLY_FULL_SPEC = ModelSpec(
    name="pjm_rto_hourly_full",
    description=(
        "Load + solar + wind + net_load per-HE levels; outages + M3 gas as "
        "daily filters. For ablation experiments — net_load alongside its "
        "components."
    ),
    match_unit="hour",
    domains=(
        "rto_load_profile",
        "solar_profile",
        "wind_profile",
        "rto_net_load_profile",
        "outages_level",
        "gas_level",
    ),
    flt_radius=1,
)

# Sunny-aligned spec: adds load ramps (1h, 3h), temperature, and the
# soft calendar features (dow_sin, dow_cos, is_weekend). Mirrors the
# feature set of like_day_model_knn_sunny's PJM_RTO_HOURLY_SUNNY_SPEC,
# but kept on this package's per-HE windowed Euclidean engine. Use this
# spec for ablation/comparison against ``pjm_rto_hourly_full``.
PJM_RTO_HOURLY_SUNNY_ALIGNED_SPEC = ModelSpec(
    name="pjm_rto_hourly_sunny_aligned",
    description=(
        "Sunny feature set on the per-HE engine. With ``flt_radius=0``, "
        "windowed groups match scalar at the target HE only — sunny-equivalent. "
        "Domains: load + load_ramps (combined 1h+3h) + renewable (combined "
        "solar+wind) + net_load + temperature (windowed scalar) + outage "
        "(total only) + gas + calendar (broadcast)."
    ),
    match_unit="hour",
    domains=(
        "rto_load_profile",
        "load_ramps_profile",
        "renewable_profile",
        "rto_net_load_profile",
        "temperature_profile",
        "outages_level",
        "gas_level",
        "calendar_level",
    ),
    # flt_radius=0 makes the windowed Euclidean degenerate to a scalar
    # match at the target HE (sunny-equivalent). Bump to 1 or 3 if the
    # neighbor-hour smoothing earns its keep on a backtest.
    flt_radius=0,
)

MODEL_REGISTRY: dict[str, ModelSpec] = {
    PJM_RTO_HOURLY_SPEC.name: PJM_RTO_HOURLY_SPEC,
    PJM_RTO_HOURLY_FULL_SPEC.name: PJM_RTO_HOURLY_FULL_SPEC,
    PJM_RTO_HOURLY_SUNNY_ALIGNED_SPEC.name: PJM_RTO_HOURLY_SUNNY_ALIGNED_SPEC,
}

DEFAULT_MODEL: str = PJM_RTO_HOURLY_SPEC.name


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
    same_weekend_group: bool = FILTER_SAME_WEEKEND_GROUP
    same_weekend_group_for_weekends: bool = FILTER_SAME_WEEKEND_GROUP_FOR_WEEKENDS
    exclude_holidays: bool = FILTER_EXCLUDE_HOLIDAYS
    exclude_dates: list[str] = field(default_factory=lambda: list(EXCLUDE_DATES))
    use_day_type_profiles: bool = True
    day_type_profiles: dict[str, dict[str, Any]] | None = None

    # Recency knobs
    max_age_years: int | None = MAX_AGE_YEARS
    recency_half_life_days: float = RECENCY_HALF_LIFE_DAYS

    # Label source: "hub_lmp" or "system_energy". See LABEL_SOURCE doc.
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
        self,
        target_date: date,
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
