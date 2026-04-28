"""Configuration for forward-only KNN DA LMP forecasting."""
from __future__ import annotations

import copy
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from da_models.common.configs import CACHE_DIR as DEFAULT_SHARED_CACHE_DIR

# Database
SCHEMA: str = "pjm_cleaned"
HUB: str = "WESTERN HUB"
LOAD_REGION: str = "RTO"
LOAD_REGIONS: list[str] = ["RTO", "MIDATL", "WEST", "SOUTH"]


def _per_region(*metrics: str, prefix: str = "") -> list[str]:
    """Expand metric names to per-region feature columns: <prefix><metric>_<region_lower>."""
    return [f"{prefix}{m}_{r.lower()}" for m in metrics for r in LOAD_REGIONS]


# Cache
_DEFAULT_CACHE_DIR: Path = DEFAULT_SHARED_CACHE_DIR
CACHE_DIR: Path = Path(os.getenv("DA_MODELS_CACHE_DIR", str(_DEFAULT_CACHE_DIR)))
CACHE_ENABLED: bool = os.getenv("CACHE_ENABLED", "true").lower() in ("true", "1", "yes")
CACHE_TTL_HOURS: float = float(os.getenv("CACHE_TTL_HOURS", "4"))
FORCE_CACHE_REFRESH: bool = os.getenv("FORCE_CACHE_REFRESH", "false").lower() in ("true", "1", "yes")

# Forecast defaults
DEFAULT_TARGET_DATE: date = date.today() + timedelta(days=1)
DEFAULT_N_ANALOGS: int = 20
MIN_POOL_SIZE: int = 150

# Hours / quantiles
HOURS: list[int] = list(range(1, 25))
QUANTILES: list[float] = [0.10, 0.25, 0.50, 0.75, 0.90]

# Calendar groups use Sun=0 .. Sat=6 convention.
DOW_GROUPS: dict[str, list[int]] = {
    "weekday": [1, 2, 3, 4, 5],
    "saturday": [6],
    "sunday": [0],
}

# Distance feature groups.
#
# Forward-only by design: every group describes the TARGET delivery day's
# conditions (forecasts on the query side, actuals on the analog side).
# Backward-looking reference-day features are deliberately excluded — see
# modelling/@TODO/pjm-research-for-modelling/backward_vs_forward_looking.md
# for why anchoring to yesterday's signals causes regime-change misses.
FEATURE_GROUPS: dict[str, list[str]] = {
    "load_level": _per_region(
        "load_daily_avg",
        "load_daily_peak",
        "load_daily_valley",
    ),
    "load_ramps": _per_region(
        "load_morning_ramp",
        "load_evening_ramp",
        "load_ramp_max",
    ),
    "gas_level": [
        "gas_m3_daily_avg",
        "gas_tco_daily_avg",
        "gas_tz6_daily_avg",
        "gas_dom_south_daily_avg",
    ],
    "outage_level": [
        "outage_total_mw",
        "outage_forced_mw",
        "outage_forced_share",
    ],
    "renewable_level": _per_region(
        "solar_daily_avg",
        "wind_daily_avg",
        "renewable_daily_avg",
    ),
    "net_load": _per_region(
        "net_load_daily_avg",
        "net_load_daily_peak",
        "net_load_daily_valley",
        "net_load_morning_ramp",
        "net_load_evening_ramp",
    ),
    "calendar_dow": [
        "is_weekend",
        "dow_sin",
        "dow_cos",
    ],
}

FEATURE_GROUP_WEIGHTS: dict[str, float] = {
    "load_level":      3.0,
    "load_ramps":      1.0,
    "gas_level":       2.0,
    "outage_level":    2.0,
    "renewable_level": 1.5,
    "net_load":        2.0,
    "calendar_dow":    1.0,
}

# Filtering
FILTER_SAME_DOW_GROUP: bool = True
FILTER_EXCLUDE_HOLIDAYS: bool = True
FILTER_SEASON_WINDOW_DAYS: int = 60

# Outage regime filter — drops candidates whose outage_total_mw z-score is
# more than `tolerance_std` away from the target's z-score. Mirrors the old
# helioscta-pjm-da `outage_regime_filter`.
FILTER_OUTAGE_REGIME: bool = True
FILTER_OUTAGE_TOLERANCE_STD: float = 1.5
FILTER_OUTAGE_COL: str = "outage_total_mw"

# Recency
RECENCY_HALF_LIFE_DAYS: int = 730

# Horizon feature gating
GAS_FEATURE_MAX_HORIZON_DAYS: int = 1
OUTAGE_FEATURE_MAX_HORIZON_DAYS: int = 7
RENEWABLE_FEATURE_MAX_HORIZON_DAYS: int = 7
NET_LOAD_FEATURE_MAX_HORIZON_DAYS: int = 7

# Labels
LMP_LABEL_COLUMNS: list[str] = [f"lmp_h{h}" for h in HOURS]


# ── Day-type profiles ──────────────────────────────────────────────────
# Per-DOW overrides applied via `ForwardOnlyKNNConfig.with_day_type_overrides`.
# Each profile may override `feature_group_weights` (shallow-merged onto the
# base weights), `n_analogs`, `season_window_days`, `same_dow_group`,
# `recency_half_life_days`. Saturday/Sunday tighten on flatter shapes and
# concentrate analog weight on the smaller candidate pool.
DAY_TYPE_PROFILES: dict[str, dict[str, Any]] = {
    "weekday": {},
    "saturday": {
        "season_window_days": 45,
        "n_analogs": 12,
        "feature_group_weights": {
            # Flatter shape → ramps less informative.
            "load_ramps":      0.5,
            "net_load":        2.5,   # peak net load still drives weekend pricing
            "renewable_level": 2.0,   # solar share is a bigger fraction on weekends
            "calendar_dow":    0.25,  # already filtered on DOW group
        },
    },
    "sunday": {
        "season_window_days": 60,
        "n_analogs": 10,             # fewer Sundays in pool — concentrate weight
        "feature_group_weights": {
            "load_ramps":      0.25,
            "net_load":        2.5,
            "renewable_level": 2.5,
            "calendar_dow":    0.25,
        },
    },
}


def resolved_feature_columns(feature_weights: dict[str, float]) -> list[str]:
    """Return unique feature columns with positive weight."""
    cols: list[str] = []
    for group_name, weight in feature_weights.items():
        if weight <= 0:
            continue
        for col in FEATURE_GROUPS.get(group_name, []):
            if col not in cols:
                cols.append(col)
    return cols


def _dow_key_for(target_date: date) -> str:
    """Map a date to a DAY_TYPE_PROFILES key (Sun=0..Sat=6)."""
    dow_num = (target_date.weekday() + 1) % 7
    if dow_num == 0:
        return "sunday"
    if dow_num == 6:
        return "saturday"
    return "weekday"


@dataclass
class ForwardOnlyKNNConfig:
    """Run-level configuration for forward-only KNN."""

    forecast_date: str | None = None
    n_analogs: int = DEFAULT_N_ANALOGS
    quantiles: list[float] | None = None
    feature_group_weights: dict[str, float] | None = None
    min_pool_size: int = MIN_POOL_SIZE
    same_dow_group: bool = FILTER_SAME_DOW_GROUP
    exclude_holidays: bool = FILTER_EXCLUDE_HOLIDAYS
    season_window_days: int = FILTER_SEASON_WINDOW_DAYS
    recency_half_life_days: int = RECENCY_HALF_LIFE_DAYS
    gas_feature_max_horizon_days: int = GAS_FEATURE_MAX_HORIZON_DAYS
    weight_method: str = "inverse_distance"
    schema: str = SCHEMA
    hub: str = HUB
    outage_feature_max_horizon_days: int = OUTAGE_FEATURE_MAX_HORIZON_DAYS
    renewable_feature_max_horizon_days: int = RENEWABLE_FEATURE_MAX_HORIZON_DAYS
    net_load_feature_max_horizon_days: int = NET_LOAD_FEATURE_MAX_HORIZON_DAYS

    # Day-type profile overrides
    use_day_type_profiles: bool = True
    day_type_profiles: dict[str, dict[str, Any]] | None = None

    # Outage regime filter
    apply_outage_regime_filter: bool = FILTER_OUTAGE_REGIME
    outage_tolerance_std: float = FILTER_OUTAGE_TOLERANCE_STD
    outage_filter_col: str = FILTER_OUTAGE_COL

    def resolved_target_date(self) -> date:
        """Forecast date with tomorrow fallback."""
        if self.forecast_date:
            return date.fromisoformat(self.forecast_date)
        return DEFAULT_TARGET_DATE

    def resolved_quantiles(self) -> list[float]:
        """Quantile list with defaults."""
        return list(self.quantiles) if self.quantiles is not None else list(QUANTILES)

    def resolved_feature_weights(
        self,
        include_gas: bool = True,
        include_outages: bool = True,
        include_renewables: bool = True,
        include_net_load: bool = True,
    ) -> dict[str, float]:
        """Feature-group weights after optional horizon gating."""
        weights = copy.deepcopy(self.feature_group_weights or FEATURE_GROUP_WEIGHTS)
        if not include_gas and "gas_level" in weights:
            weights["gas_level"] = 0.0
        if not include_outages and "outage_level" in weights:
            weights["outage_level"] = 0.0
        if not include_renewables and "renewable_level" in weights:
            weights["renewable_level"] = 0.0
        if not include_net_load and "net_load" in weights:
            weights["net_load"] = 0.0
        return weights

    def resolved_day_type_profiles(self) -> dict[str, dict[str, Any]]:
        """Return day-type override profiles with module defaults filled in."""
        base = copy.deepcopy(DAY_TYPE_PROFILES)
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
    ) -> tuple["ForwardOnlyKNNConfig", str]:
        """Return (config, day_type) with weekend/weekday profile applied."""
        day_type = _dow_key_for(target_date)
        if not self.use_day_type_profiles:
            return self, day_type

        profile = self.resolved_day_type_profiles().get(day_type, {})
        if not profile:
            return self, day_type

        cfg = copy.deepcopy(self)
        if "feature_group_weights" in profile:
            base = dict(cfg.feature_group_weights or FEATURE_GROUP_WEIGHTS)
            base.update(copy.deepcopy(profile["feature_group_weights"]))
            cfg.feature_group_weights = base

        for key in (
            "n_analogs",
            "season_window_days",
            "same_dow_group",
            "recency_half_life_days",
        ):
            if key in profile:
                setattr(cfg, key, profile[key])

        return cfg, day_type
