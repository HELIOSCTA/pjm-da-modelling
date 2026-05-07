"""Spec registration for the regional Meteologica variant.

Defines and registers ``pjm_meteo_regional_hourly_sunny`` into the shared
``like_day_model_knn_sunny.configs.MODEL_REGISTRY``.

Apples-to-apples vs ``pjm_rto_hourly_sunny``:

  - ``regional_meteo_scalar`` (MIDATL+WEST, weights 3.0+1.5+2.0=6.5)
    REPLACES ``rto_load_scalar`` (3.0) + ``renewable_at_hour_scalar``
    (1.5) + ``rto_net_load_scalar`` (2.0) = 6.5 in the baseline spec.
  - ``load_ramps_scalar`` retained — derived from RTO load. A small
    amount of RTO-derived signal leaks into the meteo model via ramps,
    but ramps are deltas (not levels), so the level-comparison the
    hypothesis tests is preserved. Re-evaluate if results are noisy.
  - ``temperature_scalar`` retained — RTO weather. Out of scope for the
    single-day PoC; revisit with regional weather later.
"""

from __future__ import annotations

from da_models.like_day_model_knn_sunny.configs import (
    MODEL_REGISTRY,
    ModelSpec,
)


PJM_METEO_REGIONAL_HOURLY_SUNNY_SPEC = ModelSpec(
    name="pjm_meteo_regional_hourly_sunny",
    description=(
        "Regional Meteologica variant: MIDATL + WEST + SOUTH load / solar / "
        "wind / net_load (12 features in 3 distance groups) replace the "
        "RTO-aggregated supply-demand groups. Shares load_ramps_scalar / "
        "temperature_scalar / outages / gas / calendar with the RTO baseline "
        "so configured feature-group weights are identical (effective "
        "contributions differ — see _shared engine notes on dim-weighting)."
    ),
    match_unit="hour",
    domains=(
        "regional_meteo_scalar",
        "load_ramps_scalar",
        "temperature_scalar",
        "outages_scalar",
        "gas_scalar",
        "calendar_scalar",
    ),
)


MODEL_REGISTRY[PJM_METEO_REGIONAL_HOURLY_SUNNY_SPEC.name] = (
    PJM_METEO_REGIONAL_HOURLY_SUNNY_SPEC
)
