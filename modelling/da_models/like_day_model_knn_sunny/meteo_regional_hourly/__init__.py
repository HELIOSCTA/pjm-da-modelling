"""Regional Meteologica variant of the sunny KNN like-day forecaster.

Replaces RTO-aggregated load/solar/wind/net_load matching features with
Meteologica regional forecasts for all three Meteologica-covered sub-
zones: MIDATL + WEST + SOUTH.

Importing this package registers a single new feature domain
(``regional_meteo_scalar``) into the shared
``like_day_model_knn_sunny.domains.DOMAIN_REGISTRY`` and a single new
``ModelSpec`` (``pjm_meteo_regional_hourly_sunny``) into
``like_day_model_knn_sunny.configs.MODEL_REGISTRY`` — both as side
effects of module load. The variant deliberately reuses the
``pjm_rto_hourly`` engine / forecast / metrics / printers modules
unchanged because they are domain-agnostic.
"""

from __future__ import annotations

from da_models.like_day_model_knn_sunny.meteo_regional_hourly import (  # noqa: F401
    configs as _configs,
    domains as _domains,
)
