"""Meteologica-fed sibling of pjm_rto_hourly under the Sunny KNN family.

The KNN matching engine, forecast assembly, printers, and metrics are
reused verbatim from ``pjm_rto_hourly`` (re-exported below). Only the
target-date *query* differs: the pool is unchanged historical data, but
target features for D+1..D+14 come from Meteologica's latest published
regional vintage + forward-filled outages and gas, so the variant can
project a 14-day forward strip without the lead-1-only ceiling of the
PJM forecast feeds.

Temperature is deliberately dropped from the spec — the WSI hourly
weather mart runs out well before a 14-day horizon, and the user's
preference is "drop the feature group rather than carry stale values."
"""

from __future__ import annotations

from da_models.like_day_model_knn_sunny.pjm_rto_hourly import (
    engine,
    forecast,
    metrics,
    printers,
)
from da_models.like_day_model_knn_sunny.meteo_rto_hourly.builder import (
    build_horizon_query_rows,
    build_pool,
    build_query_row,
)

__all__ = [
    "build_horizon_query_rows",
    "build_pool",
    "build_query_row",
    "engine",
    "forecast",
    "metrics",
    "printers",
]
