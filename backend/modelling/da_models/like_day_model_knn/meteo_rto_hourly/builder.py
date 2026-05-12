"""Pool and query builder for meteo_rto_hourly (thin wrapper).

Mirror of ``pjm_rto_hourly/builder.py``. Delegates to
``_shared.build_pool_from_spec`` / ``build_query_row_from_spec``, which
dispatch through ``DOMAIN_REGISTRY``. The data swap from PJM-fed to
Meteologica-fed supply/demand happens at the domain level — the spec's
``meteo_*`` domain names route to the Meteologica-aware
``FeatureDomain`` instances in ``like_day_model_knn/domains.py``, which
in turn call ``loader.load_meteologica_supply_demand_coalesced``.

The unified Meteologica coalescer is imported here so the data source
is grep-discoverable from this module — the source-of-truth for the
"this variant feeds on Meteologica" claim.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from backend.modelling.da_models.common.data.loader import load_meteologica_supply_demand_coalesced
from backend.modelling.da_models.like_day_model_knn import _shared, configs

# Re-export so callers can introspect the data source without importing
# from common.data.loader directly. Also keeps ruff from stripping the
# import as unused.
__all__ = (
    "build_pool",
    "build_query_row",
    "load_meteologica_supply_demand_coalesced",
)


def build_pool(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.METEO_RTO_HOURLY_SUNNY_ALIGNED_SPEC,
    label_source: str = configs.LABEL_SOURCE,
) -> pd.DataFrame:
    _ = schema
    return _shared.build_pool_from_spec(
        spec=spec,
        hub=hub,
        cache_dir=cache_dir,
        label_source=label_source,
    )


def build_query_row(
    target_date: date,
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.METEO_RTO_HOURLY_SUNNY_ALIGNED_SPEC,
) -> pd.DataFrame:
    _ = schema
    return _shared.build_query_row_from_spec(
        spec=spec,
        target_date=target_date,
        cache_dir=cache_dir,
    )
