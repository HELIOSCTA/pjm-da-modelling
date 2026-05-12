"""Pool and query builder for pjm_rto_hourly (thin wrapper).

Delegates to ``_shared.build_pool_from_spec`` / ``build_query_row_from_spec``.
The engine consumes per-HE feature columns (load_h*, solar_h*, wind_h*)
through a 3-hour window per target HE; daily-broadcast groups (outage_*,
gas_*) ride along through the engine's broadcast distance path.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from backend.modelling.da_models.like_day_model_knn import _shared, configs


def build_pool(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.PJM_RTO_HOURLY_SPEC,
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
    spec: configs.ModelSpec = configs.PJM_RTO_HOURLY_SPEC,
) -> pd.Series:
    _ = schema
    return _shared.build_query_row_from_spec(
        spec=spec,
        target_date=target_date,
        cache_dir=cache_dir,
    )
