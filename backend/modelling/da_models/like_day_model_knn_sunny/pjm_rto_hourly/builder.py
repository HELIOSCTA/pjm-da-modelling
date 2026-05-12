"""Pool and query builder for pjm_rto_hourly (Sunny variant) — thin wrapper.

Delegates to ``_shared.build_pool_from_spec`` / ``build_query_row_from_spec``
which produce long-format ``(date, hour_ending, ...)`` frames.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from backend.modelling.da_models.like_day_model_knn_sunny import _shared, configs


def build_pool(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    label_source: str = configs.LABEL_SOURCE,
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.PJM_RTO_HOURLY_SUNNY_SPEC,
) -> pd.DataFrame:
    _ = schema
    return _shared.build_pool_from_spec(
        spec=spec,
        hub=hub,
        label_source=label_source,
        cache_dir=cache_dir,
    )


def build_query_row(
    target_date: date,
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.PJM_RTO_HOURLY_SUNNY_SPEC,
) -> pd.DataFrame:
    _ = schema
    return _shared.build_query_row_from_spec(
        spec=spec,
        target_date=target_date,
        cache_dir=cache_dir,
    )
