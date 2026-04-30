"""Pool and query builder for per_hour (thin wrapper).

Delegates to ``_shared.build_pool_from_spec`` / ``build_query_row_from_spec``.
The per-hour engine consumes the resulting load_h1..load_h24 columns
through a 3-hour window per target HE; non-load groups (when the spec is
``per_hour__all``) are also broadcast through the engine.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from da_models.like_day_model_knn import _shared, configs


def build_pool(
    schema: str = configs.SCHEMA,
    hub: str = configs.HUB,
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.PER_HOUR_SPEC,
) -> pd.DataFrame:
    _ = schema
    return _shared.build_pool_from_spec(spec=spec, hub=hub, cache_dir=cache_dir)


def build_query_row(
    target_date: date,
    schema: str = configs.SCHEMA,
    cache_dir: Path | None = configs.CACHE_DIR,
    spec: configs.ModelSpec = configs.PER_HOUR_SPEC,
) -> pd.Series:
    _ = schema
    return _shared.build_query_row_from_spec(
        spec=spec, target_date=target_date, cache_dir=cache_dir,
    )
