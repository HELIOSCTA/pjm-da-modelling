"""Helpers for publishing scheduler outputs to the shared modelling cache."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from backend import settings

logger = logging.getLogger(__name__)

MARKET_TZ = ZoneInfo("America/New_York")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _cache_date() -> str:
    if settings.CACHE_DATE:
        return settings.CACHE_DATE
    return datetime.now(MARKET_TZ).strftime("%Y-%m-%d")


def _blob_path(*parts: str) -> str:
    cleaned = [part.strip("/") for part in parts if part and part.strip("/")]
    return "/".join(cleaned)


def _upload_blob(blob_name: str, file_path: Path) -> None:
    from azure.storage.blob import BlobServiceClient

    if not settings.CACHE_BLOB_CONNECTION_STRING:
        raise RuntimeError("MODEL_CACHE_BLOB_CONNECTION_STRING or AZURE_STORAGE_CONNECTION_STRING is not set")
    if not settings.CACHE_BLOB_CONTAINER:
        raise RuntimeError("MODEL_CACHE_BLOB_CONTAINER is not set")

    client = BlobServiceClient.from_connection_string(settings.CACHE_BLOB_CONNECTION_STRING)
    blob_client = client.get_blob_client(
        container=settings.CACHE_BLOB_CONTAINER,
        blob=blob_name,
    )
    with file_path.open("rb") as stream:
        blob_client.upload_blob(stream, overwrite=True)


def write_model_cache(
    df: pd.DataFrame,
    *,
    dataset_name: str,
    pipeline_name: str,
    source_relation: str,
    cache_dir: Path = settings.CACHE_DIR,
) -> Path:
    """Write a dataset locally and optionally publish it to Azure Blob.

    Blob layout is intentionally simple:
      dates/<YYYY-MM-DD>/<dataset>.parquet

    Date folder is the current market date (America/New_York), or
    MODEL_CACHE_DATE if set. Blob upload errors propagate and fail the
    Prefect flow by design: downstream modelling reads from blob, so a
    missed publish is a pipeline failure, not a warning.
    """
    published_at = _utc_now()
    cache_date = _cache_date()

    cache_dir.mkdir(parents=True, exist_ok=True)
    local_path = cache_dir / f"{dataset_name}.parquet"

    df.to_parquet(local_path, index=False)
    logger.info(
        "Wrote %s rows to %s from %s via %s at %s",
        f"{len(df):,}",
        local_path,
        source_relation,
        pipeline_name,
        published_at.isoformat(),
    )

    mirror_dir = settings.MODELLING_CACHE_DIR
    mirror_dir.mkdir(parents=True, exist_ok=True)
    mirror_path = mirror_dir / f"{dataset_name}.parquet"
    df.to_parquet(mirror_path, index=False)
    logger.info("Mirrored %s to %s", dataset_name, mirror_path)

    if not settings.CACHE_BLOB_ENABLED:
        return local_path

    if not settings.CACHE_BLOB_CONNECTION_STRING or not settings.CACHE_BLOB_CONTAINER:
        raise RuntimeError(
            "MODEL_CACHE_BLOB_ENABLED is true, but MODEL_CACHE_BLOB_CONNECTION_STRING / "
            "AZURE_STORAGE_CONNECTION_STRING or MODEL_CACHE_BLOB_CONTAINER is not set",
        )

    dated_data_blob = _blob_path(
        settings.CACHE_BLOB_PREFIX,
        "dates",
        cache_date,
        f"{dataset_name}.parquet",
    )
    _upload_blob(dated_data_blob, local_path)

    logger.info(
        "Published model cache dataset=%s date=%s container=%s prefix=%s",
        dataset_name,
        cache_date,
        settings.CACHE_BLOB_CONTAINER,
        settings.CACHE_BLOB_PREFIX,
    )
    return local_path


def write_mart_cache(
    df: pd.DataFrame,
    *,
    mart: str,
    pipeline_name: str,
    schema: str = settings.DBT_SCHEMA,
    cache_dir: Path = settings.CACHE_DIR,
) -> Path:
    """Write a dbt mart to the modelling cache."""
    return write_model_cache(
        df,
        dataset_name=mart,
        pipeline_name=pipeline_name,
        source_relation=f"{schema}.{mart}",
        cache_dir=cache_dir,
    )
