"""Download modelling cache parquet files from Azure Blob.

Reads non-secret config from modelling/settings.py and the connection string
from modelling/secrets.py. Uses ``importlib`` rather than putting modelling/
on ``sys.path`` — that would shadow the stdlib ``secrets`` module for any code
running in the same process.

Usage:
    python modelling/data/download.py                    # today (ET)
    python modelling/data/download.py --date 2026-04-24
    python modelling/data/download.py --date 2026-04-24 --force
"""
from __future__ import annotations

import argparse
import importlib.util
import logging
from datetime import datetime
from pathlib import Path
from types import ModuleType

logger = logging.getLogger(__name__)

_MODELLING_ROOT = Path(__file__).resolve().parent.parent


def _load_module(module_name: str, file_path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load {module_name} from {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


settings = _load_module("modelling_settings", _MODELLING_ROOT / "settings.py")
modelling_secrets = _load_module("modelling_secrets", _MODELLING_ROOT / "secrets.py")


def _default_date() -> str:
    if settings.MODEL_CACHE_DATE:
        return settings.MODEL_CACHE_DATE
    return datetime.now(settings.MARKET_TZ).strftime("%Y-%m-%d")


def _blob_container_client():
    from azure.storage.blob import BlobServiceClient

    conn = modelling_secrets.MODEL_CACHE_BLOB_CONNECTION_STRING
    if not conn:
        raise RuntimeError(
            "Connection string not set. Populate MODEL_CACHE_BLOB_CONNECTION_STRING "
            "or AZURE_STORAGE_CONNECTION_STRING in modelling/.env",
        )
    if not settings.MODEL_CACHE_BLOB_CONTAINER:
        raise RuntimeError("MODEL_CACHE_BLOB_CONTAINER is not set in modelling/.env")

    service = BlobServiceClient.from_connection_string(conn)
    return service.get_container_client(settings.MODEL_CACHE_BLOB_CONTAINER)


def download_all(
    *,
    date: str | None = None,
    cache_dir: Path | None = None,
    force: bool = False,
) -> list[Path]:
    """Download every parquet under ``<prefix>/dates/<date>/`` into cache_dir.

    Existing files are skipped unless ``force=True``. Raises FileNotFoundError
    if the date prefix has no blobs — almost always means the publisher hasn't
    run yet for that date.
    """
    date = date or _default_date()
    target_dir = cache_dir or settings.CACHE_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    container = _blob_container_client()
    blob_prefix = f"{settings.MODEL_CACHE_BLOB_PREFIX}/dates/{date}/"

    downloaded: list[Path] = []
    skipped = 0
    listed = 0
    for blob in container.list_blobs(name_starts_with=blob_prefix):
        listed += 1
        file_name = blob.name.rsplit("/", 1)[-1]
        if not file_name:
            continue
        local_path = target_dir / file_name
        if local_path.exists() and not force:
            logger.info("Skip (exists): %s", local_path)
            skipped += 1
            continue
        logger.info("Downloading %s -> %s (%s bytes)", blob.name, local_path, blob.size)
        with local_path.open("wb") as out:
            container.download_blob(blob).readinto(out)
        downloaded.append(local_path)

    if listed == 0:
        raise FileNotFoundError(
            f"No blobs under {settings.MODEL_CACHE_BLOB_CONTAINER}/{blob_prefix} - "
            f"check the date and that Prefect has published for {date}",
        )

    logger.info(
        "Downloaded %d, skipped %d, listed %d from %s/%s",
        len(downloaded),
        skipped,
        listed,
        settings.MODEL_CACHE_BLOB_CONTAINER,
        blob_prefix,
    )
    return downloaded


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download modelling cache parquet files from Azure Blob",
    )
    parser.add_argument("--date", help="YYYY-MM-DD (defaults to today ET)")
    parser.add_argument(
        "--force", action="store_true", help="Re-download even if local file already exists",
    )
    parser.add_argument("--cache-dir", help="Override target directory")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Azure SDK logs every HTTP request at INFO; quiet it.
    logging.getLogger("azure").setLevel(logging.WARNING)

    cache_dir = Path(args.cache_dir).expanduser() if args.cache_dir else None
    download_all(date=args.date, cache_dir=cache_dir, force=args.force)


if __name__ == "__main__":
    main()
