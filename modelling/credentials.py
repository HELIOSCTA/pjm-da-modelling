"""Secret material for modelling runs (loaded from .env)."""
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

env_file = Path(__file__).parent / ".env"
if env_file.exists():
    logger.info("Loading %s", env_file)
    load_dotenv(dotenv_path=env_file, override=True)

# Prefer the cache-specific var; fall back to the generic string emitted by
# `az storage account show-connection-string`.
MODEL_CACHE_BLOB_CONNECTION_STRING = os.getenv("MODEL_CACHE_BLOB_CONNECTION_STRING") or os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING",
)
