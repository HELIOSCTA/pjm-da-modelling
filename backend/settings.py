import os
from pathlib import Path

from dotenv import load_dotenv

# NOTE: TASKS
BASE_DIR = Path(__file__).parent

# Load .env before reading env vars below so settings are populated regardless
# of whether backend.credentials has been imported yet.
_env_file = BASE_DIR / ".env"
if _env_file.exists():
    load_dotenv(dotenv_path=_env_file, override=False)

CACHE_DIR: Path = BASE_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DBT_PROJECT_DIR = BASE_DIR / "dbt" / "dbt_azure_postgresql"

DBT_SCHEMA = "pjm_da_modelling_cleaned"

# Optional model cache publishing to Azure Blob Storage.
CACHE_BLOB_ENABLED = os.getenv("MODEL_CACHE_BLOB_ENABLED", "false").lower() in (
    "true",
    "1",
    "yes",
)
CACHE_BLOB_CONNECTION_STRING = os.getenv("MODEL_CACHE_BLOB_CONNECTION_STRING") or os.getenv(
    "AZURE_STORAGE_CONNECTION_STRING",
)
CACHE_BLOB_CONTAINER = os.getenv("MODEL_CACHE_BLOB_CONTAINER")
CACHE_BLOB_PREFIX = os.getenv("MODEL_CACHE_BLOB_PREFIX", "da-models-cache").strip("/")
CACHE_DATE = os.getenv("MODEL_CACHE_DATE")
