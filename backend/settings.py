from pathlib import Path

# NOTE: TASKS
BASE_DIR = Path(__file__).parent

CACHE_DIR: Path = BASE_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DBT_PROJECT_DIR = BASE_DIR / "dbt" / "dbt_azure_postgresql"

DBT_SCHEMA = "pjm_da_modelling_cleaned"
