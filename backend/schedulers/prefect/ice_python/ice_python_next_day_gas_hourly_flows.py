import logging

from prefect import flow

from backend.settings import DBT_SCHEMA
from backend.utils import azure_postgresql_utils, pipeline_run_logger, model_cache_utils

logger = logging.getLogger(__name__)

MART = "ice_python_next_day_gas_hourly"


@flow(name="ICE Python Next-Day Gas Hourly")
def ice_python_next_day_gas_hourly():
    """Hourly cache refresh — pull the ICE next-day gas hourly mart from Postgres and
    export to parquet.

    Scrape runs on the Windows Task Scheduler host (ICE XL requires the ICE add-in on
    Windows). The mart is a dbt view, so this flow only materializes the post-scrape
    cache — no dbt invocation needed.
    """
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="ice_python_next_day_gas_hourly", source="ice_python",
    )
    run.start()
    try:
        df = azure_postgresql_utils.pull_from_db(f"SELECT * FROM {DBT_SCHEMA}.{MART}")
        model_cache_utils.write_mart_cache(df, mart=MART, pipeline_name=__name__)

        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    ice_python_next_day_gas_hourly()
