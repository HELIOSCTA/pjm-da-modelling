"""Daily PJM DA-price forecast runs (promoted from modelling/da_models).

One task per forecaster: each calls the family's single-day ``run(...)`` with
``publish=True`` so the run lands a row in ``pjm_model_outputs.forecast_runs``
(the seam the frontend reads). The tasks are loosely coupled -- a failure in
one model does not block the others; the flow re-raises at the end if any
failed, so Prefect marks the run failed and retries surface in the UI.

Schedule (see the .yaml): once daily in the morning EPT, after the upstream
Meteologica DA-price + supply/demand marts have been rebuilt and exported to
``backend/cache/`` by their hourly deployments. A fixed time is the pragmatic
choice today; a downstream trigger off the Meteologica deployments would be
more robust and is the natural follow-up.
"""

import logging

from prefect import flow, task

from backend.utils import pipeline_run_logger

logger = logging.getLogger(__name__)

# (label, "module.path:callable") -- each callable is a family's single-day
# pipeline ``run`` and is invoked as ``run(publish=True)``.
FORECASTS: list[tuple[str, str]] = [
    (
        "baseline_meteo_da_price",
        "backend.modelling.da_models.baseline_meteo_da_price.pipelines.forecast_single_day:run",
    ),
    (
        "baseline_meteo_da_price_ice_anchored",
        "backend.modelling.da_models.baseline_meteo_da_price.pipelines.forecast_single_day_ice_anchored:run",
    ),
    (
        "like_day_knn_pjm_rto_hourly",
        "backend.modelling.da_models.like_day_model_knn.pjm_rto_hourly.pipelines.forecast_single_day:run",
    ),
    (
        "like_day_knn_meteo_rto_hourly",
        "backend.modelling.da_models.like_day_model_knn.meteo_rto_hourly.pipelines.forecast_single_day:run",
    ),
    (
        "like_day_knn_sunny_pjm_rto_hourly",
        "backend.modelling.da_models.like_day_model_knn_sunny.pjm_rto_hourly.pipelines.forecast_single_day:run",
    ),
]


def _resolve(target: str):
    module_path, _, attr = target.partition(":")
    import importlib

    return getattr(importlib.import_module(module_path), attr)


@task(name="run-forecast", retries=1)
def run_forecast(label: str, target: str) -> None:
    logger.info("Running forecaster: %s", label)
    run = _resolve(target)
    run(publish=True, quiet=True)
    logger.info("Forecaster published: %s", label)


@flow(name="PJM DA Forecasts Daily")
def pjm_da_forecasts_daily():
    """Run every promoted single-day DA-price forecaster and publish its run."""
    run = pipeline_run_logger.PipelineRunLogger(
        pipeline_name="pjm_da_forecasts_daily",
        source="modelling",
    )
    run.start()
    failures: list[str] = []
    try:
        for label, target in FORECASTS:
            try:
                run_forecast(label, target)
            except Exception as model_err:
                failures.append(label)
                logger.exception("%s forecast failed: %s", label, model_err)

        if failures:
            raise RuntimeError(
                f"Flow completed but {len(failures)} forecast(s) failed: {failures}"
            )
        run.success()
    except Exception as e:
        run.failure(error=e)
        raise


if __name__ == "__main__":
    pjm_da_forecasts_daily()
