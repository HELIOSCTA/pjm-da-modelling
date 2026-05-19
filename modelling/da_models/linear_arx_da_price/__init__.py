"""LEAR-style linear ARX day-ahead price forecaster (Western Hub).

Research / standalone family: 24 independent per-hour ridge regressions
on a forward-fundamental feature matrix, target in asinh space, with
exponential recency weighting. Probabilistic bands come from a
rolling-holdout empirical residual quantile. See
``modelling/@TODO/pjm-research-for-modelling/linear_regression_model.md``
for the design memo and the Tier-2 roadmap (quantile regression,
conformal post-processing, multi-window calibration).

Nothing here writes Postgres — publishing to ``pjm_model_outputs.forecast_runs``
is owned by the scheduled twin under ``backend/modelling/da_models/``.
"""
