"""Cross-model historical replay + leaderboard for the DA-price models.

For each delivery date in a range, calls every registered model's
``pipelines/forecast_single_day.py::run(quiet=True)``, normalises each
family's idiosyncratic output to a single tall schema, joins to settled
DA LMP, and writes one parquet per backtest invocation -- consumed by
the leaderboard pipeline that renders per-regime point/quantile metrics
and DM-style pairwise comparison.

Layout (per the routing decision recorded in this folder):
  - ``configs.py``     -- date-range default, hub, output dir, defaults.
  - ``registry.py``    -- {model_name: ModelEntry(run_callable, adapter)}.
  - ``schemas.py``     -- canonical tall-parquet columns + builder helper.
  - ``replay.py``      -- replay_day_range(model_name, dates) -> tall DF.
  - ``metrics/``       -- per-model point + quantile scoring (DM/GW later).
  - ``regime.py``      -- minimal day-type / season classifiers.
  - ``pdc.py``         -- price-duration-curve construction.
  - ``diagnostics/``   -- family-specific checks (supply-stack fuel-mix,
                          linear-arx coef stability, ...). Stubbed in v1.
  - ``pipelines/``     -- ``run_replay`` + ``run_leaderboard`` entry points.
  - ``output/``        -- {run_id}.parquet + {run_id}_meta.json + tables.

All backtest code lives here -- no per-family ``backtest/`` folders
scattered under ``da_models/<family>/``. The research note at
``modelling/@TODO/pjm-research-for-modelling/backtest_eval_metrics.md``
is the upstream design discussion (variogram scoring, DM/GW tests,
on-peak block conventions); implementation lives in this folder.
"""
