"""KNN load-only DA LMP forecast model.

The model lives in its own subfolder with a dedicated builder, engine,
forecast, and single_day backtest:

  per_hour/                - 3-hour window features      x per-hour matching

Shared values (constants, per-model ``ModelSpec`` registry) live in configs.py.
Shared parquet loaders + LMP-label pivoting live in _shared.py.
Shared dashboard figure helpers live in diagnostics_common.py.
"""
