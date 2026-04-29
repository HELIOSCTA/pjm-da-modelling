"""Three KNN load-only DA LMP forecast models for ablation testing.

Each model lives in its own subfolder with a dedicated builder, engine,
forecast, and single_day backtest:

  per_day_daily_features/  - daily summary features (6)  x day-level matching
  per_day_hourly_features/ - hourly bucketed features    x day-level matching
  per_hour/                - 3-hour window features      x per-hour matching

Shared values (constants, per-model ``ModelSpec`` registry) live in configs.py.
Shared parquet loaders + LMP-label pivoting live in _shared.py.
Shared dashboard figure helpers live in diagnostics_common.py.
"""
