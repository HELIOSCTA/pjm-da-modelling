# helioscta-pjm-da-data-scrapes

PJM day-ahead market modelling and data infrastructure. Active focus:
`modelling/da_models/like_day_model_knn` (KNN like-day analog forecaster).

## Conventions

When writing or substantially modifying a Python script (anything with
a `__main__` block or meant to be run directly), follow
`.claude/standards/python_scripts.md`. Read that file before scaffolding
a new script.

The canonical worked example is
`modelling/da_models/common/data/verify_data_loader.py`.

## Layout pointers

- `modelling/da_models/common/` — shared loaders, configs, calendar.
- `modelling/da_models/like_day_model_knn/` — current model, with
  per-variant subpackages (`per_day_daily_features/`,
  `per_day_hourly_features/`, `per_hour/`).
- `modelling/data/cache/` — parquet cache (single source per dataset key
  in `common/data/loader.py::_DEFAULT_PATTERNS`).
- `modelling/streamlit_app/` — operator console.
