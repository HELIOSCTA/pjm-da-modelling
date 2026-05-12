"""KNN like-day forecaster — Sunny's variant.

Parallel to ``like_day_model_knn`` but holds Sunny's per-hour scalar
implementation faithfully:

  - long-format pool (one row per (date, hour_ending))
  - scalar per-target-HE matching (no window)
  - sum-Euclidean over valid z-scored dims (no /n_valid)
  - linear pre-selection age penalty, days-based half-life
  - inverse-distance² post-selection weighting
  - joint MC quantile bands for OnPeak/OffPeak/Flat aggregates

Variant subpackage: ``pjm_rto_hourly/``.

Cross-family imports flow forward only: this package may import from
``backend.modelling.da_models.common`` only — never from ``backend.modelling.da_models.like_day_model_knn``.
"""
