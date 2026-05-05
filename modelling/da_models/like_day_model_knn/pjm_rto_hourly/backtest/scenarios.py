"""Shared scenario definitions for the pjm_rto_hourly backtest harnesses.

Both ``single_day_backtest.py`` and ``param_sweep.py`` import the
``SCENARIOS`` dict from here so weight/knob hypotheses are defined in
one place. Edit this file to add, remove, or tune scenarios; both
backtest entry points pick up the change on their next run.

Each scenario:

    "<name>": {
        "weights": {group: raw_weight, ...} | None,    # None -> spec defaults
        "overrides": {run_kwarg: value, ...},          # any forecast_run() kwarg
    }

Weights are RAW multipliers — renormalized to sum=1.0 inside the engine.
Pass ``None`` to use the spec's default weights. ``overrides`` can carry
any kwarg accepted by ``forecast_single_day.run()`` — most usefully:

    flt_radius          (default 1)  — half-width of the per-HE window
    n_analogs           (default 20)
    season_window_days  (default 60)
    min_pool_size       (default 100)

Valid weight keys (from ``PJM_RTO_HOURLY_SPEC.feature_groups``):

    load_level, solar_level, wind_level, outage_level, gas_level

The five ``load_*`` time-of-day sub-groups were collapsed into a single
``load_level`` group — per-HE windowed matching already localizes the
match, so spec-side bucketing was redundant.

Unknown weight keys raise ``ValueError`` with the valid-keys list, so
typos surface immediately rather than silently zeroing a group.

To override scenarios for a single run without editing this file:

    from da_models.like_day_model_knn.pjm_rto_hourly.backtest import (
        single_day_backtest, scenarios,
    )
    single_day_backtest.run(scenarios={"only_one": scenarios.SCENARIOS["default"]})
"""

from __future__ import annotations


SCENARIOS: dict[str, dict] = {
    "default": {
        "weights": None,
        "overrides": {},
    },
    # Heavy-load scenario: bias selection toward load similarity. After
    # the time-of-day bucket collapse, this is one knob (was five). Raw
    # 13.5 = sum of the prior heavy_load_peak sub-bucket weights.
    "heavy_load_peak": {
        "weights": {
            "load_level": 13.5,
            "solar_level": 0.5,
            "wind_level": 0.5,
            "outage_level": 1.0,
            "gas_level": 0.5,
        },
        "overrides": {},
    },
    # The OLD spec defaults (pre-2026-05-04). Kept as an ablation so
    # backtests can compare the current `default` against the prior
    # weights. Raw 10.0 = sum of the prior previous_default load buckets.
    "previous_default": {
        "weights": {
            "load_level": 10.0,
            "solar_level": 1.5,
            "wind_level": 1.5,
            "outage_level": 2.0,
            "gas_level": 1.0,
        },
        "overrides": {},
    },
    # Renewable-heavy: down-weights load and bumps solar/wind. Raw 4.75
    # = sum of the prior renewables_first load buckets.
    "renewables_first": {
        "weights": {
            "load_level": 4.75,
            "solar_level": 4.0,
            "wind_level": 4.0,
            "outage_level": 2.0,
            "gas_level": 1.0,
        },
        "overrides": {},
    },
    # "no_window": {"weights": None, "overrides": {"flt_radius": 0}},
    # "more_analogs": {"weights": None, "overrides": {"n_analogs": 40}},
    # "tight_season": {"weights": None, "overrides": {"season_window_days": 30}},
}
