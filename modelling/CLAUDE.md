# modelling/

Python forecasters, data loaders, and the streamlit operator console.

## Conventions

When writing or substantially modifying a Python script (anything with
a `__main__` block or meant to be run directly), use the
`python-scripts` skill. The canonical worked example is
`modelling/da_models/common/data/verify_data_loader.py`.

Cross-family imports flow forward only: every model family
(`like_day_model_knn`, `baseline_meteo_da_price`, future) may import
from `common/`, but never from a sibling family. If a utility is
shared, lift it to `common/`.

Forecasters here are **research / standalone** — `run(...)` computes, prints,
and returns a result dict; nothing in `modelling/` writes Postgres. Publishing
to the `pjm_model_outputs.forecast_runs` mart (the seam the frontend reads) is
owned exclusively by the scheduled copies under `backend/modelling/da_models/`,
via `backend.modelling.da_models.common.publish.publish_forecast_run` — see
`backend/modelling/README.md` and the root `CLAUDE.md` "Cross-subtree
contracts" section. When you change a forecaster here, port the change to its
`backend/modelling/` twin if it should reach the frontend.

## Layout pointers

- `modelling/da_models/common/` — shared loaders, configs, calendar.
  - `common/data/loader.py` — parquet loaders, single source per
    dataset key in `_DEFAULT_PATTERNS`.
  - `common/data/lmp_pool.py` — DA LMP loading + wide-pivot
    primitives (`build_lmp_labels`, `LMP_HOUR_COLUMNS`).
  - `common/forecast/output.py` — display helpers
    (`actuals_from_pool`, `add_summary_cols`, `build_output_table`).
- `modelling/da_models/like_day_model_knn/` — KNN analog forecaster.
  Variant subpackage: `pjm_rto_hourly/` (long-format pool, one row per
  `(date, hour_ending)`, per-HE scalar matching — sunny-aligned). The
  pre-T4 `flt_radius`-windowed wide pool is gone; `actuals_from_pool`
  and the spec's `feature_groups` reference scalar long col names
  (`load_mw_at_hour`, `solar_at_hour`, `lmp`, etc., catalogued in
  `domains.HOURLY_STEM_TO_LONG_COL`).
- `modelling/data/cache/` — parquet cache.
- `modelling/streamlit_app/` — operator console.

## Data loader conventions

When pulling **load + solar + wind + net_load together** for the same
`(region, date)`, use the unified loaders:

- `loader.load_pjm_supply_demand_coalesced(region="RTO")` — PJM forecast
  with PJM RT fallback. RTO only.
- `loader.load_meteologica_supply_demand_coalesced()` — Meteologica
  forecast with PJM RT fallback. 4 regions.

Both make a **single forecast-vs-RT decision per `(region, date)` for all
four series**, so the identity `net_load = load - solar - wind` holds
within each row by construction.

The per-series coalescers (`load_load_coalesced`, `load_solar_coalesced`,
`load_wind_coalesced`, `load_pjm_net_load_coalesced`, and the Meteologica
equivalents) remain valid for **single-series consumption** or
**intentional per-series display-comparison** (streamlit Data page,
individual check_loaders). **Never compose `load - solar - wind` from
their outputs** — each decides forecast-vs-RT independently and the
identity breaks on dates with mixed coverage (concrete repro:
2025-05-01, ~3.9 GW max gap).

**Forecast-vs-RT rule.** Across all coalesced loaders: the DA-cutoff
forecast wins when the historical mart has all 24 `hour_ending` values
present; RT actuals fill every other `(region, date)`. `lead_days=1` is
the DA-cutoff vintage default (`as_of_date == forecast_date - 1`); pass
`lead_days=None` to skip the vintage filter.

**Region scope.** Source coverage by region:

| Series | Forecast | RT actuals |
|---|---|---|
| load | RTO + MIDATL/WEST/SOUTH | RTO + MIDATL/WEST/SOUTH |
| solar | system-wide (treated RTO) | RTO + MIDATL/WEST/SOUTH |
| wind | system-wide (treated RTO) | RTO + MIDATL/WEST/SOUTH |
| net_load (PJM) | RTO only | RTO + MIDATL/WEST/SOUTH |
| net_load (Meteologica) | RTO + MIDATL/WEST/SOUTH | RTO + MIDATL/WEST/SOUTH |

Therefore `load_pjm_supply_demand_coalesced` is RTO-only by design — PJM
solar/wind/net_load forecasts don't exist sub-zonally. Sub-zonal demand
needs Meteologica (`load_meteologica_supply_demand_coalesced`), or the
per-series load loader if only load is needed.
