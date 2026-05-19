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
- `modelling/da_models/baseline_meteo_da_price/` — passthrough of the
  Meteologica DA-price deterministic + ENS forecast as a baseline.
- `modelling/da_models/linear_arx_da_price/` — LEAR-style linear ARX
  forecaster: 24 independent per-hour `Ridge` regressions, target in
  `asinh` space, exponential recency weighting, residual-quantile bands,
  Western Hub. Shared machinery lives at the package root (`configs.py`
  estimator/window/band constants; `features/common.py` panel assembly;
  `trainer.py`, `forecast.py`, `printers.py`; `run.py::run_single_day`).
  Two variants differ only in the demand block: **`pjm_hourly/`** — PJM
  RTO supply-demand + sub-zonal *load* (MIDATL/WEST/SOUTH); **`meteo_hourly/`**
  — Meteologica regional supply-demand (load/solar/wind/net-load for
  RTO + all three sub-zones, the sub-zonal renewable detail PJM doesn't
  publish). Each variant: `config.py` (variant knobs), `builder.py`
  (`build_panel`), `pipelines/forecast_single_day.py::run()`;
  `meteo_hourly/` also has `pipelines/forecast_next_14_days.py` — a
  D+1..D+14 strip that trains once, takes the further-out demand from the
  latest published Meteologica vintage, and forward-fills the feeds that
  don't reach the horizon (outages, ICE next-day gas) from their last
  known value (`features/common.py::assemble_panel` grew
  `extra_target_dates` + `forward_fill_target_cols` for this). The
  feed-agnostic feeds (weather, ICE next-day gas, PJM outage forecast,
  calendar + engineered interactions) and the optional toggle-able
  backward-LMP anchor are shared. Design memo + Tier-2 roadmap (quantile
  regression, conformal bands, multi-window):
  `modelling/@TODO/pjm-research-for-modelling/linear_regression_model.md`.
- `modelling/da_models/supply_stack/` — structural merit-order forecaster
  (per `pjm-research-for-modelling/supply_stack_model.md`). **Per-unit
  PJM fleet** (`data/pjm_fleet.parquet`, ~2,800 thermal units after
  netting renewables off load) extracted from the legacy Excel stack
  model — `data/_extract_fleet_from_excel.py` builds it from
  `.archive/.excel/PJM_Stack_Model_v1_2026_mar_10.xlsx`; refresh annually
  (or swap for a real EIA-860/PUDL pull later). Each hour: build the
  outage-derated cost-ordered stack (`stack/merit_order.py` — `var_cost =
  heat_rate × fuel_price + VOM + RGGI carbon`; gas units priced by the 4
  scraped ICE hubs, coal/oil/uranium + the other gas hubs from
  `configs.FUEL_PRICES`), dispatch the hour's net load (`stack/dispatch.py`
  — clearing price = marginal var cost × bid-stack markup(util) +
  congestion + hour-of-day ramp adder + reserve-utilization scarcity
  adder(util)), Monte-Carlo bands from load/forced-outage/gas draws.
  Forward-looking and extrapolates by construction (a heat-event day just
  dispatches further up the convex curve), so it complements the
  data-driven models during regime shifts; demand comes from
  `load_meteologica_supply_demand_coalesced` (longest horizon → next-week
  dates work). Entry: `pipelines/forecast_single_day.py::run()`.
  **Calibration is provisional** — per-unit heat rates/VOM are real, but
  the curves (bid markup, scarcity bands, ramp adder), the coal/oil price
  constants, and the available-capacity assumption (currently nameplate −
  outages, no reserve-requirement haircut → on a 108 GW net-load day it
  sits at ~82% utilization and never reaches the scarcity bands, so it
  under-prices heat events vs Meteologica) are hand-set, not fitted; the
  design-memo Phase-5 backtest (price-duration-curve, marginal-fuel-match,
  DM test) is what tunes them. Tier-2: validation/backtest harness, then
  ensemble with the linear ARX + baseline_meteo via QRA.
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
