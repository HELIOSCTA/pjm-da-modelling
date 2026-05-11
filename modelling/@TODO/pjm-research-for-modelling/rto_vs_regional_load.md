# RTO vs Regional Supply-Demand Features — Single-Day A/B Test

CODEX prompt for a one-day proof-of-concept comparing PJM RTO vs
Meteologica regional (MIDATL/WEST/SOUTH) load, solar, and wind
forecasts as matching features in the sunny KNN like-day forecaster.

---

## Hypothesis

For DA LMP forecasting at PJM Western Hub on a single target date,
swapping the matching features from PJM RTO load/solar/wind/net_load
forecasts to Meteologica regional (MIDATL + WEST + SOUTH) forecasts
produces a lower hourly MAE. RTO aggregation dilutes the Western Hub
signal with SOUTH (Dominion) demand that doesn't drive Western Hub
clearing.

This is a one-day proof-of-concept, not a backtest. Goal: validate the
plumbing and get a directional read.

## Target

- Target date: **2026-05-04** (a recent date with realized DA LMP
  actuals so the run produces metrics, not just a forecast). Confirm
  actuals exist at startup; if missing, fall back to the most recent
  date with full 24-HE Western Hub DA LMP coverage and report which
  date you used.
- Hub: WESTERN HUB
- Label: hub_lmp (sunny default)

## Architecture — what to build and what NOT to build

The existing variant `like_day_model_knn_sunny/pjm_rto_hourly/` is the
template. Recon shows the engine, builder, forecast, and metrics
modules are all **domain-agnostic** — they iterate `spec.domains`,
z-score whatever features land in the pool, and run KNN. The feature
source is encoded entirely in `like_day_model_knn_sunny/domains.py`
via per-domain `pool_builder(cache_dir)` and
`query_builder(target_date, cache_dir)` callbacks.

Therefore:

**DO build:**

1. New regional domains in
   `like_day_model_knn_sunny/meteo_regional_hourly/domains.py` (the
   package directory exists and is empty). Define one domain per
   region + series, e.g. `meteo_load_midatl_at_hour`,
   `meteo_solar_west_at_hour`, `meteo_wind_south_at_hour`,
   `meteo_net_load_midatl_at_hour`, etc. For the single-day test,
   ship MIDATL and WEST only — SOUTH adds noise for Western Hub by
   hypothesis. Make this explicit and revisitable.
2. Register those domains so `_shared.build_pool_from_spec()` picks
   them up — follow the pattern in the existing
   `like_day_model_knn_sunny/domains.py::DOMAIN_REGISTRY`.
3. A new `ModelSpec` named e.g. `pjm_meteo_regional_hourly_sunny`
   registered in `MODEL_REGISTRY` that lists the new regional domains
   alongside the kept-as-is daily-broadcast domains
   (`outages_scalar`, `gas_scalar`, `calendar_scalar`,
   `temperature_scalar`).
4. A new pipeline at
   `like_day_model_knn_sunny/meteo_regional_hourly/pipelines/forecast_single_day.py`
   that mirrors the existing
   `pjm_rto_hourly/pipelines/forecast_single_day.py` verbatim except
   `MODEL_NAME = "pjm_meteo_regional_hourly_sunny"`.

**DO NOT build:**

- Do NOT create new `builder.py` / `engine.py` / `forecast.py` /
  `metrics.py` / `printers.py` under `meteo_regional_hourly/`. The
  existing modules in `pjm_rto_hourly/` are domain-agnostic and
  re-importable as-is. If you find yourself copying any of them, stop
  and ask.
- Do NOT modify the `pjm_rto_hourly/` variant.

## Data layer — exact loader to use

Single call:

```python
from da_models.common.data import loader
df = loader.load_meteologica_supply_demand_coalesced(cache_dir=CACHE_DIR)
# returns long: (region, date, hour_ending, source, load_mw, solar_mw,
#                wind_mw, net_load_mw, *_execution_datetime_local)
# regions: RTO, MIDATL, WEST, SOUTH
# 96 rows per date (4 x 24)
# net_load = load - solar - wind holds row-wise (verified)
```

Filter to the desired regions post-load and pivot to one column per
(region, series, hour_ending) for the per-HE feature vector. Do NOT
recompute net_load from the per-series coalescers — the unified loader
has already made the joint forecast-vs-RT decision (CLAUDE.md
"Data loader conventions").

For the head-to-head, the existing `pjm_rto_hourly` already consumes
PJM RTO via the per-series loaders inside its domains; you don't need
to touch its loader path.

## Head-to-head run procedure

1. Run the existing `pjm_rto_hourly` pipeline for the target date —
   capture its `output_table` (Actual / Forecast / Error) and `metrics`
   (MAE, RMSE, rMAE, pinball, coverage).
2. Run the new `meteo_regional_hourly` pipeline for the same target
   date — same calendar / recency / n_analogs / season window
   settings.
3. Print a side-by-side comparison table: HE 1–24, Actual,
   RTO-Forecast, RTO-Error, Regional-Forecast, Regional-Error.
4. Print headline metrics for both runs in one block. Include rMAE
   against the same naive d-7 baseline so the two are directly
   comparable.
5. Flag the hours where each model wins by >$2/MWh.

## Validation — what counts as a directional pass

For a single day, "the regional model has lower 24-hour MAE than the
RTO model on the target date" is a weak signal — call it a positive
read but explicitly note that confirming the hypothesis requires a
multi-day backtest (out of scope here). Also report which analogs
each model picked per HE; if regional and RTO share most of their
top-N analogs, the test isn't really exercising the hypothesis and
that should be called out.

## Standards (non-negotiable, from `.claude/standards/python_scripts.md`)

- No argparse. Module-level constants near the top of the new
  pipeline file; single `run()` entry; `__main__` is a one-liner.
- `from __future__ import annotations` first.
- Reconfigure stdout / stderr to UTF-8 at the top of `run()`.
- ASCII-only printed output: no emojis, no Unicode box-drawing. Use
  `===` / `---` / `|` separators.
- Path bootstrap if the script is run as `python path/to/file.py`.

## Standards (from `CLAUDE.md`)

- Cross-family rule: this lives under `like_day_model_knn_sunny/`. It
  may import from `common/` and from sibling modules under
  `like_day_model_knn_sunny/`. It must NOT import from
  `like_day_model_knn/`.
- Use `load_meteologica_supply_demand_coalesced()` as the single
  source for the bundle; don't compose net_load from per-series
  coalescers.

## Literature note

`modelling/@TODO/pjm-research-for-modelling/pjm-like-day-research.md`
emphasizes RTO-level features and Western Hub temperature as the load
driver. It does not directly compare regional vs RTO feature sources
for like-day matching — treat this as an open question this test
nibbles at, not a literature-backed claim.

## Plan first, then implement

Return a plan covering, before writing code:

1. Exact list of regional domains you'll register (which regions,
   which series, count of features added to the per-HE vector).
2. The `ModelSpec.domains` tuple for the new model — including which
   existing daily-broadcast domains you keep.
3. The target date you'll use after checking DA LMP actuals coverage,
   and your fallback if 2026-05-04 isn't available.
4. The exact head-to-head config (`n_analogs`, `season_window_days`,
   `recency_half_life_days`, calendar filter knobs) — match the sunny
   defaults unless you flag a deliberate change.
5. The output format for the side-by-side comparison.

Wait for confirmation on the plan before scaffolding.

## Useful pre-recon (already done — don't redo)

- **pjm_rto_hourly entry:** `pipelines/forecast_single_day.py::run()`,
  invoked as
  `python -m da_models.like_day_model_knn_sunny.pjm_rto_hourly.pipelines.forecast_single_day`.
- **Domain registry & ModelSpec lookup:**
  `like_day_model_knn_sunny/domains.py::DOMAIN_REGISTRY` and
  `configs.py::MODEL_REGISTRY`.
- **Pool/query assembly:**
  `like_day_model_knn_sunny/_shared.py::build_pool_from_spec`
  (~lines 138–223) and `build_query_row_from_spec` (~lines 226–289).
  They iterate `spec.domains` with no regional knowledge baked in.
- **Engine:** `pjm_rto_hourly/engine.py::find_twins` —
  distance-agnostic, reusable as-is.
- **Meteologica loader signature confirmed:**
  `loader.load_meteologica_supply_demand_coalesced(*, cache_dir=None, columns=None, lead_days=1)`,
  returns 96 rows/date wide-format, regions
  {RTO, MIDATL, WEST, SOUTH}, identity verified row-wise.
