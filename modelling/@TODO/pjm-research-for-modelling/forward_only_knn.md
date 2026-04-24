# Forward-Only KNN: Ship-Now MVP Plan

Inference-first implementation for `backend/src/forward_only_knn/`.
Goal: ship a live D+1/D+N forecast path quickly, without backtesting or research notebooks in Phase 1.

Terminology:
- `Like-Day` (current): match today state, use what followed.
- `Forward-Only KNN` (new): match tomorrow setup, use matched day price.

---

## Scope

### In scope (now)
- Build a production-usable forward-only analog forecaster.
- Keep output shape compatible with existing like-day reporting path.
- Support D+1 and strip (D+1..D+N) inference.

### Out of scope (later)
- Backtesting harness
- CV tuning workflows
- Evaluation notebooks
- Side-by-side historical performance report

---

## Non-Negotiable Live Guardrails

1. As-of data discipline
- Query row may use only inputs available at model run time for each target date.
- Pool features must follow one consistent construction rule per feature group.

2. Pool-only scaling
- Fit normalization stats on the candidate pool only.
- Transform query with those pool stats.
- Never include query in scaler fitting.

3. Minimum analog fallback
- If strict calendar filter returns too few rows, relax in a fixed ladder until minimum pool size is met.
- Example ladder: same DOW+holiday -> same DOW -> DOW group -> no DOW hard filter.

4. Horizon feature gating
- Explicitly gate features by horizon based on reliable availability.
- If gas/ICE are not reliably available for D+k, disable those groups for that horizon.

---

## Directory Layout (MVP)

```
backend/src/forward_only_knn/
|-- __init__.py
|-- settings.py
|-- configs.py
|-- features/
|   |-- __init__.py
|   `-- builder.py
|-- similarity/
|   |-- __init__.py
|   |-- metrics.py
|   |-- filtering.py
|   `-- engine.py
`-- pipelines/
    |-- __init__.py
    |-- forecast.py
    `-- strip_forecast.py
```

Deferred modules (`evaluation/`, `validation/`, `notebooks/`, reporting adapters) are intentionally excluded from MVP.

---

## Data Contracts

### Pool row contract (`build_pool`)
- One row per historical delivery date `D`.
- Feature columns represent that day conditions under a consistent rule.
- Label columns are `lmp_h1..lmp_h24` for that same `D`.

### Query row contract (`build_query_row`)
- One row for target delivery date `T`.
- Columns must match pool feature namespace exactly.
- No label columns in query.

### Critical
- Pool/query feature names must be identical for all enabled feature groups.
- Missing groups for a horizon must be disabled explicitly, not silently zero-filled.

---

## Feature Set for MVP

Start with high-coverage groups:
- `load_level`, `load_ramps`
- `weather_level`, `weather_hdd_cdd`
- `calendar_dow`
- optional: `gas_level` and `ice_forward_level` only when reliable for that horizon

Defer outage/renewables to Phase 2 unless coverage is already solid.

---

## Similarity Engine (MVP)

`find_twins(query, pool, n_analogs, feature_weights, target_date, ...)`

Pipeline:
1. Apply strict calendar filter.
2. If pool too small, run fallback ladder to reach minimum size.
3. Normalize per feature group using pool-only stats.
4. Compute NaN-aware per-group distance (skip missing dims, do not impute 0).
5. Weighted sum over groups.
6. Apply optional recency penalty.
7. Select top N.
8. Convert distance to analog weights (sum to 1).

Determinism:
- Stable sort on `(distance, date)` so ties are reproducible.

---

## Forecast Pipelines

### `pipelines/forecast.py`
- Default `target_date = today + 1 day`.
- Build pool (cached).
- Build query row for target date.
- Find twins.
- Produce:
  - hourly point forecast (weighted mean)
  - hourly quantiles (weighted quantile)
  - analog table with rank/distance/weight
- Return same output schema used by like-day API consumers.

### `pipelines/strip_forecast.py`
- For each forecast day in horizon:
  - build that day query
  - run `find_twins`
  - compute hourly outputs
- No synthetic reference row.

---

## Config Defaults (MVP)

- `DEFAULT_N_ANALOGS = 20`
- `MIN_POOL_SIZE = 150` after fallback
- `HOURS = [1..24]`
- `QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90]`
- Initial feature weights (hand-tuned):
  - `load_level: 3.0`
  - `load_ramps: 1.0`
  - `weather_level: 2.0`
  - `weather_hdd_cdd: 2.0`
  - `calendar_dow: 1.0`
  - `gas_level: 2.0` (only when enabled)
  - `ice_forward_level: 2.0` (only when enabled)

---

## Implementation Order

1. Scaffold package and `configs.py`.
2. Implement `features/builder.py` with pool/query column contract checks.
3. Implement `similarity/filtering.py` including fallback ladder.
4. Implement `similarity/metrics.py` with NaN-aware distance.
5. Implement `similarity/engine.py` with pool-only scaling and stable ranking.
6. Implement `pipelines/forecast.py`.
7. Implement `pipelines/strip_forecast.py`.
8. Wire minimal logging and cache hooks.

---

## MVP Acceptance Checks (No Backtest)

1. Tomorrow run check
- `run_forecast()` executes end-to-end for tomorrow without missing-feature failure.

2. Strip run check
- `run_strip_forecast(horizon=3)` executes and returns per-day hourly outputs.

3. Schema compatibility check
- Output keys/tables align with existing like-day consumer expectations.

4. Sanity range check
- Forecast HE values and on-peak/off-peak aggregates are numerically plausible (no NaN/inf, no exploded outliers from weighting bugs).

---

## Phase 2 (After MVP Ships)

- Add outage and renewable feature groups.
- Add preflight data freshness checks.
- Add formal evaluation metrics and CV tuning.
- Add side-by-side benchmark reporting versus Like-Day.
