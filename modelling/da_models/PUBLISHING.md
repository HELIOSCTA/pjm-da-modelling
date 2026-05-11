# Forecast publishing registry

How every forecaster under `modelling/da_models/` lands a row in
`pjm_model_outputs.forecast_runs` — the single seam the frontend reads.

## The contract

**Table columns (physical order):**

```
pjm_model_outputs.forecast_runs (
    model_family                  text,
    model_name                    text,
    run_date                      date,        -- forecast vintage (date produced)
    target_date                   date,        -- delivery date
    da_lmp_total_onpeak_forecast  numeric(10,2),
    payload                       jsonb,
    run_id                        uuid,
    created_at                    timestamptz, -- appended by azure_postgresql_utils
    updated_at                    timestamptz, -- appended by azure_postgresql_utils
    PRIMARY KEY (model_name, target_date, run_id)
)
```

- `run_date` is the **vintage** (the date the run was produced); `target_date` is the
  **delivery** date. For "delivery is tomorrow" runs they're a day apart;
  `target_date - run_date` is the lead. `run_date` is a plain column, not part of the
  PK — every run is kept, "latest" is `ORDER BY run_date DESC, created_at DESC LIMIT 1`.
- `da_lmp_total_onpeak_forecast` is the headline OnPeak HE8-23 point forecast, pulled
  out of the payload so scorecards can join on it without unpacking jsonb.
- Run-creation timestamps (`created_at_utc` / `created_at_local`) live **inside the
  payload jsonb**, not as columns; the table's row timestamps are the helper's
  `created_at` / `updated_at`.
- The table (and the `pjm_model_outputs` schema) is created on the first write —
  `utils.azure_postgresql_utils.upsert_to_azure_postgresql` does `CREATE SCHEMA / TABLE
  IF NOT EXISTS`, no migration step.

## The composition (centralization rule)

There is exactly **one** `publish_forecast_run` symbol in `modelling/`, in
`modelling/da_models/common/publish.py`. It owns the row layout and the
`da_lmp_total_onpeak_forecast` column, and delegates DDL + write to
`azure_postgresql_utils`. Each model **family** owns `build_payload` and
`extract_onpeak_forecast` in its own `publish.py`. A pipeline composes, inside an
`if publish:` block:

```
build_payload(...) -> extract_onpeak_forecast(payload) -> publish_forecast_run(...)
```

passing `run_date` alongside `target_date`. Two payload shapes exist, keyed by
`model_family` (the frontend's `frontend/types/forecast.ts` `ForecastRunPayload`
discriminated union switches on it):

- **`like_day` -> `KnnPayload`** — `payload`: `hub, day_type, n_analogs,
  n_unique_analog_dates, hourly[], blocks[] ({block, quantile_label, value}),
  analogs[]`. `extract_onpeak_forecast` reads the `blocks[]` row `block='OnPeak'` +
  `quantile_label='Forecast'`. KNN families: `like_day_model_knn/*` (own per-variant
  `publish.py`) and `like_day_model_knn_sunny/pjm_rto_hourly` (`like_day_model_knn_sunny/publish.py`).
- **`baseline` -> `IcePayload`** — `payload`: `lead_days, det_executed_local,
  ens_executed_local, ice_anchor{...}, hourly[] ({hour_ending, point_forecast, ens_avg,
  ens_bottom, ens_top, members_p25, members_p75, actual_lmp}), blocks[] ({series, block,
  value}), ice_trades[]`. `extract_onpeak_forecast` reads `blocks[]` row `block='OnPeak'`
  + `series='Det'`. The unanchored Meteo baseline reuses `baseline_meteo_da_price/publish.py`'s
  `build_payload` with no-anchor inputs, yielding `ice_anchor.applied=False`.

**Every pipeline publishes unconditionally** (`PUBLISH = True`). The `publish` parameter
on `run()` is the per-call escape hatch — batch/backtest callers pass `publish=False`
so a sweep over many dates doesn't write a row per date.

## Registry

One row per `forecast_single_day*.py` under `modelling/da_models/`.

| model_family | model_name | run_date | target_date | da_lmp_total_onpeak_forecast | payload (top-level keys) | run_id | created_at / updated_at | status |
|---|---|---|---|---|---|---|---|---|
| `like_day` | `pjm_rto_hourly` | `run()`'s `run_date` arg, default `date.today()` (vintage) | `target_date` arg, default tomorrow (delivery) | `blocks[]` row `block='OnPeak'`, `quantile_label='Forecast'` | `hub, day_type, n_analogs, n_unique_analog_dates, hourly[], blocks[], analogs[]` + `run_date, target_date, model_name, model_family, run_id, created_at_utc, created_at_local` | `uuid4()` per run | set by `azure_postgresql_utils` (MST) | publishes |
| `like_day` | `meteo_rto_hourly` | same | same | same as above | same as above (Meteologica-fed pool) | `uuid4()` per run | same | publishes |
| `like_day` | `pjm_rto_hourly_sunny` | same | same | same as above | same as above (sunny spec; `like_day_model_knn_sunny/publish.py`) | `uuid4()` per run | same | wired this pass |
| `baseline` | `baseline_meteo_da_price_ice_anchored` | `run()`'s `run_date` arg, default `date.today()` | `target_date` arg, default tomorrow | `blocks[]` row `block='OnPeak'`, `series='Det'` | `lead_days, det_executed_local, ens_executed_local, ice_anchor{...}, hourly[], blocks[], ice_trades[]` + the common keys | `uuid4()` per run | same | publishes |
| `baseline` | `baseline_meteo_da_price` | same | same | `blocks[]` row `block='OnPeak'`, `series='Det'` | same as the anchored payload, but `ice_anchor.applied=False` and `ice_trades=[]` (no anchor) | `uuid4()` per run | same | wired this pass |

Not in the table:
- `modelling/da_models/naive_baselines/` — being removed; not wired.
- (`like_day_model_knn_sunny/meteo_regional_hourly/` was removed during the wiring pass — it had a `forecast_single_day.py` + a `head_to_head.py` comparison harness; both gone.)

## Adding a new forecaster

1. Pick a globally-unique `PUBLISHED_MODEL_NAME` and a `PUBLISHED_MODEL_FAMILY` that
   matches an existing `ForecastRunPayload` branch (`"like_day"` or `"baseline"`) whose
   `build_payload` produces your payload shape — or add a new branch to
   `frontend/types/forecast.ts` in the same change.
2. Reuse an existing family's `build_payload` / `extract_onpeak_forecast` if the shape
   fits; otherwise add a `publish.py` next to your pipeline (or at the family root).
3. In the pipeline: `RUN_DATE: date | None = None` + a `run_date` param on `run()`
   resolved `None -> date.today()`; `PUBLISH: bool = True` + a `publish: bool = PUBLISH`
   param (the per-call escape hatch); an `if publish:` block lazily importing
   `build_payload` / `extract_onpeak_forecast` and composing them into
   `publish_forecast_run(..., run_date=resolved_run_date, ...)`.
4. Add a row to the table above.
