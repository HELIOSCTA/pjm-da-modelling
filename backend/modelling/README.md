# backend/modelling

Promoted home for the PJM DA-price forecasters — the code that runs on a
Prefect schedule. It is a near-pure relocation of `modelling/da_models/`:

- **Import root** is `backend.modelling.da_models.*` (not `da_models.*`).
  Every internal import uses the `backend.` prefix so the package resolves
  under a Prefect worker rooted at `/app`, with no `sys.path` hacks.
- **Parquet data** comes from `backend/cache/` via
  `backend.modelling.da_models.common.configs.CACHE_DIR`, which reads
  `backend.settings.CACHE_DIR`. `DA_MODELS_CACHE_DIR` still overrides it
  (one env var repoints both the backend and the modelling code).
- **Publishing — this tree only.** Families compose
  `build_payload -> extract_onpeak_forecast -> publish_forecast_run`, and
  there is exactly one `publish_forecast_run` symbol —
  `backend.modelling.da_models.common.publish`. It upserts one row per run
  into `pjm_model_outputs.forecast_runs` (the seam the frontend reads).
  `backend/modelling/` is the **sole writer** of that table; the `modelling/`
  tree at the repo top is research/compute-only and writes nothing.
- **Family-import rule still holds**: a family (`baseline_meteo_da_price`,
  `like_day_model_knn`, `like_day_model_knn_sunny`) may import from
  `common/`, never from a sibling family.

`modelling/` (top of repo) stays as the research / standalone tree — the
two copies coexist for now. The scheduled deployment lives at
`backend/schedulers/prefect/modelling/pjm/da_forecasts_daily.{yaml,_flows.py}`
and runs each family's single-day `run(publish=True)` daily.

Run a forecaster directly:

```
python -m backend.modelling.da_models.baseline_meteo_da_price.pipelines.forecast_single_day
```

## Preflight data validation

Each family has a `preflight.py` next to its `pipelines/` — a **standalone**
input check that runs *before* the forecast and is never imported by the
forecast pipeline (so a bad-data abort never half-runs a forecast, and the two
have separate change-cycles).

A preflight loads exactly the inputs that family's forecast consumes (via
`common/data/loader.py` — it never re-reads parquet itself), runs a battery of
checks, prints a per-check report, and raises
`common.validation.DataValidationError` if anything reached ERROR severity. It
collects **all** results before deciding to raise, so one run surfaces every
problem. Exit code is 0 when healthy, non-zero on failure.

- Checks live once in `common/validation/` (`checks.py` primitives,
  `runner.py` `run_checks` / `ValidationReport` / `print_report`,
  `errors.py` `DataValidationError`). The package imports no model family —
  keep it that way.
- Two severities: **ERROR** aborts (missing target date, all-NaN series, wrong
  vintage, out-of-range $/MWh, stale feature feed); **WARN** is printed but does
  not abort (e.g. a known source double-publish the analog pool's `pivot_table`
  mean absorbs, an aged-but-present vintage).
- Sanity bounds (`DA_LMP_MIN_USD` / `MAX_USD`, `LOAD_MW_*`, `NET_LOAD_MW_*`,
  `FRESHNESS_WARN_DAYS`) are deliberately wide constants in `checks.py` —
  tighten when a real bug slips through.

Run a preflight directly:

```
python -m backend.modelling.da_models.baseline_meteo_da_price.preflight
python -m backend.modelling.da_models.like_day_model_knn_sunny.preflight
```

The `like_day_model_knn_sunny` preflight currently covers the highest-leverage
inputs (DA LMP label pool, SEP, the PJM calendar incl. the target-date row, and
the RT-load / weather / solar / wind hourly feeds the analog query row needs);
the softer per-domain feeds in `like_day_model_knn_sunny/domains.py` degrade
gracefully and are left for later (there's a TODO in that `preflight.py`). The
Prefect `da_forecasts_daily` flow can add a preflight task upstream of each
forecast task later — out of scope here.
