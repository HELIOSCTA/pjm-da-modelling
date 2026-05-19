# Linear-Regression DA-Price Forecaster — Literature Review & Design Memo

Scoping memo for a new linear-regression model family under
`modelling/da_models/`. Phase 1 deliverable: synthesise the
`pjm-research-for-modelling/` notes for the linear-regression family,
state what the prior LASSO-QR work covered and where it fell short,
propose a concrete v1 design, and surface the open questions. **No model
code is written until the design below is approved.**

Companion notes (all in this folder):
[da_price_model_families_survey.md](./da_price_model_families_survey.md),
[lasso_model.md](./lasso_model.md),
[backward_vs_forward_looking.md](./backward_vs_forward_looking.md),
[hourly_vs_daily_features.md](./hourly_vs_daily_features.md),
[rto_vs_regional_load.md](./rto_vs_regional_load.md),
[next_day_gas_prices.md](./next_day_gas_prices.md),
[strip_forecasts.md](./strip_forecasts.md),
[backtest_eval_metrics.md](./backtest_eval_metrics.md),
[onpeak_shape_metrics.md](./onpeak_shape_metrics.md),
[mean_vs_median.md](./mean_vs_median.md),
[new_models_to_implement.md](./new_models_to_implement.md),
[historical_forecasts.md](./historical_forecasts.md),
[pjm_data_sources.md](./pjm_data_sources.md).

---

## (a) What the literature / notes recommend

### The reference model: LEAR (LASSO-Estimated AutoRegressive)

- The single most load-bearing finding across the notes
  ([da_price_model_families_survey.md](./da_price_model_families_survey.md) §1.2,
  §2.1; [strip_forecasts.md](./strip_forecasts.md) "Literature Support";
  [new_models_to_implement.md](./new_models_to_implement.md) §1): **LEAR is
  the modern statistical benchmark.** Lago, Marcjasz, De Schutter & Weron
  (2021) defines LEAR + a 4-layer DNN as the two reference models any new EPF
  method must beat, and publishes scores on PJM in the `epftoolbox`
  benchmark — i.e. there is a public accuracy floor for PJM Western Hub.
- LEAR = a high-dimensional ARX (≈150–300 candidate regressors: lagged
  prices, load forecast, gas, calendar) with **LASSO doing the variable
  selection per hour**. Foundational: Uniejewski, Nowotarski & Weron (2016);
  Ziel & Weron (2018).

### Univariate (24 per-hour models) beats multivariate

- Ziel & Weron (2018), via
  [da_price_model_families_survey.md](./da_price_model_families_survey.md)
  §2.2 and [backtest_eval_metrics.md](./backtest_eval_metrics.md): **24
  independent per-hour models** ("univariate" framework) beats a single
  24-output model on average across GEFCom / NordPool / EPEX. Cuaresma et al.
  (2004) reached the same conclusion on EEX. This is the design pattern the
  planned LASSO-QR and LightGBM models already adopt.

### Variance-stabilizing transform on the target is near-mandatory

- [da_price_model_families_survey.md](./da_price_model_families_survey.md)
  §1.5; [lasso_model.md](./lasso_model.md) Tier-1 item 1;
  [backward_vs_forward_looking.md](./backward_vs_forward_looking.md):
  Uniejewski, Weron & Ziel (2018) — **asinh (area-hyperbolic-sine) is the
  most robust VST** across markets; `np.arcsinh(y)` before fit, `np.sinh()`
  after predict; handles negative prices, no tuning needed. Without it,
  pinball loss at tail quantiles collapses to a near-median fit and the
  quantile bands degenerate. Chec, Uniejewski & Weron (2025): parameterised
  asinh cuts LEAR MAE up to ~14.6% in volatile sub-periods.

### Calibration-window choice & multi-window averaging

- [lasso_model.md](./lasso_model.md) Tier-2 item 4: Marcjasz, Serafin &
  Weron (2018, 2019); Hubicka, Marcjasz & Weron (2018, 2019) — **averaging
  forecasts across several calibration windows** (e.g. 56-day + ~728-day)
  is the most-validated technique in EPF; short windows track the current
  regime (e.g. shoulder-season load–price dynamics) without winter/summer
  contamination, long windows give seasonal stability.
- [lasso_model.md](./lasso_model.md) Tier-1 item 2: **exponential recency
  weighting** via `sample_weight = gamma**arange(n-1,-1,-1)` is the cheap
  approximation of the same idea (start `gamma≈0.997`, half-life ~231 d).

### Forward fundamentals must not be drowned by backward LMP anchors

- [backward_vs_forward_looking.md](./backward_vs_forward_looking.md) is the
  central cautionary note: in the prior LASSO-QR run, the top-10 features by
  |coef| were ~75% **backward-looking** (yesterday's realized LMP stats:
  `lmp_onpeak_avg`, `lmp_daily_min`, `lmp_per_load`, …); the forward load
  forecast was regularised away. Result: $44.96 OnPeak forecast for
  2026-04-14 when DA cleared $83+, because "yesterday looked like spring."
  Fixes recommended there: multi-window (a 56-day window in April promotes
  load over backward LMP naturally), capping/dropping backward LMP features,
  and an **ICE forward-price feature** (the most direct fix — a
  market-consensus forward price for the delivery day overrides the backward
  anchor). The Sunday→Monday transition is the worst case.
- [strip_forecasts.md](./strip_forecasts.md): unlike the like-day model,
  feature-to-price linear models extrapolate cleanly to D+2…D+N — plug in
  the forward load/gas/outage forecasts for that day. So a linear model is
  also the natural home for the strip horizon, not just D+1.

### Probabilistic output

- [da_price_model_families_survey.md](./da_price_model_families_survey.md)
  §2.3, §6; Uniejewski & Weron (2021); Nowotarski & Weron (2018):
  **quantile regression** (one pinball-loss fit per quantile level) is the
  native way to get probabilistic output from a linear model. Quantile-
  specific alpha ([lasso_model.md](./lasso_model.md) Tier-1 item 3) — lower
  L1 penalty for tail quantiles — widens collapsed bands. Conformalised
  quantile regression (CQR / MAPIE) and isotonic post-processing
  ([lasso_model.md](./lasso_model.md) Tier-2/3) give coverage guarantees and
  fix quantile crossing, but are post-processing layers, not v1.
- [mean_vs_median.md](./mean_vs_median.md): keep a separate **mean ("EV")**
  row distinct from the **P50 (median)** row — on right-skewed peak hours
  the expected $/MWh that matters for settlement math sits well above the
  median. The standard output table already carries both.

### Feature granularity & geography

- [hourly_vs_daily_features.md](./hourly_vs_daily_features.md): the
  daily-vs-hourly debate there is about the *like-day matching distance*
  (curse of dimensionality on a ~300-day pool) — it does **not** bind a
  regression model trained on years of data. For a linear ARX the standard
  EPF practice is the opposite: a rich per-hour feature matrix, 24 models.
  Still, derived daily aggregates (peak / valley / ramp) are useful
  engineered regressors.
- [rto_vs_regional_load.md](./rto_vs_regional_load.md): open hypothesis
  that Meteologica regional (MIDATL + WEST) load beats PJM RTO load as a
  Western Hub signal because RTO dilutes with SOUTH/Dominion demand. Not
  literature-backed; worth carrying both and letting the fit decide
  (regularisation will drop the dead one). Note the repo rule (MEMORY: "never
  drop SOUTH a priori") — include all three sub-zones as candidate
  regressors rather than pre-pruning.

### Gas feature

- [next_day_gas_prices.md](./next_day_gas_prices.md): the right gas number
  for a DA model is the **locked daily VWAP-close keyed on `gas_day =
  forecast_target_date`** (ICE next-day physical). Available via
  `loader.load_gas_prices_hourly` (hubs incl. Tetco M3, Transco Z6, Dominion
  South). It is forward-looking with respect to the delivery day (priced the
  prior session) — exactly the kind of forward fundamental
  [backward_vs_forward_looking.md](./backward_vs_forward_looking.md) wants
  promoted.

### Evaluation

- [backtest_eval_metrics.md](./backtest_eval_metrics.md) /
  [onpeak_shape_metrics.md](./onpeak_shape_metrics.md): score against a
  naive **d-7 (last-week-same-hour)** baseline → rMAE; report pinball loss
  and coverage for the bands; on the HE8-23 on-peak block report both the
  **block-mean error** (matches futures settlement) and shape-within-block
  metrics (truncated variogram, extremum-position errors). `epftoolbox`
  ships no block primitive — block scoring is a caller-side filter.
- `modelling/da_models/common/evaluation/metrics.py` already has
  `crps_from_quantiles`, `coverage`, `point_errors`.

### What the notes say *not* to bother with for a linear model

- [da_price_model_families_survey.md](./da_price_model_families_survey.md)
  §9: multivariate/cross-zone joint models (production target is Western Hub
  single-output); deep nets / TFT (small, inconsistent gains over LightGBM at
  high cost). For the linear family specifically, plain OLS with no
  regularisation on a ~250-feature matrix will overfit — some shrinkage
  (ridge or LASSO) is expected even for a "baseline."

---

## (b) What the prior LASSO-QR work covered, and where it fell short

**Status check:** the notes reference an implementation at
`backend/src/lasso_quantile_regression/` (and `backend/src/forward_only_knn/`).
**Neither directory exists in the current repo** — `backend/src/` is absent.
So in this codebase the LASSO-QR work is *documented design + post-mortems*
([lasso_model.md](./lasso_model.md),
[backward_vs_forward_looking.md](./backward_vs_forward_looking.md)) but **not
running code**. Treat those notes as inherited design knowledge to build on,
not as a module to refactor. (Open question 1 asks whether the new family
should *be* that LASSO-QR resurrected, or a simpler sibling.)

What that prior design did (per [lasso_model.md](./lasso_model.md)):

- 24 per-hour models × N quantile levels; `StandardScaler` + sklearn
  `QuantileRegressor` (L1); time-series CV for `alpha`.
- Interaction terms: `load_x_gas`, `reserve_margin_pct`, `load^2`,
  `outage^2`.
- A 730-day flat calibration window.
- Tier-1 fixes already specced (asinh VST, exponential recency weighting,
  quantile-specific alpha).

Where it fell short (per
[backward_vs_forward_looking.md](./backward_vs_forward_looking.md) and
[lasso_model.md](./lasso_model.md) "Problem Statement"):

1. **Collapsed quantile bands** — P10…P90 nearly identical for most hours
   (pinball loss dominated by the bulk). Fix: asinh VST + quantile-specific
   alpha.
2. **Backward LMP anchoring** — ~75% of top-10 |coef| mass on yesterday's
   realized LMP stats; forward load suppressed. Caused the 2026-04-14
   downside miss. Fix: multi-window, drop/cap backward LMP features, add ICE
   forward price.
3. **Load features regularised away** — LASSO spread the load signal across
   correlated derived features with diluted coefficients (an argument for
   elastic-net / group handling, or for explicitly protecting the load
   regressors).
4. **Single flat 730-day window** — Jan-2024 winter weighted same as
   yesterday. Fix: multi-window averaging (56d + 728d) and/or recency
   weighting.

---

## (c) Proposed design for the new family

Recommended directory: **`modelling/da_models/linear_arx_da_price/`**
(parallels `baseline_meteo_da_price/`). One sentence on positioning: this is
the **LEAR-style linear ARX reference** the model zoo is missing — a
feature-to-price model that extrapolates along a learned curve (unlike
like-day) and is interpretable via its coefficients (unlike LightGBM). It is
also the natural engine for the D+2…D+N strip ([strip_forecasts.md](./strip_forecasts.md)).

**Target & granularity** (open questions 2): Western Hub DA LMP, **24
independent per-hour models** (univariate framework, per Ziel & Weron 2018).
v1 forecasts a single delivery date (D+1 default) with the standard
`HE1-24 | OnPeak | OffPeak | Flat` table; a `forecast_balweek`-style
multi-day wrapper is a fast follow given the model accepts forward inputs per
day.

**Estimator** (open questions 1, 4): v1 = **point ridge regression** per
hour (`sklearn.linear_model.Ridge`, `alpha` via expanding-window time-series
CV), on a `StandardScaler`-d feature matrix, target in **asinh space**
(`arcsinh` before fit, `sinh` after predict). Bands from a **rolling-holdout
empirical residual quantile** (compute residuals in asinh space on the last
~60–90 days, add to the point forecast, inverse-transform) — honest v1
intervals without standing up 9× quantile regressions yet. A `LASSO`/quantile
variant and CQR post-processing are explicit Tier-2 items in the family
README, mirroring [lasso_model.md](./lasso_model.md).

**Calibration window:** start with a single ~728-day expanding window +
exponential recency weighting (`gamma≈0.997`); leave a hook for the
56d+728d multi-window average (Tier-2). Skip dates with incomplete coverage.

**Feature matrix (per hour h, all available at the D-1 DA cutoff):**

- *Forward fundamentals (delivery day D)* — load forecast at HE h (RTO **and**
  Meteologica MIDATL/WEST/SOUTH — carry all, let shrinkage prune), solar &
  wind forecast at HE h, net-load forecast at HE h, outage forecast total MW,
  reserve-margin proxy, weather forecast (temp, CDD, HDD) at HE h, ICE
  next-day gas (`gas_day = D`: Tetco M3, Transco Z6, Dominion South),
  calendar (DOW one-hots, NERC-holiday flag, month sin/cos).
- *Backward anchors (reference day D-1, deliberately capped)* — at most a
  small set of D-1 realized LMP summaries (e.g. `lmp_offpeak_avg`,
  `lmp_daily_min`) **with the option to disable them entirely**, per
  [backward_vs_forward_looking.md](./backward_vs_forward_looking.md). For
  Monday forecasts, use **Friday** (lag 3) for the backward LMP anchor, not
  Sunday.
- *Engineered interactions* — `load_x_gas`, `reserve_margin_pct`,
  `load^2`, `outage^2` (carried over from the LASSO-QR design).
- Log/flag the backward-vs-forward coefficient-mass ratio after each fit
  (the monitoring hook from
  [backward_vs_forward_looking.md](./backward_vs_forward_looking.md)).

**Data access (reuse `common/` — do not re-read parquet):**

- `loader.load_lmps_da` / `lmp_pool.build_lmp_labels(df, "WESTERN HUB")` +
  `lmp_pool.LMP_HOUR_COLUMNS` — labels and the d-7 naive baseline.
- `loader.load_pjm_supply_demand_coalesced(region="RTO")` and
  `loader.load_meteologica_supply_demand_coalesced()` — load/solar/wind/
  net-load with the single forecast-vs-RT decision per `(region,date)`.
- `loader.load_outages_forecast`, `loader.load_weather_forecast_hourly`
  (or `load_weather_coalesced`), `loader.load_gas_prices_hourly`.
- `common.calendar.compute_calendar_row` for the calendar block.
- `common.forecast.output.build_output_table` / `add_summary_cols` /
  `actuals_from_pool`; `common.evaluation.metrics.{crps_from_quantiles,
  coverage,point_errors}`.

**Package shape (as built — shared root + per-feed-source variants, mirroring
the `like_day_model_knn` engine/variant split):**

```
modelling/da_models/linear_arx_da_price/
  __init__.py
  configs.py            # SHARED: estimator / window / band constants, gamma, quantiles
  trainer.py            # SHARED: per-hour ridge fit in asinh space, CV alpha, recency wts
  forecast.py           # SHARED: predict + residual-quantile bands, build_quantiles_table
  printers.py           # SHARED: like-day-style terminal report + metric helpers
  run.py                # SHARED: run_single_day(build_panel, variant_cfg, ...)
  features/
    __init__.py
    common.py           # SHARED: label/weather/gas/outage/calendar/interaction groups + assemble_panel
  pjm_hourly/           # variant 1 -- demand block = PJM RTO supply-demand + sub-zonal load
    __init__.py
    config.py           # variant knobs (MODEL_NAME, regions, backward toggle, description)
    builder.py          # build_panel(): fetch PJM feeds -> common.assemble_panel
    pipelines/
      __init__.py
      forecast_single_day.py   # thin wrapper -> run_single_day(...)
  meteo_hourly/         # variant 2 -- demand block = Meteologica regional supply-demand
    __init__.py
    config.py           #            (load/solar/wind/net-load x {RTO, MIDATL, WEST, SOUTH})
    builder.py
    pipelines/
      __init__.py
      forecast_single_day.py
```

The `meteo_hourly` variant is the home for the `rto_vs_regional_load.md`
hypothesis test (sub-zonal demand vs RTO aggregate as the Western-Hub
signal) and carries the sub-zonal *renewable* + *net-load* detail PJM's own
feeds don't publish. Adding a third variant = one new `<variant>/` directory
(config + builder + a 3-line pipeline wrapper); the estimator/bands/printers
never change.

**Output / API contract:** `run(...)` computes, prints, returns a dict —
no Postgres writes (publishing stays in `backend/modelling/`). Standard
table: `Date | Type | HE1-24 | OnPeak | OffPeak | Flat` with P10/P50/P90 +
a Forecast (mean/EV) row distinct from P50
([mean_vs_median.md](./mean_vs_median.md)); Actual / Error rows auto-appear
once DA settles. Headline metrics block: MAE, RMSE, rMAE vs d-7, pinball,
coverage, on-peak block-mean error.

**How it differs from / complements the existing models:**

| | like_day_model_knn | baseline_meteo_da_price | **linear_arx_da_price (new)** |
|---|---|---|---|
| Mechanism | weighted analog historical days | Meteologica DA-price ENS passthrough | learned feature→price regression |
| Extrapolates past seen regimes? | no (bounded by historical analogs) | only if Meteologica does | **yes** (along the fitted curve) |
| Interpretable? | analog dates | ENS members | **coefficients** |
| Multi-day strip | rolling-reference hack (weak) | per Meteologica horizon | **clean** (forward inputs per day) |
| Probabilistic | empirical analog quantiles | ENS spread | residual-quantile bands (→ QR Tier-2) |

**Standards:** `python-scripts` skill (`from __future__ import annotations`
first; stdout/stderr → UTF-8 in `run()`; ASCII-only output, `=`/`-`/`|`
separators; `sys.path` bootstrap; module-level constants, no argparse;
`__main__` one-liner; `quiet` keeps the return dict populated; run-id
artefacts). Family-import rule: imports from `da_models.common` only — never
from `like_day_model_knn*` or `baseline_meteo_da_price`.

**Out of scope for the build task:** `backend/modelling/` twin, frontend,
dbt, multi-day strip wrapper, quantile-regression variant, CQR/isotonic
post-processing, multi-window averaging — all Tier-2 follow-ups.

---

## (d) Open questions (defaults adopted unless the user says otherwise)

1. **What does "linear regression model" mean here?** (a) a clean point
   OLS/ridge ARX *baseline* distinct from the planned LASSO-QR; (b) build the
   LASSO-QR design from [lasso_model.md](./lasso_model.md) as a first-class
   `modelling/da_models/` family (since `backend/src/lasso_quantile_regression/`
   doesn't exist here); (c) elastic-net / quantile-regression with the
   Tier-1/2 improvements baked in from day one. **Default: (a)** — a ridge
   ARX baseline with asinh VST + recency weighting + residual-quantile bands,
   with LASSO/QR and CQR as documented Tier-2 items. The repo lacks a
   plain-linear reference point and (b)/(c) are larger builds that overlap the
   existing roadmap.
2. **Geography & per-hour structure.** **Default: Western Hub, 24 independent
   per-hour models.** (Carry RTO + all three Meteologica sub-zones as
   candidate load regressors; let shrinkage prune — do not drop SOUTH a
   priori.)
3. **Family directory name.** **Default: `linear_arx_da_price/`.**
4. **Bands in v1.** Genuine quantile regression vs point forecast + empirical
   residual-quantile band. **Default: residual-quantile band** (point ridge
   in asinh space, residuals from a rolling ~60–90-day holdout), flagged as a
   v1 simplification with QR as Tier-2.
5. **Backward LMP anchors.** Include a capped set (Friday-lag for Mondays) or
   ship with them off and rely purely on forward fundamentals? **Default:
   include a small capped set, behind a config toggle, with the
   backward/forward coef-mass ratio logged each fit** — so the
   [backward_vs_forward_looking.md](./backward_vs_forward_looking.md) failure
   mode is observable from run one.
