# Forward-Only KNN — Implementation Audit (Agent 2)

**Auditor scope:** walk the live code in `modelling/da_models/forward_only_knn/` (and its `da_models/common/` dependencies) and report what's there, what's not, and what's risky against the two spec docs (`pjm-like-day-research.md` and `forward_only_knn.md`).

---

## 1. Code map

A bullet tree of every file read, with one-line responsibility, public symbols, and line counts.

- `modelling/da_models/forward_only_knn/__init__.py` (5 lines) — package entry; re-exports `run_forecast` from the pipeline. Public: `run_forecast`.
- `modelling/da_models/forward_only_knn/settings.py` (19 lines) — env/.env bootstrap and module-level Azure Postgres credential constants; calls `logging.basicConfig` at import time. Public: `AZURE_POSTGRESQL_DB_*` strings.
- `modelling/da_models/forward_only_knn/configs.py` (295 lines) — schema/hub/cache constants, FEATURE_GROUPS dict, FEATURE_GROUP_WEIGHTS, day-type profiles, calendar Sun=0..Sat=6 helpers, and the `ForwardOnlyKNNConfig` dataclass. Public: `ForwardOnlyKNNConfig`, `resolved_feature_columns`, `_dow_key_for`, module constants.
- `modelling/da_models/forward_only_knn/features/__init__.py` (5 lines) — re-exports `build_pool`, `build_query_row`.
- `modelling/da_models/forward_only_knn/features/builder.py` (747 lines) — pool/query feature construction, per-region load/net-load/renewable aggregation, gas/outage/calendar/reserve-margin features, LMP labels. Public: `build_pool`, `build_query_row`.
- `modelling/da_models/forward_only_knn/similarity/__init__.py` (5 lines) — re-exports `find_twins`.
- `modelling/da_models/forward_only_knn/similarity/metrics.py` (70 lines) — NaN-aware Euclidean (`nan_aware_euclidean`), pool-only z-score (`fit_pool_zscore`/`apply_zscore`), analog-weight schemes (`compute_analog_weights`: inverse_distance/softmax/rank/uniform).
- `modelling/da_models/forward_only_knn/similarity/filtering.py` (166 lines) — DOW/holiday filter ladder (`apply_filter_ladder`), outage z-score regime filter (`outage_regime_filter`), date-proximity backfill (`ensure_minimum_pool`).
- `modelling/da_models/forward_only_knn/similarity/engine.py` (257 lines) — orchestrates filter→z-score→distance→rank pipeline. Public: `find_twins`, `find_analogs` (back-compat wrapper).
- `modelling/da_models/forward_only_knn/pipelines/__init__.py` (5 lines) — re-exports `run_forecast`.
- `modelling/da_models/forward_only_knn/pipelines/forecast.py` (1342 lines) — single-day pipeline; **most of the file is print/diagnostics** (provenance block, query feature table, top-5 analog comparison, config table). Public: `run_forecast`, `run`, `weighted_quantile`.
- `modelling/da_models/forward_only_knn/validation/__init__.py` (5 lines) — re-exports `PreflightReport`, `run_preflight`.
- `modelling/da_models/forward_only_knn/validation/preflight.py` (137 lines) — pool-size + per-group query-coverage + per-group pool-coverage check. Returns `PreflightReport`; never raises. Public: `PreflightReport`, `run_preflight`.
- `modelling/da_models/forward_only_knn/experiments/__init__.py` (0 lines, empty file).
- `modelling/da_models/forward_only_knn/experiments/registry.py` (92 lines) — named `ForwardOnlyKNNConfig` factories (`baseline`, `netload_3x`, `netload_4x_renewables_3x`, `low_load_low_gas`, `tight_system`, `outage_regime_off`, `with_reserve_margin`). Public: `BASELINE_WEIGHTS`, `CONFIG_REGISTRY`.
- `modelling/da_models/forward_only_knn/experiments/evaluate_configs.py` (437 lines) — daily named-config scoreboard; runs each config, captures Forecast/Actual/Error, optionally writes CSV, prints rolling summary. Public: `run_scoreboard`, `render_summary`.
- `modelling/da_models/forward_only_knn/experiments/compare_analog_selection.py` (343 lines) — pairwise diff of two configs' selected analog dates over a date or range; reports Jaccard/rank-shift/feature-alignment.
- `modelling/da_models/common/data/loader.py` (1071 lines) — parquet pattern matchers + dataset-specific normalizers for ~17 feeds (lmps_da, load_rt/forecast, fuel_mix, outages, solar/wind, weather, gas, meteologica regional, net_load_actual/forecast, day_gen_capacity, installed_capacity).
- `modelling/da_models/common/configs.py` (27 lines) — shared cache_dir, hours, hub default ("EASTERN HUB" – note divergence from forward_only_knn.HUB="WESTERN HUB"), DOW_GROUPS map.
- `modelling/da_models/common/calendar.py` (71 lines) — NERC holidays + `compute_calendar_row` returning Mon=0..Sun=6 day_of_week_number (different convention from forward_only_knn).
- `modelling/da_models/common/data/__init__.py` (32 lines) — re-exports loaders (note: `load_meteologica_*`, `load_net_load_*`, `load_installed_capacity`, `load_day_gen_capacity` are NOT re-exported here even though `forecast.py` and `builder.py` import them via `from da_models.common.data import loader` and `loader.load_…`).
- `modelling/da_models/common/__init__.py` (1 line, docstring only).
- `modelling/da_models/common/evaluation/metrics.py` (88 lines) — `pinball_loss`, `crps_from_quantiles` (approx), `coverage`, `point_errors` (mae/rmse/mape/bias). NOT imported anywhere in `forward_only_knn/` (verified by grep).
- `modelling/da_models/common/evaluation/__init__.py` (5 lines) — re-exports.

---

## 2. Spec ↔ implementation matrix

| Spec element | Status | Evidence | Notes |
|---|---|---|---|
| Target variable: hourly DA LMP at PJM Western Hub | Implemented | `configs.py:15` (`HUB = "WESTERN HUB"`), `builder.py:435-460` (`_build_lmp_labels`), `configs.py:138` (`LMP_LABEL_COLUMNS = lmp_h1..lmp_h24`) | Hub is configurable on the dataclass (`configs.py:211`). |
| Forecast horizon = D+1 default; D+N strip optional (spec §"Out of scope" later) | Partial | `configs.py:33` (`DEFAULT_TARGET_DATE = today + 1`), `forecast.py:1131-1135` (horizon-conditional gating) | `pipelines/strip_forecast.py` is **MISSING** despite the spec calling it out (`forward_only_knn.md:65`, `forward_only_knn.md:135-141`). Only single-day `run_forecast` exists. |
| Granularity = 24 hourly values (HE1..HE24) | Implemented | `configs.py:38` (`HOURS = 1..24`), `forecast.py:50-51` (peak/off-peak), `forecast.py:1264` (`_hourly_forecast_from_analogs`) | |
| Feature: Load level (per-region peak/avg/valley) | Implemented | `configs.py:56-60`, `builder.py:35-91` (`_load_daily_aggregates`), `builder.py:156-167` (pool), `builder.py:207-236` (query) | Per-region (RTO/MIDATL/WEST/SOUTH); query uses PJM forecast for RTO and Meteologica for the rest, falls back to RT actuals. |
| Feature: Load ramps (morning/evening/max) | Implemented | `configs.py:61-65`, `builder.py:71-91` | `morning_ramp = HE8 - HE5`, `evening_ramp = HE20 - HE15`, `ramp_max = max(diff)` per day. |
| Feature: Gas (M3 / TCO / TZ6 / DOM SOUTH daily avg) | Implemented | `configs.py:66-71`, `builder.py:410-432` (`_build_gas_features`), `builder.py:670-674` (query) | **Divergent from spec which calls for "Henry Hub" or "M3/Tetco" (research §4.1, line 402)**: no Henry Hub feature, only the four PJM citygates. Reasonable but worth noting. |
| Feature: Outages (total / forced / forced share) | Implemented | `configs.py:72-76`, `builder.py:268-316` | Pool uses realized; query uses latest `forecast_execution_date`. |
| Feature: Renewable level (per-region solar/wind/renewable daily avg) | Implemented | `configs.py:77-81`, `builder.py:319-407` | RTO uses PJM system-wide solar/wind feeds; other regions Meteologica. |
| Feature: Net load (per-region avg/peak/valley + ramps) | Implemented | `configs.py:82-88`, `builder.py:170-187` (pool), `builder.py:239-265` (query) | |
| Feature: Calendar DOW (is_weekend, dow_sin/cos) | Implemented | `configs.py:89-93`, `builder.py:484-496` | DOW group filter is hard-filtered separately. |
| Feature: Reserve margin / scarcity ratio | Implemented (off by default) | `configs.py:99` (group), `configs.py:113` (`weight=0.0`), `builder.py:534-557` (`_compute_reserve_margin_pct`), `registry.py:72-81` (`with_reserve_margin` config) | Default weight is 0; only opt-in via the registry's `with_reserve_margin` config bumps to 3.0. |
| Feature: Temperature / HDD / CDD | **Missing** | searched `configs.py` `FEATURE_GROUPS`, `builder.py`, no temp aggregation; `loader._normalize_weather_hourly` exists at `loader.py:468-525` but is never called from `builder.py` | Spec lists temperature as **Tier 1** (research §4.1, line 403). MVP plan also lists `weather_level`/`weather_hdd_cdd` as in-scope (forward_only_knn.md:96). Loader is wired, builder is not. |
| Feature: Lagged DA LMP (t-1, t-7, prior-week same-DOW) | **Missing** | grep for `lmp_h` shows only label generation in `builder.py:435-460`; no lag features built | Spec §3.3 prioritizes lagged LMP as a "Tier 1/2" matching feature. The "forward-only by design" note in `configs.py:51-54` and `builder.py:12-14` argues these are deliberately excluded as backward-looking. **Divergent if the spec intent included lags.** |
| Feature: Congestion / loss components | **Missing** | only `lmp` (total) is loaded — `loader.py:198-200` selects `("lmp","lmp_total","da_lmp_total","da_lmp")` | Spec §1.1 mentions congestion as a Tier 3 refinement; not implemented. |
| Feature: Net imports/exports, reserve margin, prior-week same-day LMP | Partial | only reserve margin is implemented (off by default); other Tier 3 features missing | |
| Similarity metric: Euclidean (NaN-aware) | Implemented | `metrics.py:7-18` (`nan_aware_euclidean`), `engine.py:72-77` | Per-group Euclidean over z-scored values. |
| Similarity: Cosine / Pearson / Mahalanobis (spec offers as alternatives) | Missing | `metrics.py` only implements Euclidean | Spec §3.3 mentions cosine/Mahalanobis as enhancement options; not implemented. |
| Per-group weighted blend | Implemented | `engine.py:60-82` (`_compute_distances`), normalized by `weight_sum` per row | Note: the per-row distance is `sum(w_g * dist_g) / sum(w_g over groups with valid features)`. So a row missing one group still gets a finite distance based on whatever groups it has — see leakage/weighting risk below. |
| Feature scaling: pool-only z-score (no leakage) | Implemented | `metrics.py:21-43` (`fit_pool_zscore`), `engine.py:68-70` | Critically, the means/stds are fit on the **filtered candidate pool only**, not the full pool — see Risks. |
| Candidate pool: pre-filter strict→relaxed by DOW group + holiday | Implemented | `filtering.py:36-80` (`apply_filter_ladder`) | Ladder: exact_dow+holiday → exact_dow → dow_group+holiday → dow_group → no filter. |
| Candidate pool: season window (±60 days circular) | Implemented | `engine.py:112-122` | Default `FILTER_SEASON_WINDOW_DAYS=60` (`configs.py:119`). |
| Candidate pool: outage regime z-score filter (±1.5 std) | Implemented (extra not in MVP spec) | `filtering.py:83-138`, `engine.py:144-176` | This is **extra** — MVP spec didn't ask for it. Reasonable enhancement. |
| Candidate pool excludes target date and future | Implemented | `engine.py:107` (`work[work["date"] < target_date]`) | Strict less-than; correct for the live D+1 case. **For backtest dates with actuals available in pool, it correctly drops the target row but does NOT exclude future-relative dates between target_date+1 and today** — see leakage section. |
| K (number of analogs) — default 20 | Implemented | `configs.py:34` (`DEFAULT_N_ANALOGS = 20`), spec calls for 20 | Day-type override drops to 12 (Sat) / 10 (Sun) (`configs.py:147-170`). |
| Neighbor weighting: inverse distance (default) | Implemented | `metrics.py:46-70` (`compute_analog_weights`) — `1/d²` not `1/d` | **Divergent**: spec uses `1/distance` (research §3.3, lines 326-328 and §5.2 line 484). Code uses `1.0 / np.square(np.maximum(distances, 1e-8))` at `metrics.py:65`. Inverse-square is more aggressive — closer analogs dominate even more. |
| Neighbor weighting: softmax / rank / uniform alternatives | Implemented (extra) | `metrics.py:55-63` | Not in MVP spec but available. |
| Neighbor aggregation: weighted mean → point forecast | Implemented | `forecast.py:211` (`np.average(values, weights)`) | Per-hour. |
| Neighbor aggregation: weighted quantiles | Implemented | `forecast.py:83-90` (`weighted_quantile` via cumulative interp), `forecast.py:212-213` | Default quantiles `[0.10, 0.25, 0.50, 0.75, 0.90]` (`configs.py:39`). |
| Forward-only constraint enforcement (no leakage) | Partial | feature design uses forecasts not actuals on query side; pool excludes target date | But: query-side forecast loaders take the **latest published** forecast vintage rather than the issuance-time vintage available at D-1 morning. See §3 details. |
| Train/test split | **Missing as a concept** | the package is purely live/inference; there's no dedicated split module | `pipelines/forecast.py` runs against today's data. The "test" on a historical date in `evaluate_configs.py:114` does `run_forecast(target_date=…, config=cfg)` and `_actuals_from_pool` checks if labels exist; no formal train/test boundary because there is no model fitting. |
| Evaluation metrics & benchmarks | Partial | `common/evaluation/metrics.py:14-87` defines pinball/crps/coverage/MAE/RMSE — but **none are imported by any code in `forward_only_knn/`** (grep confirms). The scoreboard only computes flat/onpk/offpk MAE inline at `evaluate_configs.py:182-186`. | No CRPS, no pinball loss, no calibration/coverage report, no benchmark vs LEAR/LightGBM/naive seasonal anywhere. |
| Post-processing | None | no normalization/denormalization, no calibration, no conformal layer | Quantiles come straight from analog pool weighted-quantile. No Schaake shuffle, no KDE smoothing, no conformal calibration — these are all spec phase-2 enhancements that are not implemented. |
| Backtesting harness, CV tuning | **Missing (intentionally)** | not present | MVP plan (`forward_only_knn.md:25, 191-195`) explicitly defers these; the scoreboard is the closest thing but it's a daily-incremental forward-walk, not a backtest. |
| Holiday handling | Partial | `filtering.py:13-19` + ladder respects NERC holidays | Spec §5.3 calls for distinct-holiday dummies / discrete-interval moving seasonalities — not implemented; only a binary holiday/non-holiday filter. |

---

## 3. Data leakage audit (CRITICAL)

For each feature consumed at prediction time, I classify as forward-safe (✅), risky (⚠️), or leaks (❌).

### Query-side features (target day T = today + 1)

| Feature | Source loader | Cutoff respected? | Verdict | Evidence |
|---|---|---|---|---|
| `load_daily_*_<region>` (RTO) | `loader.load_load_forecast` | Loader pulls latest parquet matching `pjm_load_forecast_hourly_da_cutoff*` if available, else `pjm_load_forecast_hourly`. Pattern name suggests a DA-cutoff snapshot is used when present. | ⚠️ Risky — depends on parquet naming discipline | `loader.py:22-28` lists the priority list; first match wins. If only the non-cutoff parquet exists, the loader silently uses the *latest* forecast vintage rather than the morning-of-D-1 vintage. |
| `load_daily_*_<region>` (MIDATL/WEST/SOUTH) | `loader.load_meteologica_load_forecast` | `_normalize_meteologica_regional` keeps highest `forecast_rank` per (region, date, hour). | ⚠️ Risky — keeps **latest** rank, which is the most recently published forecast, not the D-1 issuance forecast | `loader.py:619-629`. For a backtest on a past T, this means we may be using a Meteologica forecast issued *after* T's DA market closed. |
| `solar_daily_avg_<region>` / `wind_daily_avg_<region>` (RTO) | `loader.load_solar_forecast` / `load_wind_forecast` | Same parquet-name priority: `pjm_solar_forecast_hourly_da_cutoff*` first. | ⚠️ Risky — same as load forecast | `loader.py:37-48`. |
| `solar_daily_avg_<region>` / `wind_daily_avg_<region>` (other regions) | Meteologica solar/wind | Latest rank kept. | ⚠️ Risky | `loader.py:619-629`. |
| `net_load_*_<region>` (RTO) | `loader.load_net_load_forecast` | Pattern list includes `pjm_net_load_forecast_hourly_da_cutoff` and falls back to `meteologica_pjm_net_load_forecast_hourly_da_cutoff` then non-cutoff. | ⚠️ Risky — fallback to Meteologica's latest-rank parquet erases the cutoff | `loader.py:99-105`, `loader.py:687-714`. |
| `net_load_*_<region>` (other regions) | Meteologica net-load | Latest rank | ⚠️ Risky | Same. |
| `gas_*_daily_avg` | `loader.load_gas_prices_hourly` (ICE) | Loader takes the parquet, no vintage filtering. ICE gas price for D+1 is naturally known at T-1 close (next-day market). | ✅ Forward-safe for D+1, but only if the ICE feed is updated through T-1; horizon gate in `configs.py:132` (`GAS_FEATURE_MAX_HORIZON_DAYS=1`) enforces this for D+2 and beyond by zeroing the weight. | `forecast.py:1132`. |
| `outage_*_mw` (query side) | `loader.load_outages_forecast` | `_build_outage_features_query` sorts by `forecast_execution_date` descending and takes the *latest* forecast for that delivery date. | ⚠️ Risky — for backtest, this picks the most recently published outage forecast for an old delivery date, which is typically the *closer-to-real-time* (more accurate) forecast and not the one you'd actually have at the D-1 morning DA cutoff. | `builder.py:305-307`. |
| `is_weekend`, `dow_sin`, `dow_cos`, `is_nerc_holiday` | calendar deterministic from date | Always known. | ✅ Forward-safe | `builder.py:484-496`. |
| `reserve_margin_pct` (query side) | `outage_total_mw` (forecast, ⚠️ above) + `load_daily_peak_rto` (forecast, ⚠️ above) + EA monthly capacity | Inherits the riskiness of both outage and load forecasts; EA capacity is published forward through ~2030 so OK. | ⚠️ Risky — same reason as inputs | `builder.py:721-726`. |

### Pool-side features (historical day D, label = realized DA LMP)

Pool features are realized actuals. The `lmp_h*` labels come from realized DA LMP for D (`builder.py:435-460`). This is **correct and consistent**: the analog "what followed under realized D conditions" view.

A subtle point: pool features are realized but the query is forecasts. This is the well-known "drift" between offline and online — analogs whose realized state was X are matched against a forecast saying X, but operational reality is that the forecast has its own noise. This is methodologically OK for analog matching (the spec endorses this approach) but is worth flagging.

### Pool exclusion of target date

`engine.py:107` does `work[work["date"] < target_date]`. This drops the target day but **does not drop dates between `target_date + 1` and `today` when running a backtest on a historical target**. For example, if `target_date = 2026-01-15` and you run the script today (2026-04-28), the pool legitimately contains dates 2026-01-16 through 2026-04-27 with realized labels — these can become analogs for `target_date`, which **is leakage** because the model would not have those labels available at issuance time on 2026-01-14 evening. There is a `recency_half_life_days=730` penalty (`engine.py:189`) that scales distance with age, but it does **not** exclude future-relative dates.

### Z-score scope

`fit_pool_zscore` runs on `pool_vals` after the calendar+season+outage filters (`engine.py:65-70`). This is **pool-only** as the spec requires — query is transformed using the fit-on-pool stats (`engine.py:70`). ✅ No leakage from query into the scaler.

However, fitting per-call on the filtered pool means the z-score reference distribution **changes** with target date, season window, and outage regime — analogs at the boundary of the season window get scaled against a different reference than analogs near the target's day-of-year. This is statistically defensible (it normalizes within the local regime) but worth flagging in case downstream interpretation assumes a fixed reference.

### Per-row distance normalization (per-feature dropping)

In `_compute_distances` (`engine.py:60-82`), per-row distance is `weighted_sum / weight_sum`, where `weight_sum` only counts groups with `n_valid_dims > 0`. A pool row with NaN in 3 of 7 active groups will be compared on 4 groups, then normalized by the sum of those 4 weights. **A row missing the highest-weight group can effectively beat a row that has full coverage**, because its averaged distance becomes deceptively low. This is not exactly leakage but it's a serious silent-failure risk during the period before EA capacity / Meteologica data was available — see §7.

---

## 4. Train/test split correctness

There is **no formal train/test split** because the package does not fit any model parameters — it's a non-parametric KNN. Functional split logic:

- **Pool exclusion of `target_date`** is at `similarity/engine.py:107`: `work = work[pd.to_datetime(work["date"]).dt.date < target_date]`. Strict less-than. Implementation correct for live D+1.
- **Backtest "test" set** is implicit: when running `evaluate_configs.py` on a past date, the actuals are read from `pool[pool["date"] == target_date]` at `forecast.py:146-156`. The pool is the same pool used for analog selection — only the target row is masked out via the strict `<` filter.
- **No embargo / gap.** There is no buffer between target_date and the most recent allowed analog date. For PJM where realized LMP data publishes within days, this means the most-recent analog could be `target_date - 1`, whose realized DA LMP at issuance time of `target_date - 1` close was **not yet known** to the issuer of the `target_date` forecast at T-1 morning. In practice for the live use case this is fine (analog from the day before is usable since DA settled before the next day's DA cutoff). For an offline backtest, ⚠️ the pool can contain dates in `(target_date, today]` that leak future information — see §3.
- **Backtesting style:** The closest thing is `evaluate_configs.py` which is a **forward-walking single-date scoreboard** — each day's run uses whatever was in the parquet cache at that moment. There is **no rolling-window backtest** of any kind. Spec MVP defers backtesting (`forward_only_knn.md:25, 191-195`), so this is consistent with stated scope but means we cannot answer questions like "how would this model have performed across 2024" without writing a separate harness.

---

## 5. Config drift between experiments

| Experiment | What differs from baseline | Hypothesis tested |
|---|---|---|
| `baseline` (`registry.py:33`) | nothing | baseline reference |
| `netload_3x` (`registry.py:37`) | `net_load: 2.0 → 3.0` | does up-weighting net load improve fits? |
| `netload_4x_renewables_3x` (`registry.py:41`) | `net_load: 2.0 → 4.0`, `renewable_level: 1.5 → 3.0` | renewable + net-load combo emphasizing decarbonization signal |
| `low_load_low_gas` (`registry.py:47`) | `load_level: 3.0 → 2.0`, `gas_level: 2.0 → 1.0` | what if load and gas are over-weighted? |
| `tight_system` (`registry.py:53`) | `load_level: 3.0 → 2.0`, `gas_level: 2.0 → 1.0`, `renewable_level: 1.5 → 3.0`, `net_load: 2.0 → 4.0` | combines net-load/renewables emphasis with reduced load/gas |
| `outage_regime_off` (`registry.py:64`) | turns off outage z-score filter (`apply_outage_regime_filter=False`) | control for whether the filter helps |
| `with_reserve_margin` (`registry.py:72`) | adds `target_reserve: 0.0 → 3.0` to baseline | tests whether reserve margin moves analog selection |

**Drift hazards:**

1. `BASELINE_WEIGHTS` in `registry.py:16-24` is **pinned independently** from `configs.FEATURE_GROUP_WEIGHTS` (`configs.py:102-114`). They currently match, but if someone edits one and forgets the other, the "baseline" interpretation in the scoreboard CSV will silently change. The dev comment at `registry.py:6-9` is aware of this: "configs.FEATURE_GROUP_WEIGHTS can drift without invalidating the historical interpretation of the baseline row."
2. Day-type profiles (`configs.py:147-170`) **mutate** configs at runtime via `with_day_type_overrides` (`configs.py:268-295`). Saturday/Sunday targets get reduced `n_analogs` (12/10), changed `season_window_days`, and shallow-merged `feature_group_weights`. The scoreboard's `_config_fingerprint` (`evaluate_configs.py:86-103`) uses the **pre-override** config to compute the hash. **For weekend backtests, the recorded config_hash does not match the actual weights used.** This causes silent drift in the CSV's "config_name → config_hash" mapping when the same name is run on a weekday vs weekend.
3. `_score_one` references an undefined name `log_path` at `evaluate_configs.py:126`. This will crash with `NameError` whenever a date has no actuals — that branch logs a warning and references a variable that's never bound in the file. Verified by grep (only one occurrence). **This is a latent bug.**
4. `compare_analog_selection.py:283` defaults `--configs` to `["baseline", "with_reserve_margin"]` — a hardcoded experiment.
5. `compare_analog_selection.py:328-329` rebuilds pool and query inside the per-target loop **even though** `_run_quiet` just ran a forecast that already built them. Each target date thus rebuilds pool 3x (twice via run_forecast for cfg_a/cfg_b and once explicitly). Performance, not correctness.
6. `experiments/__init__.py` is empty (0 bytes), so `from da_models.forward_only_knn.experiments import …` fails for any direct attribute import. Scripts work because they use absolute paths.
7. `evaluate_configs.py:301` has CSV writing **commented out** (`# _append_rows(rows)`) — the daily scoreboard runs but persists nothing. The summary mode at `render_summary` reads from a file that is never being written. In-progress work flag.

---

## 6. Code present but not in spec (extras)

- **Outage regime z-score filter** (`filtering.py:83-138`, on by default) — explicitly mirrors a feature from the legacy `helioscta-pjm-da` codebase per the doctring; not in `forward_only_knn.md`.
- **Day-type profile overrides** for weekday/Sat/Sun (`configs.py:147-170`, `configs.py:268-295`) — adds per-day-type weight/window/n_analogs overrides; not in MVP plan.
- **Reserve margin feature** (`configs.py:99`, `builder.py:534-557`) — fuel-aware EA-installed-capacity-based scarcity ratio; off by default.
- **Recency penalty** (`engine.py:187-190`) — multiplies distance by `1 + age_days / half_life`; not in spec.
- **Weighted-quantile output table + bands** (`forecast.py:1278-1300`) — empirical quantile printing; aligns with spec phase-1 recommendation but presented inline rather than as a separate module.
- **Preflight system** (`validation/preflight.py`) — pool-coverage and query-coverage checks that drive auto-disabling of weak feature groups (`forecast.py:176-192`). Effectively a soft fallback for bad data days.
- **Provenance + diagnostic printing** — the bulk of `forecast.py` is print code: input feed paths/timestamps, query feature table, top-5 analog comparisons, daily/hourly delta tables. Not in spec; useful for daily ops review.
- **Per-region net-load and renewables features** with PJM-RTO + Meteologica regional fallback — not explicitly in spec (which speaks at the system level).
- **`compute_analog_weights` schemes:** softmax, rank, uniform alternatives (`metrics.py:46-70`).
- **Backward-compatible `find_analogs` wrapper** (`engine.py:229-258`) — for some legacy caller, but no current callers found inside this package.

---

## 7. Implementation risks (ranked)

1. **Inverse-square (not inverse) distance weighting silently diverges from spec at `metrics.py:65`.** Spec uses `1/distance`; code uses `1/distance²`. With small distances dominating, a single very-close analog can absorb >50% of the weight and the forecast collapses toward that one day. *Mitigation:* either switch to `1/distance` (matches spec) or document and pin this as the project's deliberate choice.
2. **Latent NameError crash in scoreboard at `evaluate_configs.py:126`** — references undefined `log_path`. Whenever a backtest target has no actuals, the script will crash instead of warning. *Mitigation:* remove or define the variable.
3. **Forecast-vintage leakage in backtests** at multiple loader sites (`loader.py:79-105`, `builder.py:305-307`). The query-side load/renewables/outage forecasts use the **latest published** forecast for a historical delivery date, not the D-1-morning vintage. For any historical date in the parquet, this means "the model gets a forecast that was actually issued days later" — reading future information. The pattern names `*_da_cutoff*` suggest a snapshot exists for some feeds, but the fallback path silently bypasses it. *Mitigation:* add an explicit `vintage_at` parameter to forecast loaders that filters by issuance datetime; fail loudly when no cutoff snapshot is available.
4. **Pool does not exclude future-relative dates** (`engine.py:107`). When backtesting `target_date < today`, dates in `(target_date, today]` remain in the pool with realized labels and can become analogs. *Mitigation:* add an `as_of_date` parameter that drops `pool[pool["date"] >= as_of_date]`; default to `today` for live runs and to `target_date` for backtests.
5. **Day-type profile silently invalidates `config_hash`** at `evaluate_configs.py:86-103`. Saturday/Sunday runs produce a hash computed from pre-override weights even though `n_analogs`, `season_window_days`, and feature weights are overridden inside `run_forecast` (`forecast.py:1129`). *Mitigation:* compute the fingerprint **after** `with_day_type_overrides`, or persist both pre- and post-override hashes in the CSV.
6. **Per-row distance averaging masks missing groups** (`engine.py:80-82`). A pool row missing the highest-weight group is normalized over a smaller `weight_sum` and may win the ranking with fewer dimensions checked. *Mitigation:* require a minimum coverage threshold (e.g. ≥80% of active groups) to be eligible, or penalize incomplete rows.
7. **Pool size requirement is satisfied even if the rows are mostly NaN-filled** (`features/builder.py:629`, `_validate_pool` at `forecast.py:280-288`). `_ensure_columns` fills missing columns with NaN, then the pool reports its row count; preflight only checks pool size, not feature density per row. Combined with risk #6 this can produce extremely biased analog sets without any warning. *Mitigation:* extend `_validate_pool` and `run_preflight` to require minimum non-null density per row.
8. **Temperature / HDD / CDD entirely missing from the feature builder** despite being listed in spec MVP (`forward_only_knn.md:96`) and Tier-1 in research (`pjm-like-day-research.md:403`). The loaders for weather are wired (`loader.py:468-525`). *Mitigation:* add `_build_weather_features_*` functions and a `weather_level` group to FEATURE_GROUPS.
9. **No formal evaluation surface** (`common/evaluation/metrics.py` is unused by `forward_only_knn/`). The scoreboard tracks flat/onpk/offpk MAE only; CRPS, pinball loss, coverage, calibration are not measured. Hard to judge probabilistic forecast quality. *Mitigation:* import and use `crps_from_quantiles`, `pinball_loss`, `coverage` in the scoreboard.
10. **No `strip_forecast.py`** despite the spec calling it out as in-scope (`forward_only_knn.md:65, 135-141`). D+N strip use case is unsupported. *Mitigation:* implement the multi-day loop wrapper.
11. **Logger configured at import time** (`settings.py:13`). `logging.basicConfig(level=logging.INFO)` runs whenever `forward_only_knn.configs` is imported (because `configs` does NOT import settings, but other modules likely do — verified `pipelines/forecast.py` uses a `utils.logging_utils` setup instead, but `settings.py` import path is fragile). Risk of log-config conflicts when this package is imported into a larger app. *Mitigation:* move `basicConfig` into a dedicated init function called only from CLI entrypoints.
12. **Mixed DOW conventions across modules.** `da_models/common/calendar.py:60-69` returns Mon=0..Sun=6 day_of_week_number. `forward_only_knn/configs.py:42-46` and `builder.py:484-496` use Sun=0..Sat=6 by remapping. Two modules implement two different conventions; bug surface if anyone reads `compute_calendar_row` directly without the remapping wrapper. *Mitigation:* unify on one convention or rename the fields to make the convention explicit (e.g. `dow_sun0`).
13. **Hub default mismatch.** `forward_only_knn/configs.py:15` sets `HUB="WESTERN HUB"` while shared `common/configs.py:13` says `HUB="EASTERN HUB"`. Right now `forward_only_knn` always uses its own constant, but a refactor that pulls from `common.HUB` would silently switch hubs.
14. **`compare_analog_selection.py:328-329` rebuilds pool inside the per-date loop** after `_run_quiet` already did. 3x pool rebuild per target date when both configs and the explicit fetch are counted. Performance only; no correctness impact, but for ranges of >30 days it becomes a real wait.
15. **`experiments/__init__.py` is empty** so package-level imports like `from da_models.forward_only_knn.experiments import CONFIG_REGISTRY` fail; only fully-qualified module paths work.
16. **Scoreboard CSV writing is commented out** (`evaluate_configs.py:301`). `render_summary` reads from a file that's never being populated; running `--summary` against a live system will print "No scoreboard found." This is in-progress work, but if anyone trusts the CLI usage example in the docstring (`evaluate_configs.py:13-14`) they'll be confused.

---
