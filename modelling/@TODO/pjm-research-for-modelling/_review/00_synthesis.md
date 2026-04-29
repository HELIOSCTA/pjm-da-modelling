# 00 — Synthesis: Forward-Only KNN Review

Source documents (read in full):
- `01_literature_review.md` — spec checklist + ambiguities (Agent 1)
- `02_implementation_audit.md` — code audit + leakage check + ranked risks (Agent 2)
- `03_workflow_and_gaps.md` — pipeline trace, node annotations, gap list (Agent 3)

---

## 1. TL;DR

- The MVP forecast path is structurally complete and broadly faithful to the spec: per-region load/net-load/renewables/outage/gas/calendar features, NaN-aware z-scored Euclidean distance, fallback ladder, top-N inverse-distance weighting, weighted mean/quantile aggregation, and a preflight that auto-disables low-coverage groups (see `02_implementation_audit.md` §1, `03_workflow_and_gaps.md` §1).
- Two correctness landmines: distance weighting is `1/d^2` not the spec's `1/d` (`metrics.py:65`), and backtests leak future information through (a) latest-vintage forecast loaders (`loader.py:79-105`, `builder.py:305-307`) and (b) a pool that does not exclude dates between target+1 and today (`engine.py:107`).
- One in-scope spec deliverable is missing: weather features (`weather_level`, `weather_hdd_cdd`) and `pipelines/strip_forecast.py` (`forward_only_knn.md:94, 135-141`). Loaders for weather are wired but never called from `builder.py` (`03_workflow_and_gaps.md` §4).
- Several silent-failure paths (per-row distance averaging that masks missing groups, `_safe_load` swallowing loader errors, day-type profiles invalidating `config_hash`, scoreboard CSV write commented out, latent `NameError` in scoreboard) make the system look healthy when it is not (`02_implementation_audit.md` §7 risks 2, 5, 6, 7; `03_workflow_and_gaps.md` §4).
- Modeler must decide before more code lands: `1/d` vs `1/d^2`, holiday-as-filter vs holiday-via-ladder, weighted-quantile convention, mean vs P50 as the headline forecast, vintage policy for forecast feeds, and whether to keep the recency formula linear or rename it (`01_literature_review.md` §4 ambiguities; `02_implementation_audit.md` §3 leakage section).

---

## 2. Spec ↔ Implementation matrix

| Spec element | Status | Evidence | Risk |
|---|---|---|---|
| Target = hourly DA LMP at PJM Western Hub | Implemented | `configs.py:15`, `builder.py:435-460`, `configs.py:138` | None — hub fixed to WESTERN HUB |
| Horizon = D+1 default; D+N strip in scope | Partial | `configs.py:33`, `forecast.py:1131-1135` (D+1 works); `pipelines/strip_forecast.py` missing per `forward_only_knn.md:135-141` | Medium — strip path not delivered, single-day only |
| Granularity = 24 hourly values (HE1..HE24) | Implemented | `configs.py:38`, `forecast.py:1264` | None |
| Feature: load level (per-region peak/avg/valley) | Implemented (extended to per-region) | `builder.py:35-91, 156-167, 207-236`; `configs.py:56-60` | Low — query falls back to RT actuals if forecast missing (`builder.py:221`) |
| Feature: load ramps (morning/evening/max) | Implemented | `builder.py:71-91`; `configs.py:61-65` | None |
| Feature: weather level + HDD/CDD | Missing | Loader exists at `loader.py:468-525, 994-1018`; no caller in `builder.py`; spec at `forward_only_knn.md:96` | High — Tier-1 spec feature absent |
| Feature: gas level (Henry Hub or M3/Tetco) | Divergent | `configs.py:66-71`, `builder.py:410-432`, `builder.py:670-674`; spec calls Henry Hub (`pjm-like-day-research.md:402`), code uses four PJM citygates only | Low — defensible but worth noting |
| Feature: ICE forward level (horizon-gated) | Missing | Spec lists optional `ice_forward_level` (`forward_only_knn.md:98`); no group in `configs.FEATURE_GROUPS` | Low — spec marks optional |
| Feature: calendar DOW (sin/cos + is_weekend) | Implemented | `builder.py:484-496`; `configs.py:89-93` | Low — Sun=0..Sat=6 convention diverges from `common/calendar.py:60-69` Mon=0..Sun=6 |
| Feature: outages (total/forced/forced_share) | Implemented (extra: also drives a hard filter) | `builder.py:268-316`; `filtering.py:83-138`; double-use noted in `03_workflow_and_gaps.md` §3 #3 | Medium — outage signal counts twice (filter + distance) |
| Feature: net load (per-region) | Implemented | `builder.py:170-187, 239-265`; `configs.py:82-88` | Low |
| Feature: renewables (solar/wind per region) | Implemented (deferred per spec) | `builder.py:319-407`; `configs.py:77-81`; spec defers (`forward_only_knn.md:99, 192`) | Low — spec is out of date relative to code |
| Feature: reserve margin / scarcity ratio | Implemented (off by default) | `configs.py:99,113`; `builder.py:534-557`; `registry.py:72-81` | Low |
| Feature: lagged DA LMP / prior-week-same-DOW | Missing (intentionally per "forward-only by design") | No `lmp_h*` lag features; only labels at `builder.py:435-460`; design note `builder.py:12-14` | Low — design choice, but spec does not explicitly drop |
| Feature: congestion / loss components | Missing | Loader selects total LMP only at `loader.py:198-200` | None — Tier 3 |
| Similarity metric: NaN-aware Euclidean per group | Implemented | `metrics.py:7-18`; `engine.py:60-82` | Low — scalar metric not pinned by spec (`forward_only_knn.md:111`); code chooses Euclidean |
| Per-group weighted blend | Implemented (with hazard) | `engine.py:60-82` divides by `weight_sum` of *valid* groups only | High — rows missing high-weight groups can win on partial coverage |
| Pool-only z-score scaling | Implemented | `metrics.py:21-43`; `engine.py:65-70` | Low — fit-on-filtered-pool means reference shifts with target date |
| Pool: calendar filter ladder (strict→relaxed) | Implemented (slight rung mismatch) | `filtering.py:36-80`; spec ladder at `forward_only_knn.md:39-42` | Low — extra `dow_group+holiday` rung beyond spec example |
| Pool: season-window ±60d | Implemented (extra) | `engine.py:112-122`; `configs.py:119` | None — useful enhancement |
| Pool: outage-regime z-score filter | Implemented (extra, on by default) | `filtering.py:83-138`; `engine.py:144-176`; not in spec | Medium — re-uses outage signal as filter and feature |
| Pool excludes target_date | Implemented (incomplete for backtest) | `engine.py:107` strict `<`; does not drop dates in `(target, today]` | Critical — leakage in any historical backtest |
| Min pool size = 150 with date-proximity backfill | Implemented | `configs.py:147`; `filtering.py:141` (`ensure_minimum_pool`) | Medium — backfill ignores DOW/holiday |
| K = 20 (default) | Implemented | `configs.py:34`; weekend overrides 12/Sat, 10/Sun (`configs.py:147-170`) | Low |
| Neighbor weighting: inverse distance | Divergent (`1/d^2` not `1/d`) | `metrics.py:65` uses `1.0 / np.square(...)`; spec/research uses `1/d` (`pjm-like-day-research.md:483-484`) | High — closer analogs absorb >50% of weight; forecast collapses on one day |
| Neighbor weighting: alternative methods | Implemented (extra, untested) | `metrics.py:46-70` (softmax/rank/uniform); no registry config exercises them | Low |
| Aggregation: weighted mean → point forecast | Implemented | `forecast.py:211` | Low — spec also lists P50 quantile; convention not pinned |
| Aggregation: weighted quantiles | Implemented | `forecast.py:83-90, 212-213`; `configs.py:39` | Medium — weighted-quantile convention not specified by spec (`forward_only_knn.md:132`) |
| Recency penalty (optional, form unspecified) | Divergent | `engine.py:187-190` is linear `1 + age/half_life`; variable named `recency_half_life_days` (`configs.py:129`) suggests exponential | Medium — name/formula mismatch is a maintenance hazard |
| Forward-only / as-of discipline (Guardrail 1) | Partial | Pool exclusion at `engine.py:107`; query loaders take latest-vintage forecasts (`loader.py:79-105`, `builder.py:305-307`) | Critical — backtests are not as-of |
| Horizon feature gating | Implemented | `forecast.py:1131-1141`; `configs.py:132` (`GAS_FEATURE_MAX_HORIZON_DAYS=1`) | Low |
| No silent zero-fill for missing groups | Implemented (with caveat) | NaN-aware in `metrics.py`; preflight forces `calendar_dow >= 1.0` fallback at `forecast.py:188-191` | Medium — silent fallback is opaque |
| Train/test split | N/A (non-parametric KNN; deferred) | Spec defers backtesting (`forward_only_knn.md:25, 191-195`) | None for MVP |
| Evaluation metrics & benchmarks | Partial | `common/evaluation/metrics.py:14-87` defines pinball/CRPS/coverage but never imported; scoreboard tracks flat/onpk/offpk MAE inline only (`evaluate_configs.py:182-186`) | Medium — metrics shipped but unused |
| Post-processing (clipping/calibration/conformal) | Missing | None implemented; spec defers Method B/C | None for MVP |
| Output schema compatible with like-day API | Implemented | `forecast.py:115` (`_build_output_table`); spec at `forward_only_knn.md:133` | Low — schema is by-reference, may drift |
| Determinism (stable sort by `(distance, date)`) | Implemented | `engine.py:199` | None |
| Fallback ladder ordering matches spec | Partial | `filtering.py:36-80` adds `dow_group+holiday` rung not in spec example (`forward_only_knn.md:39-42`) | Low |

(34 rows; every cell populated.)

---

## 3. Top risks

Ranked worst-first. Every entry has a `file.py:line` pointer.

1. **Forecast-vintage leakage in backtests.** Query-side load/renewables/outage/net-load loaders take the latest-published forecast for a historical delivery date, not the D-1 issuance vintage — `loader.py:79-105` (parquet-priority fallback erases `*_da_cutoff*` snapshots), `builder.py:305-307` (outage uses max `forecast_execution_date`), `loader.py:619-629` (Meteologica keeps max `forecast_rank`). Mitigation: add a required `vintage_at` argument to forecast loaders; fail loudly when no cutoff snapshot exists. (`02_implementation_audit.md` §3 row table + risk #3.)
2. **Pool does not exclude future-relative dates** at `engine.py:107` — strict `< target_date` only. For any backtest target before today, dates in `(target_date, today]` legitimately enter the pool with realized labels. Mitigation: add `as_of_date` knob; default to `today` for live, `target_date` for backtest. (`02_implementation_audit.md` §3 + risk #4.)
3. **Inverse-square distance weighting silently diverges from spec** at `metrics.py:65` (`1.0 / np.square(np.maximum(distances, 1e-8))`). Spec/research uses `1/distance`. A single very-close analog can absorb >50% of weight and collapse the forecast. Mitigation: switch to `1/d` or pin `1/d^2` as a deliberate project choice (with rationale). (`02_implementation_audit.md` §7 risk #1.)
4. **Per-row distance averaging masks missing groups** at `engine.py:80-82`. `weighted_sum / weight_sum` only counts groups with valid dims, so a row missing the highest-weight group can beat a row with full coverage. Mitigation: require minimum coverage threshold (e.g. ≥80% of active groups) for eligibility, or add a missing-group penalty. (`02_implementation_audit.md` §7 risk #6; `03_workflow_and_gaps.md` §5.)
5. **Weather features missing entirely** despite Tier-1 spec status (`forward_only_knn.md:94-96`). `FEATURE_GROUPS` has no weather entries (`configs.py:55-100`); loader is wired at `loader.py:468-525, 994-1018` but never called from `builder.py`. Mitigation: implement `_build_weather_features_pool/_query` and add `weather_level` / `weather_hdd_cdd` groups. (`02_implementation_audit.md` §7 risk #8; `03_workflow_and_gaps.md` §4.)
6. **Recency formula is linear despite "half_life" name** at `engine.py:187-190` (`1 + age_days / half_life`). 730-day-old day → multiplier ≈ 2.0, not the 0.5 an exponential half-life would imply. Mitigation: either rename to `recency_linear_scale_days` or change formula to `exp(-age_days * ln2 / half_life)`. (`03_workflow_and_gaps.md` §4 undocumented assumptions.)
7. **Latent `NameError` in scoreboard** at `evaluate_configs.py:126`. References undefined `log_path` in the no-actuals branch — script will crash instead of warning during backtest sweeps. Mitigation: remove the bad reference or define the variable. (`02_implementation_audit.md` §7 risk #2.)
8. **Day-type profiles silently invalidate `config_hash`** at `evaluate_configs.py:86-103`. Hash computed pre-override; weights/n_analogs/season_window mutated post-override inside `run_forecast` (`forecast.py:1129`). Same `config_name` produces different model on weekday vs weekend with identical hash. Mitigation: fingerprint after `with_day_type_overrides`. (`02_implementation_audit.md` §7 risk #5.)
9. **Pool size satisfies `min_pool_size` even when rows are NaN-heavy** — `_ensure_columns` (`features/builder.py:629`) fills missing columns with NaN, preflight only checks pool row count not feature density per row (`forecast.py:280-288`). Combined with risk #4, produces extremely biased analog sets silently. Mitigation: extend `_validate_pool` and `run_preflight` to require minimum non-null density per row. (`02_implementation_audit.md` §7 risk #7.)
10. **`strip_forecast.py` not implemented** despite spec calling it out as in-scope (`forward_only_knn.md:65, 135-141`). D+N strip use case is unsupported. Mitigation: implement multi-day loop wrapper that calls `run_forecast` per horizon and respects horizon-gated feature flags. (`02_implementation_audit.md` §7 risk #10; `03_workflow_and_gaps.md` §4.)
11. **Scoreboard CSV write commented out** at `evaluate_configs.py:300-301`. `--summary` reads from a file new runs do not populate. Mitigation: re-enable `_append_rows` or remove the docstring claim. (`02_implementation_audit.md` §7 risk #16; `03_workflow_and_gaps.md` §4.)
12. **`_safe_load` swallows all loader exceptions** at `builder.py:499-504`, demoting missing parquets to silent NaN. Combined with risk #4, parquet outages manifest as low-quality analogs not as errors. Mitigation: distinguish "parquet missing" from "parquet empty" and surface the former as a hard error at preflight. (`03_workflow_and_gaps.md` §4 undocumented assumptions.)

---

## 4. Recommended next steps

Ordered: blockers (correctness/leakage) before features. Each step ≤1 day.

**Step 1 — Add `as_of_date` exclusion to the analog pool.** Touch `similarity/engine.py` (`find_twins`/`_compute_distances` chain) and the `ForwardOnlyKNNConfig` dataclass at `configs.py`. Add `as_of_date: date | None = None` to the config; in `engine.py:107` change `work[work["date"] < target_date]` to also drop `work[work["date"] >= as_of_date]` when set. Default to `today` for live; default to `target_date` for backtest helpers. Closes risk #2.

**Step 2 — Pin a vintage policy for forecast loaders.** Touch `modelling/da_models/common/data/loader.py` (`load_load_forecast`, `load_solar_forecast`, `load_wind_forecast`, `load_net_load_forecast`, `load_outages_forecast`, Meteologica equivalents). Add a required `vintage_at: datetime | None` parameter; when present, filter the parquet to rows where `forecast_execution_date <= vintage_at` and select max-vintage per delivery hour/day. When absent and parquet has multi-vintage data, raise rather than silently picking max. Update `build_query_row` at `builder.py:637-700` to pass `vintage_at = target_date - 1 day @ DA-cutoff hour`. Closes risk #1.

**Step 3 — Decide and align the distance weighting.** Touch `metrics.py:46-70`. Either switch the default to `1/distance` to match spec, or add `weight_method = "inverse_distance_squared"` as the new explicit name and require an architecture-decision comment in `configs.py` justifying the divergence. Add a unit test that fixes a 3-analog input and asserts the expected weight vector. Closes risk #3.

**Step 4 — Patch the scoreboard `NameError` and re-enable CSV write.** Touch `experiments/evaluate_configs.py:126` (remove or define `log_path`) and `:300-301` (uncomment `_append_rows(rows)`). Add a unit/smoke test that runs `run_scoreboard` over a 3-day range with one date that has no actuals and confirms it neither crashes nor silently drops rows. Closes risks #7 and #11.

**Step 5 — Add minimum non-null density gate to preflight + analog selection.** Touch `validation/preflight.py:78-85` (add `min_row_feature_density` knob) and `similarity/engine.py:60-82` (drop pool rows whose covered weight share < threshold before computing top-N). Default 0.6–0.8; expose in `ForwardOnlyKNNConfig`. Closes risks #4 and #9.

**Step 6 — Make `_safe_load` failures observable.** Touch `features/builder.py:499-504`. Distinguish (a) parquet not found, (b) parquet empty, (c) loader exception. Surface (a) and (c) as preflight errors that gate the run; allow (b) to proceed only with explicit override. Closes risk #12.

**Step 7 — Resolve the recency-name/formula mismatch.** Touch `similarity/engine.py:187-190` and `configs.py:129`. Either (a) keep linear and rename to `recency_linear_scale_days`, or (b) implement true exponential decay `np.exp(-age_days * np.log(2) / half_life)`. Add a test fixture that pins the chosen behavior. Closes risk #6.

**Step 8 — Implement `pipelines/strip_forecast.py`.** New file under `modelling/da_models/forward_only_knn/pipelines/`. Loop over `horizons = range(1, N+1)`, call `run_forecast(target_date=today + h, config=config)` per horizon honoring the horizon-gated `include_*` flags already in `forecast.py:1131-1141`. Avoid synthetic reference rows per spec (`forward_only_knn.md:141`). Re-export from `pipelines/__init__.py`. Add a smoke test that strips D+1..D+5 with realistic parquets. Closes risk #10.

**Step 9 — Add weather features.** Touch `features/builder.py` (new `_build_weather_features_pool` and `_build_weather_features_query`), `configs.py:55-100` (add `weather_level` and `weather_hdd_cdd` groups with default weights from `forward_only_knn.md:150-158`: 2.0/2.0). Use existing `loader.load_weather_*` and `_normalize_weather_hourly:468-525`. Decide whether to use station-weighted PJM-RTO temperature or per-region; align with how load is per-region. Add to preflight. Closes risk #5.

**Step 10 — Compute config fingerprint after day-type override.** Touch `experiments/evaluate_configs.py:86-103`. Move `_config_fingerprint` invocation to after `with_day_type_overrides` is applied (or persist both pre- and post-override hashes). Update the scoreboard CSV schema accordingly. Closes risk #8.

**Step 11 — Wire formal evaluation metrics into the scoreboard.** Touch `experiments/evaluate_configs.py:182-186`. Import `crps_from_quantiles`, `pinball_loss`, `coverage` from `common/evaluation/metrics.py`. Compute per-day from the quantile output and append to scoreboard rows. Closes risk #11 partially (turns the scoreboard into a real evaluation surface).

**Step 12 — Document or remove the outage double-use and unify DOW conventions.** Touch `similarity/filtering.py:83-138` + `03_workflow_and_gaps.md` notes. Decide whether `outage_total_mw` should drive both the regime filter and the distance metric (current behavior, double-counts). Likely either drop the `outage_level` distance group when `apply_outage_regime_filter=True`, or rename the filter to make the redundancy explicit. Separately, unify `da_models/common/calendar.py:60-69` (Mon=0..Sun=6) and `forward_only_knn/configs.py:42-46` (Sun=0..Sat=6) on one convention or rename to `dow_sun0`. Closes Spec-matrix row "outages double-use" + DOW-convention drift.

---

## 5. Open questions for the modeler

These are judgment calls only a human can make. Each is yes/no or short-list.

- **Distance weighting:** keep `1/d^2` (current `metrics.py:65`) or switch to `1/d` (spec/research at `pjm-like-day-research.md:483-484`)? Yes/no on switching to `1/d`. (Surfaces Agent 1 ambiguity §4.2 + Agent 2 risk #1.)
- **Weighted-quantile convention:** Hyndman-Fan #7 (linear interpolation between order statistics) or cumulative-weight threshold (the form in `pjm-like-day-research.md:332-339` and current `forecast.py:83-90`)? Pick one. (Agent 1 ambiguity §4.13.)
- **Headline forecast statistic:** weighted mean (current `forecast.py:211`, spec `forward_only_knn.md:131`) or P50 weighted quantile (spec `forward_only_knn.md:149`, research-doc default)? Pick one to surface as "the forecast" in the output schema. (Agent 1 ambiguity §4.14.)
- **Holiday handling:** keep as a fallback-ladder rung (current `filtering.py:36-80`, `forward_only_knn.md:42`) or promote to a hard pre-filter (research recommendation `pjm-like-day-research.md:412`)? Yes/no on hard filter. (Agent 1 cross-doc disagreement §5.3.)
- **Pool-side load:** realized actual load (current `builder.py:35-91`) or vintage-matched D-1 forecast for D (consistent with query)? The spec is silent (`forward_only_knn.md:78`). Pick A (realized) or B (vintage forecast). (Agent 1 ambiguity §4.7.)
- **Forecast-vintage policy:** require `*_da_cutoff*` snapshots or accept latest published as a documented compromise? If accept, define an acceptable lag (e.g. ≤ T-1 18:00 EPT) and fail loudly outside. (Agent 1 ambiguity §4.8; Agent 2 risk #3.)
- **Recency formula:** keep linear and rename to `recency_linear_scale_days`, or implement exponential `exp(-age * ln2 / half_life)`? Pick one. (Agent 1 ambiguity §4.3; Agent 3 §4.)
- **Outage signal — feature, filter, or both?** Currently both (`builder.py:268-316` distance; `filtering.py:83-138` filter). Pick: distance-only / filter-only / both with a documented rationale. (Agent 3 §3 #3 + §5.)
- **Renewables/outages tier:** spec defers to Phase 2 (`forward_only_knn.md:99, 192`); code already ships them in baseline (`registry.py:33`, `configs.py:77-100`). Update the spec to "in MVP" or revert the code to off-by-default? Yes/no on updating the spec. (Agent 3 §5.)
- **Lagged DA LMP features:** include "prior-week same-DOW LMP" / "recent DA LMP level" as a distance feature (research Tier 1/2 at `pjm-like-day-research.md:411`) or keep the "forward-only" exclusion (`builder.py:12-14`)? Yes/no on adding lag features. (Agent 1 cross-doc disagreement §5.1.)
- **OnPeak/OffPeak split:** stay with hardcoded HE8-23 weekend-included (current `forecast.py:50-51`) or switch to NERC-convention weekday-only on-peak? Pick one. (Agent 3 §4.)
- **Day-type profiles:** keep silent runtime override of weekend `n_analogs`/`season_window`/weights (`configs.py:147-170, 268-295`), or require explicit per-day-type configs in the registry? Yes/no on keeping silent override. (Agent 2 risk #5; Agent 3 §4.)
- **Backtest scope for MVP:** is the daily-incremental scoreboard (`evaluate_configs.py`) sufficient, or does Phase 1 need a rolling-window backtest harness with `as_of_date` discipline before more model tuning? Pick now / Phase 2. (Agent 1 §2 train/test row; Agent 2 §4.)
- **Hub default:** retain `WESTERN HUB` for `forward_only_knn` and `EASTERN HUB` for `common/configs.py` (current divergence at `configs.py:15` vs `common/configs.py:13`), or unify? Yes/no on unifying. (Agent 2 risk #13.)
