# Historical Forecasts for Forward-Only KNN Matching

## Problem

The forward-only KNN matches a query day's forecasted fundamentals against a pool whose features are computed from realized values. The two sides of the distance computation are not on equal footing:

- **Query** carries forecast error baked in (load D-1 forecast, weather D-1 forecast, outage D-1 forecast, renewable D-1 forecast).
- **Pool** carries zero forecast error — features are realized values.

The "closest" analog under this setup is the day whose *realized* fundamentals matched today's *forecasted* fundamentals, which is not the same as the day whose *forecasted* fundamentals matched today's *forecasted* fundamentals. Whatever systematic bias the forecast carries (e.g., PJM load forecast under-predicting on extreme heat days, wind forecast under-confidence at the tails) leaks directly into the analog selection.

## Why this matters: the AnEn prescription

The Analog Ensemble methodology (Delle Monache et al., 2013, MWR 141:10) is built on **(forecast, observation)** pairs:

- The pool is indexed by historical *forecasts* issued at the same lead time as the query.
- The pool labels are the realized observations for those days.
- A new query (today's forecast at the same lead time) is matched against the pool's forecast features.
- The ensemble member contributed by each analog is the analog's *observation*.

This makes pool and query symmetric in their forecast-error distribution. Our current implementation pool features = realized fundamentals breaks that symmetry.

## Where the asymmetry lives in our code

| Feature group | Pool side | Query side |
|---|---|---|
| Load | `rt_load_mw` realized (`features/builder.py:438`) | `forecast_load_mw` D-1 forecast (`features/builder.py:514`) |
| Weather | `weather_observed_hourly` (`features/builder.py:424`) | `weather_forecast_hourly` (`features/builder.py:500`) |
| Outages | `outages_actual` (`features/builder.py:428`) | `outages_forecast`, latest vintage (`features/builder.py:200-202`) |
| Renewables | `fuel_mix` realized solar+wind (`features/builder.py:429`) | `solar_forecast` + `wind_forecast` (`features/builder.py:552-553`) |
| Net load | realized RT load minus realized fuel-mix solar+wind (`features/builder.py:90-133`) | `net_load_forecast_mw` (`features/builder.py:559`) |
| Gas | hourly spot | hourly spot (same — minimal asymmetry) |
| Calendar | deterministic | deterministic (no asymmetry) |

The outage block is a partial exception: `_build_outage_features_query` already references `forecast_execution_date` to pick the most-recent forecast vintage, which means the outage feed *does* preserve vintage history. That is the only feed where vintage-aware reconstruction is currently feasible without data backfill.

## Three options ranked by faithfulness to AnEn theory

### Option A — Vintage-matched historical forecast pool (AnEn-correct)

For every historical delivery date D in the pool, store the forecast as it stood at the D-1 issue time. Pool features are computed from those archived D-1 forecasts; pool labels remain the realized DA LMP profile.

- **Pros:** Apples-to-apples on both sides of the distance. Removes the systematic forecast-bias leak entirely. Aligns with the AnEn literature.
- **Cons:** Requires building or sourcing the historical-forecast archive at the right vintage for each feed.
- **Plumbing already in place:** Outage forecasts are vintage-stamped via `forecast_execution_date`. Need to audit load / solar / wind / weather / net-load parquet caches to see whether they preserve vintage or are overwritten in place.
- **External data sources for missing pieces:**
    - Weather: Open-Meteo `historical-forecast-api` returns ECMWF/GFS archives at the D-1 vintage.
    - Load / solar / wind: PJM Data Miner publishes vintage-stamped forecast archives for these feeds.

### Option B — Parallel forecast features on the pool side

Pool rows carry both `<feature>_forecast` and `<feature>_realized`. Distance is computed only on the `_forecast` columns; `_realized` columns are kept around for diagnostics and label construction.

- Same data requirement as A, but the schema makes the asymmetry explicit.
- Lets us run head-to-head experiments: match-on-forecast vs match-on-realized vs blend, scored on out-of-sample CRPS.
- Useful as a transition step toward A while we confirm the forecast archive is reliable.

### Option C — Perturbation / bagged KNN, no historical forecasts

For each query, sample N noisy versions of the query vector by injecting Gaussian noise sized to known D-1 forecast error variance per feature. Run KNN on each perturbed query, ensemble the resulting analog distributions.

- **Captures:** random forecast error (variance).
- **Does not capture:** systematic forecast bias (mean error). If the load forecast systematically misses the tail, perturbation around the central forecast cannot reach those analogs.
- **Cost:** trivial — a loop and an error-magnitude config dict.
- **Use case:** an immediate uncertainty band experiment without rebuilding any data, valid as an interim while A is being built.

## Magnitudes: which feeds bite hardest

Not all features are equally affected by the pool/query asymmetry. The bias is proportional to (forecast error magnitude) / (inter-day variance of the realized feature). Rough order:

| Feed | D-1 forecast error | Inter-day realized variance | Asymmetry severity |
|---|---|---|---|
| Wind | High (RMSE often 25-35% of forecast value) | High but skewed | **Severe** — most likely to mis-rank analogs |
| Outages | Moderate (5-10 GW typical) | Moderate | **Moderate to severe** — drives reserve-margin matching |
| Solar | Moderate (cloud-dependent) | High but seasonal | Moderate |
| Load | Low (~1.5% MAPE) | Moderate | **Mild** — probably doesn't move analog ranks much |
| Weather temp | Low (~1-2 deg F at D-1) | Moderate | Mild |
| Gas | Effectively zero (spot at D-1) | Low | Negligible |

This implies the highest leverage of any vintage-archive backfill is **wind first, outages second, solar third**. Load and weather are nice-to-have but unlikely to change analog selection in a meaningful way.

## Vintage audit results (2026-04-27)

Inspection of `modelling/data/cache` shows that *all* forecast feeds — PJM and Meteologica — currently behave as rolling snapshots, not historical archives. Realized feeds, by contrast, carry years of history.

| Feed | Distinct `forecast_execution_date` | `forecast_date` range | History |
|---|---|---|---|
| `meteologica_pjm_load_forecast_hourly_da_cutoff` | 2 | 2026-04-27 → 2026-05-11 | ~2 days |
| `meteologica_pjm_net_load_forecast_hourly_da_cutoff` | (no exec col) | 2026-04-27 → 2026-05-11 | ~2 days |
| `meteologica_pjm_solar_forecast_hourly_da_cutoff` | 2 | 2026-04-27 → 2026-05-11 | ~2 days |
| `meteologica_pjm_wind_forecast_hourly_da_cutoff` | 2 | 2026-04-27 → 2026-05-11 | ~2 days |
| `pjm_load_forecast_hourly_da_cutoff` | 1 | 2026-04-27 → 2026-05-03 | today only |
| `pjm_solar_forecast_hourly_da_cutoff` | 1 | 2026-04-27 → 2026-04-29 | today only |
| `pjm_wind_forecast_hourly_da_cutoff` | 2 | 2026-04-27 → 2026-04-29 | ~2 days |
| `pjm_outages_forecast_daily` | 8 | 2026-04-20 → 2026-05-03 | ~8 days |
| `pjm_lmps_hourly` (label) | n/a (realized) | 2014-01-01 → 2026-04-27 | 4,500 days |
| `pjm_load_rt_hourly` (realized) | n/a | 2014-01-01 → 2026-04-27 | 4,500 days |
| `pjm_fuel_mix_hourly` (realized) | n/a | 2020-01-01 → 2026-04-27 | 2,307 days |
| `pjm_outages_actual_daily` (realized) | n/a | 2020-01-01 → 2026-04-27 | 2,307 days |

**Implication.** Option A as originally written — "build a vintage-matched pool from existing caches" — is not feasible today for any feed except outages, and even outages have only ~8 execution dates of history. The `forecast_execution_date` column exists on most forecast feeds, but the upstream scrape currently overwrites prior vintages instead of appending. To make Option A real, the scrape itself has to change first.

This shifts the immediate action from "backfill" to "start preserving going forward." See the snapshotting plan below.

## Two-forecaster pattern: PJM + Meteologica

Both PJM and Meteologica publish D-1 forecasts for the same fundamentals (load, solar, wind, net load) at the same regional grain (MIDATL, RTO, SOUTH, WEST). Even without a historical archive, having two independent forecasters at the same vintage is itself a signal we can exploit today.

### Why dual-forecaster matters separately from the AnEn fix

The asymmetry problem (Options A/B/C) is about pool/query symmetry. Dual-forecaster is about **query-side uncertainty quantification** — how confident is the forecast we are matching against? PJM and Meteologica disagreeing on tomorrow's load is a direct indicator that tomorrow is a regime-stress day. Days where they agree to within 1% are routine; days where they disagree by 5%+ are the days where forecast bias does the most damage to analog selection.

### What it offers without history

| Pattern | Description | Data requirement |
|---|---|---|
| **Disagreement flag** | Compute per-hour `abs(meteo_value - pjm_value) / pjm_value` for load, solar, wind, net-load. Threshold (e.g. >3% load, >25% wind) flags the day as high-uncertainty. | Today's two snapshots — already present. |
| **Ensemble query** | Run KNN twice: once with PJM-as-query, once with Meteologica-as-query. Compare analog sets. Concordance → high confidence; divergence → spread between the two ensembles is a more honest uncertainty band than either alone. | Today's two snapshots — already present. |
| **Adaptive perturbation noise** | Use the PJM/Meteologica spread on a feature as the σ for Option C's perturbation, instead of a static error magnitude. This makes the perturbation cone widen on disagreement days and narrow on agreement days, which is the right behavior. | Today's two snapshots — already present. |
| **Bias-corrected query** | If one forecaster is known to systematically miss in a regime (e.g., PJM under-predicts load on extreme heat), use METEO as the cross-check before feeding the query into KNN. | Requires offline calibration of each forecaster against realized — not blocked by archive. |

### What it offers once a historical archive exists

Once both forecasters have ~6+ months of vintage-stamped history, dual-forecaster goes from a query-side overlay to a pool-side feature group:

- **Add Meteologica as a parallel forecast group in `FEATURE_GROUPS`** alongside PJM-vintage features. Both sides of distance are forecasts at the same lead time, so the AnEn symmetry holds.
- **The PJM/Meteologica disagreement at D-1 becomes a matching feature itself.** "Match days where the two forecasters disagreed by similar amounts in similar directions" is closer to the regime signal we actually care about than matching on either forecaster alone.
- **CRPS-weighted blending.** If one forecaster outperforms on certain regimes (e.g., METEO better on shoulder-season heat, PJM better on cold snaps), the blend weight can be regime-conditional rather than static.

### What it does *not* offer

Adding `meteologica_*` features to `FEATURE_GROUPS` today would not improve matching. The pool would be 100% NaN on those columns and the existing preflight (`_derive_effective_weights`, `pipelines/forecast.py:160`) would auto-disable the group. No premature feature additions until the archive exists.

## Snapshotting plan

The single highest-leverage action available today is to start preserving every D-1 forecast vintage — both PJM and Meteologica — to disk, append-only, never overwrite. The existing scrape feeds populate the rolling cache; we add a second write that preserves the vintage permanently.

### What to snapshot

For each scrape run (one per delivery date, executed at D-1), append a row-keyed parquet keyed on `forecast_execution_date` for:

- `pjm_load_forecast_hourly_da_cutoff`
- `pjm_solar_forecast_hourly_da_cutoff`
- `pjm_wind_forecast_hourly_da_cutoff`
- `pjm_net_load_forecast_hourly_da_cutoff`
- `pjm_outages_forecast_daily`
- `meteologica_pjm_load_forecast_hourly_da_cutoff`
- `meteologica_pjm_solar_forecast_hourly_da_cutoff`
- `meteologica_pjm_wind_forecast_hourly_da_cutoff`
- `meteologica_pjm_net_load_forecast_hourly_da_cutoff`
- `wsi_pjm_hourly_forecast_temp_latest` (weather)

Schema convention: keep the existing columns, add `archive_capture_utc` (the timestamp of the snapshotting write) so we can disambiguate intra-day vintage updates if the upstream feed re-issues. Partition by month to keep file sizes manageable.

Storage location: a new sibling directory `modelling/data/cache_archive/` so the active cache stays small and the archive is ignorable when running today's forecast.

### Cost

Per execution, ~50-200 KB across all feeds. After 12 months that is ~25-75 MB total — trivial. Implementation: ~half a day of work in the scrape orchestration layer.

### When the archive becomes useful

| Window | Use case |
|---|---|
| 0-3 months | Disagreement flag and ensemble query (no archive needed; works today). |
| 3-6 months | Bias diagnostics: per-forecaster, per-feature error vs realized. Calibrate Option C perturbation σ from real residuals instead of literature estimates. |
| 6-12 months | Single-season pool. Run Option B (parallel forecast features on pool side) for shoulder-season days. CRPS A/B vs current realized-pool baseline. |
| 12-18 months | Full-cycle pool covering all four seasons. Option A becomes default; realized features retire to label-construction only. Dual-forecaster matching turns on. |

### Open question on backfill

PJM Data Miner *may* expose vintage-stamped historical archives for load and outages forecasts via their API. If true, that collapses the 12-18 month timeline by serving the archive immediately. Worth a half-day investigation before committing to forward-only snapshotting. Meteologica historical archive availability would need to be confirmed with their support — if they retain past issues internally, a one-time bulk pull would similarly compress the timeline.

## Recommendation

1. **Start the snapshotting pipeline immediately.** Half-day of work, append-only writes to `modelling/data/cache_archive/`, covers PJM + Meteologica + weather. This is the only action that makes any of the AnEn options eventually feasible. Every day we delay is a day of vintage history we cannot get back.
2. **Run Option C immediately** as an uncertainty-band experiment, sized initially with literature-based forecast error magnitudes. It does not require any archive and gives us a quick answer to "how much does forecast noise alone widen the analog distribution?"
3. **Layer on dual-forecaster overlays** — disagreement flag + ensemble query + adaptive Option C noise sized by PJM/Meteologica spread. All three work with today's snapshots, no archive needed.
4. **Investigate vendor backfill** (PJM Data Miner historical forecast archive, Meteologica support inquiry, Open-Meteo `historical-forecast-api` for weather). If any of these serve vintage history, fast-forward Option A timeline by 12+ months.
5. **Once snapshot archive ≥ 6 months**, build Option B (parallel `_forecast`/`_realized` columns) for wind and outages first — highest leverage feeds per the magnitudes table.
6. **Defer load and weather forecast features** to a later phase; their D-1 forecast error is small enough that asymmetry is unlikely to change rankings.
7. **Once archive ≥ 12 months and Option B has demonstrated CRPS gain**, transition to Option A as the default pool and retire realized features to label-construction only. Turn on Meteologica as a parallel forecast group at this stage.

## References

- Delle Monache, L. et al. (2013). "Probabilistic Weather Prediction with an Analog Ensemble." Monthly Weather Review, 141(10), 3498-3516. [Link](https://journals.ametsoc.org/view/journals/mwre/141/10/mwr-d-12-00281.1.xml)
- `pjm-like-day-research.md` §1.4 — AnEn methodology transfer to electricity markets.
- `backward_vs_forward_looking.md` — adjacent failure mode (feature-side anchoring) for LASSO QR / LightGBM. Distinct from this asymmetry, but related in that both stem from a mismatch between training-time and inference-time feature distributions.
