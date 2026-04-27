# Hourly vs. Daily Features in the Forward-Only KNN

## Question

The query inputs to the forward-only KNN are 24-hour series (load forecast per region per hour, weather per hour, gas per hour, solar / wind per hour, net load per hour). The matching distance, however, is computed on **daily-aggregated features** (`load_daily_avg`, `load_daily_peak`, `load_daily_valley`, `load_morning_ramp`, etc.), not on the raw 168-or-so hourly values. Why?

## Three reasons the matching feature space is daily, not hourly

### 1. The matching unit must equal the ensemble unit

The model is a day-as-row Analog Ensemble (Delle Monache et al., 2013). When `find_twins` returns the top-K analog days, each contributes its full 24-hour LMP profile (`lmp_h1..lmp_h24`) as one ensemble member. `_hourly_forecast_from_analogs` (`pipelines/forecast.py:179`) then takes weighted quantiles across the K analogs *per hour*.

If features were per-hour, you'd be picking 24 *separate* analog sets — one per delivery hour. That loses cross-hour coherence: the analog set at HE19 might not include the analog set at HE20, so the resulting 24-hour forecast is no longer a real historical day, just an hour-by-hour Frankenstein. Recovering cross-hour structure under that design is exactly what the Schaake shuffle and copula approaches exist to do (`pjm-like-day-research.md` §5.2.1). The day-level match avoids the problem entirely — every analog brings a coherent 24-hour profile by construction.

### 2. Curse of dimensionality versus pool size

#### The curse of dimensionality, generally

As the number of features (dimensions) grows, distance-based methods like KNN break down — even with a lot of data. The core symptom: **everything becomes roughly equidistant from everything else.** The ratio between the nearest and farthest neighbor of a query point approaches 1 as dimensions grow, so "nearest neighbor" stops being meaningful — the top match isn't really closer than a random point.

Three ways to see why:

1. **Volume concentrates in the corners.** In a unit hypercube, the volume of the inscribed sphere shrinks toward zero as dimensions grow. Almost all volume sits in the corners, far from the center; points spread into a thin shell rather than clustering.
2. **Data sparsity.** To cover a 1-D interval [0,1] at spacing 0.1 needs 10 samples. In 10 dimensions, the same density needs 10¹⁰. A 300-day pool is dense in 2-D but catastrophically sparse in 200-D — the space is mostly empty.
3. **Distance noise dominates signal.** Euclidean distance sums squared differences across *all* dimensions. If only a handful actually drive similarity but you include 200, the noisy dimensions add variance that drowns out the meaningful ones. Two genuinely similar days look far apart because they differ on irrelevant axes.

KNN's whole premise — *"close in feature space ⇒ similar outcome"* — fails once distances become uninformative; you end up picking neighbors at random. The rule of thumb is that sample size needs to grow exponentially with dimension to maintain density, which is rarely feasible. The practical response is to reduce dimensions: pick features that carry signal, compress correlated ones, or switch to methods that don't rely on Euclidean distance (tree-based models, learned embeddings).

#### How it applies here

With `MIN_POOL_SIZE=150` and a `season_window_days=60` filter (`configs.py`), the candidate pool after pre-filters is typically 200–400 days. The current daily-feature vector is ~30 numbers. An hourly version (24×load + 24×temp + 24×gas + 24×solar + 24×wind ≈ 200 dimensions) on a ~300-day pool would make weighted Euclidean distance noisy and unstable.

Weron 2014 explicitly flags this for KNN/similar-day approaches (`pjm-like-day-research.md` §1.5): the choice of dimensionality is critical because too many features over-smooth or destabilize the neighbor selection. The compression to ~30 daily features is a deliberate dimensionality-reduction step — collapsing 200 redundant hourly numbers into ~30 shape-bearing scalars keeps the 300-day pool dense enough for KNN to mean something.

### 3. Shape is already preserved — just compactly

`_load_daily_aggregates` (`features/builder.py:31`) compresses each hourly series into a small set of shape-bearing scalars:

| Daily feature | Hourly construction |
|---|---|
| `<prefix>_daily_avg` | mean over 24 hours |
| `<prefix>_daily_peak` | max over 24 hours |
| `<prefix>_daily_valley` | min over 24 hours |
| `<prefix>_ramp_max` | max hour-over-hour first difference |
| `<prefix>_morning_ramp` | HE8 − HE5 |
| `<prefix>_evening_ramp` | HE20 − HE15 |

This captures *level* (avg / peak / valley) and *shape* (ramp magnitude, morning ramp, evening ramp) in 6 numbers per series. Those are the parts of the hourly signal that drive day-to-day similarity. The hourly weather, hourly weather observed, hourly fuel mix series all flow through the same compression in `_load_daily_aggregates`, `_build_weather_features`, `_build_gas_features`, `_build_renewable_features_pool`, and `_build_net_load_features_pool`.

The pool's hourly LMP profile is *not* compressed — it's preserved as `lmp_h1..lmp_h24` and contributes directly to the ensemble (`_build_lmp_labels`, `features/builder.py:342`). So the design is asymmetric on purpose: features compress hourly to daily for matching; labels stay hourly for the forecast.

## What the design buys, what it costs

| Property | Daily features (current) | Per-hour features (hypothetical) |
|---|---|---|
| Matching unit | day | hour |
| Cross-hour coherence in output | preserved automatically (analog days bring coherent 24h profiles) | broken — needs Schaake/copula reconstruction |
| Distance dimensionality | ~30 | ~200 |
| Pool stability at 300 candidates | high | poor |
| Captures hourly shape | via avg/peak/valley/ramp_max/morning_ramp/evening_ramp | natively |
| Captures hour-specific regime (e.g. HE19 ramp pattern) | indirectly via ramp features | directly |
| Implementation complexity | low | high (24 separate KNNs + reconstruction) |

The cost paid is **resolution on hour-specific regime details** — for example, "days where the HE19 spike was steep but HE20 was flat." The current ramp features (`evening_ramp = HE20 − HE15`) collapse that into a single number. Whether that loss is meaningful depends on whether hour-specific patterns drive day-to-day variation more than the level / peak / overall shape do. Empirically for PJM Western Hub, the answer for most days is no — but it could be revisited for shoulder-season heat events where the ramp shape becomes the main signal.

## Where the hourly values still appear

Even though they don't enter the distance, hourly values are exposed in the diagnostics:

- The `QUERY FEATURE VALUES` print block (`pipelines/forecast.py:_print_query_features`) shows full 24-hour series per region/hub, with on-peak / off-peak / flat summaries — for human inspection of the forecast inputs.
- The `INPUT FEED PROVENANCE` block confirms which parquet was picked per feed.
- The `FEATURE VECTOR (KNN INPUT)` block shows the daily-aggregated values that *actually feed* the distance — this is the bridge between "what came in" and "what the model used."

This separation is the right shape: human reviewers see hourly granularity, the distance metric sees the compressed scalar form.

## When to revisit the daily-only choice

There are two scenarios where adding hourly resolution to matching would be worth the complexity:

1. **Hour-specific regime matching for known difficult day-types.** E.g., a separate "HE19-ramp-shape" feature group computed as a normalized hourly profile cosine-similarity to the query, scored only on hours HE17–HE21 — captures evening-ramp shape similarity without exploding dimension. This is a targeted addition, not a rewrite.
2. **Once the AnEn vintage archive exists** (`historical_forecasts.md`) and the pool is constrained to vintage-matched forecasts, the dimensionality penalty is the same but the matching feature space is more reliable. At that point, increasing to a moderate hourly subset (e.g. 4–6 representative hours per series) becomes more defensible.

Until either of those motivates a change, **daily aggregates are the right matching grain** for this model — they preserve cross-hour coherence in the output for free, fit the pool size, and capture the dominant level/shape signals via 5–6 scalars per series.

## References

- `pipelines/forecast.py:179` — `_hourly_forecast_from_analogs`: where weighted hourly quantiles are taken across analog days.
- `features/builder.py:31` — `_load_daily_aggregates`: the daily compression of hourly load / net load.
- `features/builder.py:271` — `_build_weather_features`: same pattern for weather.
- `features/builder.py:317` — `_build_gas_features`: same pattern for gas.
- `features/builder.py:342` — `_build_lmp_labels`: hourly LMP preserved as `lmp_h1..lmp_h24` for the ensemble.
- `configs.py:41-101` — `FEATURE_GROUPS`: full list of the daily features that enter the distance.
- Delle Monache, L. et al. (2013). "Probabilistic Weather Prediction with an Analog Ensemble." Monthly Weather Review, 141(10).
- `pjm-like-day-research.md` §1.5 — Weron 2014 KNN dimensionality warning.
- `pjm-like-day-research.md` §5.2.1 — Schaake shuffle, the alternative when cross-hour coherence is not preserved by construction.
- `historical_forecasts.md` — pool/query asymmetry, layered on top of (and orthogonal to) the daily-vs-hourly question.
