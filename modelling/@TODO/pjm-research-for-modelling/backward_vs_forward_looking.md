# LASSO QR Feature Analysis: Backward vs Forward-Looking

## Problem

The LASSO QR model's top features are dominated by backward-looking (reference-day) signals. This causes the forecast to anchor to recent realized pricing rather than respond to tomorrow's fundamentals, leading to downside bias during regime changes like shoulder-season heat events.

## Top 10 Features by |coefficient|

### Backward-looking (6 of 10)

These are all **yesterday's realized values**. They tell the model what the market looked like on the reference day, not what tomorrow's market will look like.

| Feature | Description | Source |
|---------|-------------|--------|
| `lmp_onpeak_avg` (7.23) | Avg DA LMP, HE8-23 | `lmp_features.py:59` |
| `lmp_daily_min` (6.28) | Lowest hourly DA LMP | `lmp_features.py:55` |
| `lmp_per_load` (5.69) | Avg DA LMP / avg load (price intensity) | `composite.py:60` |
| `gas_m3_offpeak_avg` (5.47) | Avg off-peak gas at Tetco M3, HE1-7,24 | `gas_hourly_features.py:83` |
| `gas_ventura_daily_avg` (2.24) | Avg daily gas at Ventura/NNG hub | `gas_hourly_features.py:97` |
| `lmp_evening_ramp` (1.74) | LMP at HE20 minus LMP at HE15 | `lmp_features.py:87` |
| `lmp_offpeak_avg` (1.52) | Avg DA LMP, HE1-7,24 | `lmp_features.py:60` |

**Combined importance: 30.47** (75% of top-10 total)

### Forward-looking (3 of 10)

These carry signal about tomorrow's delivery day, but are partially diluted by backward-looking inputs.

| Feature | Description | Source |
|---------|-------------|--------|
| `load_x_gas` (4.09) | Tomorrow's load forecast x today's M3 gas | `lasso_qr/features/builder.py:133` |
| `reserve_margin_pct` (2.41) | (185 GW - outages - load) / load for tomorrow | `lasso_qr/features/builder.py:124-127` |
| `tgt_load_change_vs_ref` (1.58) | Tomorrow's load minus yesterday's load | `target_load_features.py:207-209` |

**Combined importance: 8.08** (20% of top-10 total)

### Not present in top 10

Direct forward-looking features that LASSO has regularized away:

- `tgt_load_daily_avg` -- tomorrow's load level (appears at #15 via `tgt_load_south_daily_avg` with 0.65)
- `tgt_load_daily_peak` -- tomorrow's peak load
- `tgt_temp_daily_avg`, `tgt_cdd`, `tgt_hdd` -- tomorrow's weather
- `tgt_outage_total_mw` -- tomorrow's outages (only appears via `reserve_margin_pct`)
- `tgt_solar_daily_avg`, `tgt_wind_daily_avg` -- tomorrow's renewables

## Why This Matters

On a typical day the backward-looking anchor is fine -- tomorrow usually looks like today. But during regime changes (shoulder-season heat, cold snaps, outage events), realized pricing from yesterday is misleading:

- **April 14, 2026 example**: Load forecast was ~99 GW OnPeak (well above typical April ~85-92 GW). But yesterday's LMPs reflected mild spring pricing. The model forecast $44.96/MWh OnPeak because the backward features (75% of signal) said "spring prices" while the forward features (20%) said "elevated load."
- The forward signal couldn't overcome the backward anchor.

## Mitigations

### Already implemented (items 1-3 from lasso_model.md)

1. **Asinh VST** -- compresses tails so quantile models find distinct coefficients, improving spread
2. **Recency weighting** (gamma=0.997) -- downweights stale data, making the model more responsive to recent load-price relationships
3. **Quantile-specific alpha** -- lower L1 penalty on tail quantiles so more features survive regularization

### Next steps

4. **Multi-window ensemble** (56d + 728d) -- a short window trained only on recent shoulder-season data would naturally promote load features over backward LMP features, since recent LMP-load correlation within the same season is stronger
5. **Evaluate dropping or capping backward LMP features** -- consider adding `lmp_onpeak_avg`, `lmp_offpeak_avg`, `lmp_daily_min` to `drop_feature_names` and relying on gas + load + outages as the fundamental drivers. This is aggressive but would force the model to use forward-looking signals.
6. **Feature importance monitoring** -- log the backward-vs-forward importance ratio after each retrain; flag when backward features exceed a threshold (e.g., >60% of total importance)

---

## LightGBM QR: More Balanced, but Still Anchored

The LightGBM model has a better backward/forward balance than LASSO — tree splits can capture nonlinear interactions between reference-day and target-day features. But backward LMP features still dominate the top of the importance ranking and act as a level anchor that suppresses forward-looking signals during regime changes.

### Top 15 Features by Gain (April 14, 2026 forecast)

#### Forward-looking (8 of 15)

These carry signal about tomorrow's delivery day.

| Feature | Gain | Description |
|---------|------|-------------|
| `reserve_margin_pct` (894.64) | #1 | (185 GW - tgt_outages - tgt_load) / tgt_load |
| `tgt_renewable_change_vs_ref` (533.76) | #5 | Tomorrow's renewable gen minus yesterday's |
| `tgt_load_change_vs_ref` (532.77) | #6 | Tomorrow's load forecast minus yesterday's |
| `tgt_solar_daily_avg` (495.90) | #8 | Tomorrow's solar generation forecast |
| `tgt_outage_change_vs_ref` (480.57) | #9 | Tomorrow's outages minus yesterday's |
| `tgt_load_west_evening_ramp` (470.78) | #10 | Tomorrow's evening load ramp (western region) |
| `tgt_temp_change_vs_ref` (463.56) | #13 | Tomorrow's temperature minus yesterday's |

**Combined gain: 3,871** (47% of top-15 total)

#### Backward-looking (7 of 15)

These are all **reference-day realized values**.

| Feature | Gain | Description |
|---------|------|-------------|
| `lmp_offpeak_avg` (871.53) | #2 | Avg DA LMP, HE1-7,24 |
| `dart_spread_daily` (862.61) | #3 | DA minus RT LMP, daily avg |
| `lmp_daily_min` (584.45) | #4 | Lowest hourly DA LMP |
| `lmp_energy_share` (530.17) | #7 | Energy component / total LMP |
| `lmp_evening_ramp` (470.22) | #11 | LMP at HE20 minus LMP at HE15 |
| `outage_total_daily_change` (465.28) | #12 | Reference-day outage delta |
| `lmp_daily_flat` (456.43) | #15 | Avg DA LMP, all hours |
| `wind_speed_daily_avg` (462.77) | #14 | Reference-day wind speed |

**Combined gain: 4,703** (53% of top-15 total)

### Key differences from LASSO QR

1. **More balanced split**: 47/53 forward/backward vs LASSO's 20/75. LightGBM retains more forward features because tree splits don't have LASSO's collinearity-driven regularization that zeroes correlated features.

2. **Nonlinear interactions help**: `reserve_margin_pct` is #1 because LightGBM can split on "tight reserve margin AND high gas" — a compound signal LASSO can't represent. But the model still lacks the explicit `outage_x_load` multiplicative interaction (now added).

3. **The anchoring mechanism is different**: In LASSO, backward features dominate through linear coefficients. In LightGBM, backward LMP features appear in **early tree splits** (high gain = split near root). When the tree splits on `lmp_offpeak_avg < $35` first (typical Sunday), it routes the prediction into a "low-price" leaf subtree. Downstream forward-looking splits can only adjust within that subtree's range — they can't escape the level anchor set by the root split.

### April 14, 2026 case study

DA trading $83.00 on-peak. LightGBM forecast: $50.56 (P50), ~$52.70 (EV). Even P90: $74.14.

**Why the model missed**: Reference date was Sunday April 13. Sunday DA prices were ~$30-40. The backward features (`lmp_offpeak_avg`, `dart_spread_daily`, `lmp_daily_min`) told the trees "this is a $30-40 price environment." The forward features (`reserve_margin_pct`, `tgt_load_change_vs_ref`) signaled elevated stress — 57.5 GW outages, Monday load increase — but could only push the forecast up within the low-price subtree. The trees never reached the $80+ leaves because the root splits had already excluded them.

**The Sunday-to-Monday gap is the worst case**: Backward LMP features shift by the largest amount on this day-type transition. Saturday→Sunday and Friday→Saturday shifts are smaller because weekend pricing is structurally similar. Monday→Tuesday through Thursday→Friday are fine because weekday pricing is stable.

### Mitigations implemented (2026-04-13)

| Change | Backward/Forward impact |
|--------|------------------------|
| Asinh VST on target | Compresses tails in transformed space, letting trees split into high-price regions more often during training — reduces the dominance of "normal-price" root splits |
| Temporal decay (180d half-life) + 3x spike weighting | Recent data dominates, and high-price observations get 3x gradient signal. Trees are forced to accommodate spikes rather than treating them as noise |
| EV from quantile integration | Uses the full distribution instead of P50; right-tail extrapolation captures the asymmetry that backward-anchored P50 misses |
| `outage_x_load` interaction feature | Gives the trees explicit access to the multiplicative scarcity signal, reducing dependence on the additive `reserve_margin_pct` |

### Still needed

1. **Monday day-type profile with Friday LMP anchor** -- Use Friday's (lag=3) backward LMP features when forecasting Monday, since Friday better represents the weekday price regime. Currently configured in `DAY_TYPE_LGBM_PROFILES` but only weekday/saturday/sunday exist.

2. **Feature importance monitoring** -- Track the backward/forward gain ratio after each retrain. If backward features exceed 60% of total gain, flag it. This ratio should improve with the sample weighting changes (recent regime-change days where forward features mattered will get more weight).

3. **ICE forward price as a feature** -- The most direct fix to the anchoring problem. ICE DA peak/offpeak for delivery day is a market-consensus forward-looking price signal. If the model had access to "ICE DA OnPeak = $83" for tomorrow, it would immediately override the backward anchor. This requires plumbing ICE intraday data from the existing `ice_power_intraday` source into the feature builder.

4. **Conditional lagged LMP** -- The `include_lagged_lmp` config flag is currently `False` by default for weekdays. Consider enabling it with conditional logic: for Monday forecasts use Friday lag, for other weekdays use D-1. This gives the model a price-level anchor without the Sunday distortion.

---

## Like-Day Model: Same Problem, Different Mechanism

The like-day model has the same backward-looking bias but for a structural reason: **it can only output weighted averages of historical DA LMP profiles**. It cannot extrapolate beyond what has already happened.

### April 14, 2026 case study

DA trading $84 on-peak. Like-day model output: ~$50. Even P99: ~$80.

The model selects analogs by composite distance across ~27 feature groups. For an April day with June heat, no historical analog has the right combination:

| Analog source | Weather match | Gas match | Outage match | LMP match |
|---------------|:---:|:---:|:---:|:---:|
| June 2025 | Good | Bad | Bad (35 vs 60 GW) | Bad |
| April 2026 | Bad | Good | Good | Good |

Result: all 15 analogs are recent mild spring dates. Even with weather weights boosted to 6.0 (from 0), only 2 of 15 analogs shifted to May 2025. The other ~40 points of feature weight across outages, gas, load, LMP, and calendar anchor to recent spring.

### Hard filter cascade compounds the problem

Before distance is even computed, pre-filters shrink the pool:

1. **Calendar filter** (+/-60d season window) -- June dates enter the pool
2. **LMP regime filter** -- removes dates with different price regimes
3. **Outage regime filter** (1.5 std tolerance) -- spring maintenance at 60 GW vs summer at 35 GW is a ~2 std gap, eliminating most summer dates

### Shape vs level: why a market adjustment layer doesn't help

Scaling a mild spring LMP profile to match $84 on-peak produces the wrong hourly shape:

- **Spring shape**: low flat afternoon, moderate evening ramp, deep overnight valley
- **June heat shape**: extended afternoon plateau (AC load HE12-18), steep evening ramp (solar cliff + AC persistence), higher overnight floor

Multiplying a spring curve by 1.7x gives the right level but the wrong ramps, peaks, and valleys.

### Key difference from LASSO QR

LASSO QR's backward bias is in **feature importance** -- the coefficients favor yesterday's LMP over tomorrow's load. This is fixable by reweighting features, dropping backward features, or multi-window ensembles.

The like-day model's backward bias is **structural** -- the output IS a historical profile. No amount of feature reweighting changes this. Even if analog selection perfectly identifies "the most similar historical day," the forecast is still bounded by that day's actual LMP.

### Possible directions for like-day

1. **Separate shape from level** -- use weather/load analogs for the hourly shape, then scale to a level informed by gas/outages/forward curves
2. **Dynamic feature weighting** -- detect extreme weather forecasts and automatically boost weather/load weights (like the existing adaptive filter does for extreme LMP, but triggered by forward-looking signals instead)
3. **Expand the data window** beyond 3 years to increase chance of finding compound events
4. **Hybrid handoff** -- use like-day for normal days, switch to LASSO QR or LightGBM when the model diverges from forward markets by more than a threshold
