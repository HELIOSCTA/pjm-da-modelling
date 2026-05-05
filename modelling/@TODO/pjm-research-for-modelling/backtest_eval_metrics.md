# Backtest Evaluation Metrics for `pjm_rto_hourly`

Decision doc — not yet implemented. Sister doc to `pjm-like-day-research.md`.

## 1. The Problem

`single_day_backtest.py` currently scores forecasts in two extra sections:
**SHAPE METRICS** and **WINDOW MAE**, both keyed off hardcoded clock-hour
windows in `_SHAPE_WINDOWS` and `_MAE_WINDOWS` (e.g.
`morning_peak = HE7-9`, `evening_peak = HE19-21`,
`midday_valley = HE12-15`).

This breaks the moment the windows shift. They shift constantly:

- **Evening peak** slides from ~HE17-18 in December (early sunset) to
  ~HE20-21 in July (AC + late sunset).
- **Midday valley** is a real solar-driven trough only from roughly Apr-Sep;
  in January it disappears or inverts (winter morning peak ≈ midday).
- **Morning ramp** moves with sunrise + commercial open (HE6-8 summer,
  HE7-9 winter).
- **Holiday and weekend profiles** have entirely different anchor points —
  a Saturday "peak" sits where a weekday "valley" would.

So a metric that says "morning_peak = mean of HE7-9" is wrong on most of
the calendar. The reported "morning peak error" is then a confounded
mixture of (a) shape error and (b) "we measured the wrong hours."

**The same problem hits `param_sweep.py`, not just the single-day display.**
The sweep leaderboard ranks scenarios by `mean_rmae` aggregated across
~10 weekday targets. If shape accuracy never enters the leaderboard,
weight tuning will keep optimizing against a level-only target and the
"winning" scenario will be the one that gets the daily mean right while
missing every evening peak. The shape metrics defined below must be
computed in shared code and aggregated in both harnesses.

## 2. What the Literature Recommends

Synthesized from `pjm-like-day-research.md` plus a targeted lit search
on hour-agnostic / data-adaptive shape metrics for hourly profile
forecasts (electricity price, load, net-load).

The common thread: **derive the windows from the actual profile, not
the clock.**

### 2.1 Top three hour-agnostic candidates

#### 2.1.1 Auto-detected peak/valley + time-of-extremum errors

Compute `argmax` / `argmin` on each profile, then report **three
distinct quantities** (the previous version of this doc collapsed them,
which was wrong):

- `peak_height_err = max(forecast) - max(actual)` — magnitude difference
  between the two peaks even if they occur at different hours. Answers
  "did we predict the right peak height?"
- `peak_at_actual_hour_err = forecast[argmax(actual)] - actual[argmax(actual)]`
  — error of the forecast at the hour the actual peak occurs. Answers
  "given when the peak really happened, how close were we?" Penalizes
  timing miss differently from height miss.
- `time_of_peak_err = argmax(forecast) - argmax(actual)` (signed, in
  hours). Negative = forecast peaked early.

Same three for valley (`min` instead of `max`). Plus a
`peak_window_mae = mean(|forecast - actual|)` over
`[argmax(actual) - 1, argmax(actual) + 1]`, edge-clipped to `[0, 23]`.

Trivial (~20 LOC numpy), hour-agnostic, immediately legible to traders.
This is the practitioner default (Yes Energy "timing win rate";
Frontiers in Energy Research 2022 peak-day/peak-hour study, Ref [3]).

#### 2.1.2 Variogram score of order p (Scheuerer & Hamill 2015)

Penalizes mis-modeled hour-to-hour *differences* — the structural
property that makes a profile "ramp-shaped." For a single deterministic
forecast `x` against actual `y`, both length-24 vectors:

```
VS_p(x, y) = Σ_{0 ≤ i < j ≤ 23}  w_{ij} · ( |y_i - y_j|^p − |x_i - x_j|^p )^2
```

Locked specification:

- Index pairs are `i < j` only (each pair counted once, no duplicates,
  no `i = j` diagonal — the diagonal is identically zero and contributes
  nothing).
- **`p = 0.5` (locked).** Scheuerer & Hamill 2015 (Ref [2]) and the EPF
  follow-up literature default. Empirically (see "Empirical sensitivity"
  below), the relative ranking of weight schemes is invariant to `p`,
  but `p = 1.0` blows score scales out by ~50-100x on peaked days
  because the few large peak-vs-trough deltas dominate. `p = 0.5`
  keeps scores O(1) and down-weights outlier deltas.
- **`w_{ij} = 1 / |i - j|` (locked).** All three candidate weight
  schemes are equally translation-invariant — see caveat below — so the
  decision is about which lag structure best reflects "ramp." Locked
  rationale:
  - **uniform** weights all 276 pairs equally; with 23 lag-1 pairs vs
    1 lag-23 pair, mid/long-lag pairs collectively dominate the count
    and pull the metric toward "endpoints differ" rather than local
    ramp shape.
  - **`1 / |i - j|^2`** collapses essentially all weight onto lag-1
    and lag-2 pairs, throwing away the morning-ramp / evening-peak
    structure that spans 6-12 hours — exactly the structure a like-day
    model is supposed to capture.
  - **`1 / |i - j|`** is the Scheuerer-Hamill default and the right
    middle ground: preserves sensitivity to multi-hour ramps while
    still emphasizing local hour-to-hour deltas. Empirical
    timing-shift / shape-distortion ratio ≈ 3.1 across representative
    weekday/winter/weekend profiles.
- Normalize by sum of weights so the score is on a comparable scale
  across days: `VS_p / Σ w_{ij}`. Without this, the absolute number is
  arbitrary and only relative rankings are meaningful. For
  cross-day aggregation in `param_sweep` consider also normalizing by
  `actual.std()` per day to make magnitudes comparable across calm
  vs volatile days; defer that decision until the metric is in.

~20 LOC numpy on the 24×24 pairwise-diff matrix. Recommended in recent
EPF literature (ASCMO 2025, Ref [4]; arXiv:2504.02518, Ref [5]).

**Caveat — must be paired with a level metric, never used alone.**
Variogram is shape-only by *metric form*, not by weight choice — every
weight scheme above (uniform, `1/lag`, `1/lag^2`) gives variogram = 0
for any constant additive bias, because pairwise differences are
translation-invariant: `(y_i + c) - (y_j + c) = y_i - y_j`. So a
forecast that is `actual + $50` at every hour scores a perfect
variogram of zero while being a $50/MWh systematic miss on MAE and
CRPS. Variogram is a **secondary shape KPI**, ranked alongside
rMAE/CRPS but never replacing them as the headline objective.

**Empirical sensitivity (single-shot experiment, scratch).** Three
canonical synthetic forecasts on a representative weekday profile
(overnight $20 → morning ramp → midday $40 → evening peak $70 at HE19):

| forecast regime | uniform | `1/lag` (chosen) | `1/lag^2` |
|---|---:|---:|---:|
| constant +$10 offset | 0.000 | 0.000 | 0.000 |
| amplitude flatten 50% | 1.146 | 0.808 | 0.543 |
| timing shift +2h | 3.074 | 2.476 | 1.712 |

Scheme rank A > B > C identical at `p = 1.0`. Profile choice
(weekday peak / winter morning peak / weekend flat) shifts absolute
magnitudes ~3-5x but the rank ordering and the timing/shape ratio
are stable.

#### 2.1.3 Swinging-Door ramp score (Florita / NREL)

Run SDA on actual and forecast profiles to extract ramp segments
`(start, end, magnitude, duration)` data-adaptively, then score with
precision/recall/F1 on event matching (with a tolerance window) plus
magnitude/duration MAE on matched ramps. The dominant ISO/utility
approach for ramp accuracy in wind, solar, net-load, and load
forecasting. CAISO duck-curve work uses this family (Ref [6]).

Medium effort (~40 LOC numpy + one tolerance parameter). **Defer to v2**
— (2.1.1) and (2.1.2) cover the same ground at lower cost for our
purposes.

### 2.2 Other options worth knowing about

- **First-difference MAE / Pearson on Δprofile.** 5-line "poor man's
  variogram" — `MAE(diff(forecast), diff(actual))`. Cheap diagnostic
  complement to (2.1.2). With `w_{ij} = 1/|i-j|^∞` the variogram
  collapses to first-differences only — same idea, simpler form.
- **DILATE (soft-DTW + Temporal Distortion Index, Ref [7]).**
  Decomposes error into a *shape* term and a *timing* term. The right
  tool when you want to say "we got the shape right but rang the
  evening peak 90 min late." Medium effort.
- **Wavelet-band MAE.** DWT (db4, level 3) decomposes each profile into
  baseload + 3 detail bands; report MAE per band. Good for diagnosing
  *what kind* of error dominates. Less established as a headline KPI.
- **Variogram / energy score for probabilistic outputs** — proper
  multivariate scoring rules. The probabilistic generalization of
  (2.1.2).
- **Pinball loss stratified by data-adaptive regime** (e.g. "ramping up",
  "near peak", "trough" segments from SDA) instead of by clock hour.
  GEFCom2014/2017 standard, restratified.

### 2.3 What we keep using regardless

- **Per-hour MAE** — already there, unchanged.
- **CRPS, pinball, coverage, sharpness** — already in `metrics.py`,
  unchanged. These are the universal industry standard from GEFCom.
  They are hour-agnostic by construction (they aggregate over hours
  uniformly) but they are *also* not shape-aware on their own — getting
  the level right with the wrong shape can still score well.

## 3. Recommended Replacement — V1 Scope

Replace the two hardcoded sections, **and put the new code in shared
metric infrastructure used by both backtest harnesses.**

### 3.1 Where the code lives

Add a new function to `pjm_rto_hourly/metrics.py` (sibling to the
existing `evaluate_forecast`):

```python
def evaluate_shape(actual: np.ndarray, forecast: np.ndarray) -> dict:
    """Hour-agnostic shape/ramp metrics. Both arrays length 24.
    Returns a flat dict of scalars (suitable for parquet rows)."""
```

Return convention (locked, must mirror `evaluate_forecast`):

- Flat dict of `float` scalars. **No nesting.**
- Nullable values use `float("nan")`, **never `None`, never absent
  keys.** This matches `metrics.py::evaluate_forecast` (verified) and
  is required for parquet round-tripping in `param_sweep.py` —
  `pd.DataFrame(rows).to_parquet()` infers schema dynamically and
  represents `nan` natively, but `None` and absent keys collide with
  inferred dtypes.

Returned keys:

- `peak_height_err`
- `peak_at_actual_hour_err`
- `time_of_peak_err` (signed, hours; integer in current spec)
- `valley_height_err`
- `valley_at_actual_hour_err`
- `time_of_valley_err`
- `peak_window_mae` (mean abs err over `actual_argmax ± 1h`,
  edge-clipped)
- `first_diff_mae` (cheap shape sanity check)
- `variogram_score_p05` (the headline shape number; secondary to
  rMAE/CRPS)

### 3.2 Per-extremum implementation contract

To stop the implementation from depending on memory:

- **NaN handling.** If `actual` contains any NaN, return a dict with
  every value set to `float("nan")` (no actuals → no shape eval). Same
  if `forecast` contains NaN, plus log once at the call site. Do not
  silently mask. Never return `None` or omit keys (see return
  convention above).
- **Ties.** `argmax` returns the *first* occurrence of the max
  (numpy default). Document this — ties are rare in $/MWh data but
  matter for synthetic test cases.
- **Edge clipping.** `peak_window_mae` window is
  `range(max(0, argmax(actual) - 1), min(24, argmax(actual) + 2))` —
  always 2 or 3 hours, never wraps midnight.
- **No smoothing in v1.** Raw `argmax` / `argmin` on the integer hour
  grid. If 1-hour jitter shows up as a problem in practice (forecast
  peaks at HE19, actual at HE20, both within $0.50 of each other —
  reports a 1h timing miss that isn't really a miss), add parabolic
  interpolation around the extremum in v2. Don't pre-smooth the profile
  before extremum detection — that distorts the height.

### 3.3 Variogram implementation contract

```python
def variogram_score(actual, forecast, p=0.5):
    n = len(actual)              # 24
    i, j = np.triu_indices(n, k=1)   # i < j, no diagonal
    lag = j - i                       # 1..23
    w = 1.0 / lag                     # short-lag emphasis
    a = np.abs(actual[i] - actual[j]) ** p
    f = np.abs(forecast[i] - forecast[j]) ** p
    return float((w * (a - f) ** 2).sum() / w.sum())
```

276 pairs (= 24·23/2), each weighted by inverse lag, normalized by sum
of weights. Comparable across days.

### 3.4 Wiring into the harnesses

**`single_day_backtest.py`** (per-day display): replace `_SHAPE_WINDOWS`
+ `_MAE_WINDOWS` blocks with one section that prints the
`evaluate_shape` dict per scenario, formatted as a small table. Drop
`_window_mean`, `_window_abs_err_mean`, `_print_shape_metrics`,
`_MAE_WINDOWS`, `_SHAPE_WINDOWS`.

**`param_sweep.py`** (cross-day aggregation, **required in v1**):

Verified: `param_sweep.py` builds rows as bare dicts inside
`_execute_scenario` and writes via `pd.DataFrame(rows).to_parquet()`
with no explicit schema (`backtest/param_sweep.py:106-164, 246-249`).
Adding ~9 nullable float columns is a no-op on the writer side — pandas
infers dtype from the dict values. The JSON sidecar
(`<sweep_id>_scenarios.json`) carries only the SCENARIOS dict, not
per-row data, so it's unaffected.

- Per `(target_date, scenario)` row in the parquet: spread the
  `evaluate_shape` dict directly into the existing row dict alongside
  `mae`/`rmse`/`rmae`/`crps`. No schema changes needed.
- In the printed leaderboard, add **four** columns (locked decision —
  report time-of-peak as both signed bias and absolute magnitude, since
  they answer different questions):
  - `mean_variogram_p05` (headline shape number)
  - `mean_abs_time_of_peak_err` (magnitude — "how far off in hours")
  - `mean_signed_time_of_peak_err` (bias — "systematically early/late")
  - `mean_peak_height_err`
- Keep the leaderboard *primary sort* on `mean_rmae` (forecast quality
  comes first), but show the shape columns alongside so a scenario that
  wins on rMAE but loses badly on shape is visible. Add a secondary
  sort key option (module-level constant, no CLI) so we can re-rank
  by shape when investigating.

### 3.5 LOC budget

| Change | LOC |
|---|---|
| `metrics.py::evaluate_shape` + `variogram_score` | ~50 |
| `metrics.py` `__main__` smoke tests (3 synthetic cases, inline) | ~25 |
| `single_day_backtest.py` swap (delete + replace) | net: -30 |
| `param_sweep.py` row spread + leaderboard cols | ~20 |
| **Total** | **~65 net new LOC** |

Test home is locked: inline `__main__` block in `metrics.py`. Repo has
no pytest infrastructure (no `tests/` dir, no `conftest.py`, no
`pyproject.toml` test deps), and existing scripts use `__main__` smoke
tests per `.claude/standards/python_scripts.md`. Five canonical cases:
(a) constant offset `+10` → `variogram=0`, `peak_height_err=+10` (the
peak rises by exactly the offset), `time_of_peak_err=0`, `first_diff_mae=0`;
(b) amplitude flatten → `variogram>0`, `peak_height_err<0` (peak
squashed), `valley_height_err>0` (valley raised), `time_of_peak_err=0`;
(c) `np.roll` shift by `+2h` → `time_of_peak_err=+2`, `peak_height_err=0`
(max preserved by roll), `time_of_valley_err=+2`;
(d) NaN actuals → all-NaN dict;
(e) edge case (peak at HE1) → `peak_window_mae` window clipped to
`[0, 2)`, no IndexError. Assert each, exit non-zero on failure.

## 4. Strategic Question — Analog Selection vs Eval Metrics

> *Is it better to focus on how analog days are selected vs eval
> metrics?*

**Analog selection is the lever; eval metrics are the dashboard.**
You can't improve what you can't measure, but elaborating the dashboard
doesn't move the needle on forecast accuracy — better selection does.
Concretely:

- **Selection wins are bounded by feature richness, not weight tuning.**
  The `param_sweep` exercise showed that across the current 9-group
  feature set, the best-vs-worst rMAE spread on a single day is small
  relative to the gap between *any* scenario and the naive baseline on
  hard days (e.g. 2026-05-05 with the $168 evening peak — every
  scenario blew up because no feature in the pool encodes
  scarcity / reserve margin). New *features* (scarcity signal,
  net-load forecast, gas-electricity ratio, holiday/Easter handling)
  are higher-leverage than re-weighting the existing nine.
- **But the eval metric defines what "better" means.** If we tune weights
  against daily MAE, we'll pick a config that gets the level right
  on quiet days even if it misses every evening peak. If we tune against
  variogram score, we'll pick one that tracks ramps even if the level
  drifts $2/MWh. These give different winners — the recent backtest
  showed `default` (outage-heavy) winning overnight/morning_ramp while
  `heavy_load_peak` won evening_ramp. Daily MAE hides that split.

So the practical priority order is:

1. **Fix the eval metrics first** (this doc → ~85 LOC across both
   harnesses). Cheap, and it stops us from drawing wrong conclusions
   from the next round of selection work. Without this, every "the new
   weights are better" claim is partly an artifact of which clock hours
   we happened to stratify on, and the `param_sweep` leaderboard keeps
   ranking on a level-only target.
2. **Then invest in features**, not weight tuning. Rank-order:
   - regime/scarcity features (reserve margin, forecast-vs-historical
     load gap, gas spot at DA cutoff)
   - holiday + day-type filtering done right (Good Friday → Sunday pool,
     not weekday pool — see `pjm-like-day-research.md` §5.3)
   - DA-cutoff-correct vintages everywhere (already done for load;
     verify for solar/wind/outages)
3. **Then revisit weight tuning** with the better metrics + better
   features. The current `param_sweep` harness is the right shape; just
   re-run it after (1) and (2).
4. **Distance metric experiments come last** (Mahalanobis, DTW,
   weighted-Euclidean genetic-algorithm tuning per Lora et al. 2007,
   Ref [8]). These are bounded improvements on top of an already-correct
   feature set; doing them before (1)-(3) overfits to the wrong target.

The honest answer to "which matters more": **selection matters more for
forecast accuracy, but eval metrics matter more for not fooling
ourselves about it.** Spend the small fixed cost on the metrics first,
then put the bulk of effort into selection (features, then filtering,
then distance metric — in that order).

## 5. Decisions Locked + Remaining V2 Questions

### 5.1 Locked for v1

| Decision | Choice | Rationale |
|---|---|---|
| Variogram weight | `w_{ij} = 1 / lag` | Empirical sensitivity check (§2.1.2): preserves multi-hour ramp sensitivity; uniform pulls toward endpoints, `1/lag^2` collapses onto lag-1/2. |
| Variogram order | `p = 0.5` | EPF default; keeps scores O(1); rank-ordering of weight schemes invariant to `p`. |
| Time-of-peak reporting | Both signed bias *and* absolute magnitude | Two columns in leaderboard. Answer different questions. |
| Test home | Inline `__main__` block in `metrics.py` | No pytest infra in repo; matches existing `__main__` smoke-test convention. |
| Return convention | Flat dict, `float("nan")` for nullables | Verified: matches `evaluate_forecast`. Required for parquet round-trip. |
| Param-sweep schema | No schema changes | Verified: `pd.DataFrame(rows).to_parquet()` infers dtype dynamically. |
| Smoothing of extrema | None | v1 uses raw `argmax`/`argmin`. Add parabolic interpolation in v2 only if 1h jitter shows up empirically. |

### 5.2 Deferred to v2

- Cross-day variogram normalization. `param_sweep` aggregates raw
  `variogram_score_p05` in v1. If absolute magnitudes prove
  incomparable across calm vs volatile days, switch to per-day
  normalization by `actual.std()` before aggregation.
- Swinging-Door ramp-event score (§2.1.3). Deferred per the original
  doc.
- Parabolic interpolation around extrema. Add only if empirical
  1-hour jitter is a real problem.

## 6. References

| Ref | Citation |
|-----|----------|
| [1] | Yes Energy. "How to Evaluate Power Demand Forecasts." https://www.yesenergy.com/blog/how-to-evaluate-power-demand-forecasts |
| [2] | Scheuerer, M. & Hamill, T. M. (2015). "Variogram-Based Proper Scoring Rules for Probabilistic Forecasts of Multivariate Quantities." Monthly Weather Review, 143(4), 1321–1334. https://journals.ametsoc.org/view/journals/mwre/143/4/mwr-d-14-00269.1.xml |
| [3] | "Predicting peak day and peak hour of electricity demand…" Frontiers in Energy Research, 2022. https://www.frontiersin.org/journals/energy-research/articles/10.3389/fenrg.2022.944804/full |
| [4] | "Multivariate scoring rules for probabilistic forecasts of energy time series." ASCMO, 2025. https://ascmo.copernicus.org/articles/11/23/2025/ |
| [5] | "Online Multivariate Regularized Distributional Regression for Probabilistic Electricity Price Forecasting." arXiv:2504.02518. https://arxiv.org/html/2504.02518v2 |
| [6] | Cui, M., Florita, A. R., Hodge, B.-M. (2015). "An Optimized Swinging Door Algorithm for Wind Power Ramp Event Detection." NREL/CP-5D00-63877. https://docs.nrel.gov/docs/fy15osti/63877.pdf |
| [7] | Le Guen, V. & Thome, N. (2019). "Shape and Time Distortion Loss for Training Deep Time Series Forecasting Models." NeurIPS. https://thome.isir.upmc.fr/papers/DILATE_neurips19.pdf — code: https://github.com/vincent-leguen/DILATE |
| [8] | Lora, A. T., Santos, J. M. R., Expósito, A. G., Ramos, J. L. M., Santos, J. C. R. (2007). "Electricity Market Price Forecasting Based on Weighted Nearest Neighbors Techniques." IEEE Trans. Power Systems, 22(3), 1294–1301. https://ieeexplore.ieee.org/document/4282040/ |
| [9] | `scoringrules` PyPI package (variogram + energy + CRPS in numpy). https://pypi.org/project/scoringrules/ |
| [10] | `properscoring` PyPI package. https://pypi.org/project/properscoring/ |
