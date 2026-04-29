# Agent 1 — Literature & Spec Review: Forward-Only KNN

Scope: anchors the spec side of the spec ↔ implementation matrix for the
`forward_only_knn` day-ahead price model. All page/line citations are to files
under `modelling/@TODO/pjm-research-for-modelling/`. Primary sources:

- `pjm-like-day-research.md` (general like-day / KNN methodology — 948 lines)
- `forward_only_knn.md` (spec for the forward-only variant — 196 lines)

Secondary cross-reference docs (skimmed only):
`backward_vs_forward_looking.md`, `hourly_vs_daily_features.md`,
`mean_vs_median.md`, `pjm_data_sources.md`.

---

## 1. Methodology summary

**General like-day / KNN approach** (`pjm-like-day-research.md`). The model is
an analog ensemble for PJM day-ahead LMP. For a target delivery day, identify
the K most similar historical days ("analogs") by computing a feature-space
distance over day-level descriptors (load, weather, gas, calendar), then use
the actual realized DA LMP profiles of those K analogs as an empirical
ensemble for the forecast. Pointwise quantiles across the K analog hourly
profiles (optionally inverse-distance weighted) deliver a probabilistic
forecast (`pjm-like-day-research.md` §3.3 Step 4 lines 306–386). The
methodology lineage is Che & Chen (2007) on PJM specifically (lines 7–16) plus
the meteorological Analog Ensemble of Delle Monache et al. (2013) (lines
80–90), with weighted-KNN refinements from Lora et al. (2007) (lines 43–51).
The literature recommends matching on **drivers** of price (load forecast,
temperature, gas, calendar) rather than on price itself to avoid circularity
when forecasting (lines 257–274), z-score normalization to prevent scale
domination (line 116), inverse-distance weighting (lines 50, 327–339), and
K=10–30 with K=20 a common default (lines 296–304).

The general approach also describes a four-tier feature hierarchy (Tier 1:
load forecast, day-of-week group, gas price, temperature; Tier 2: month,
renewables forecast, recent DA LMP, holiday flag; Tier 3: congestion, net
imports, reserve margin, prior-week-same-day LMP — lines 392–421) plus three
probabilistic-output methods ordered by complexity: empirical quantiles, KDE,
conformal prediction (lines 308–386, 450–467). Hard filters (day-type,
season, holiday) are recommended pre-filters; soft features feed the distance
(lines 425–438).

**Forward-only variant** (`forward_only_knn.md`). The spec inverts the framing
of the legacy "Like-Day" model. Legacy semantics: "match today's state, use
what followed" (lines 6–7). Forward-only semantics: "match tomorrow's setup,
use the matched day's price" (line 8). Concretely: the **query row** is built
from the target delivery date `T`'s forecasted/forward-looking inputs (load
forecast, weather forecast, etc.), and the **pool rows** are historical
delivery dates `D` whose features represent that day's *conditions* under a
consistent rule, with labels being the actual `lmp_h1..lmp_h24` for that same
`D` (`forward_only_knn.md` lines 75–87). This is a ship-now MVP — no
backtesting, no CV tuning, no evaluation notebooks in Phase 1 (lines 19–24).
The non-negotiable live guardrails are as-of data discipline, pool-only
scaling, a minimum-analog fallback ladder, and explicit horizon feature
gating (lines 27–46).

---

## 2. Spec elements (the canonical checklist)

| Element | Spec value / definition | Source (file:section/line) |
|---|---|---|
| **Target variable** | Day-ahead LMP at PJM Western Hub. Pool labels are `lmp_h1..lmp_h24` for the historical delivery date `D`. Output is a per-hour point forecast plus quantile bands. | `forward_only_knn.md:79` (pool label spec); `pjm-like-day-research.md:392` (PJM Western Hub focus); `forward_only_knn.md:130-132` (output: hourly point forecast + hourly quantiles + analog table) |
| **Forecast horizon** | D+1 by default; D+1..D+N strip supported. | `forward_only_knn.md:17` ("Support D+1 and strip (D+1..D+N) inference"); `forward_only_knn.md:124-141` (`forecast.py` default `target_date = today + 1 day`; `strip_forecast.py` iterates over horizon) |
| **Granularity** | Hourly (24 hours, HE1..HE24). Daily features used for matching, hourly used for labels/output. | `forward_only_knn.md:148` (`HOURS = [1..24]`); `forward_only_knn.md:79` (`lmp_h1..lmp_h24`); `hourly_vs_daily_features.md:11-13` (day-as-row design); `forward_only_knn.md:131-132` (hourly point forecast, hourly quantiles) |
| **Candidate features (load)** | `load_level`, `load_ramps` — high-coverage groups. Daily aggregates: `daily_avg`, `daily_peak`, `daily_valley`, `ramp_max`, `morning_ramp` (HE8−HE5), `evening_ramp` (HE20−HE15). | `forward_only_knn.md:94-95`; `hourly_vs_daily_features.md:38-46` (aggregation rules — but this is implementation-derived, not pure spec) |
| **Candidate features (weather)** | `weather_level`, `weather_hdd_cdd`. | `forward_only_knn.md:96` |
| **Candidate features (gas)** | `gas_level` — **optional; only when reliable for that horizon**. | `forward_only_knn.md:98` ("optional: `gas_level` and `ice_forward_level` only when reliable for that horizon") |
| **Candidate features (forwards)** | `ice_forward_level` — optional, horizon-gated. | `forward_only_knn.md:98` |
| **Candidate features (calendar)** | `calendar_dow` (day-of-week). | `forward_only_knn.md:97` |
| **Candidate features (deferred)** | Outage and renewables groups deferred to Phase 2 unless coverage is solid. | `forward_only_knn.md:99` ("Defer outage/renewables to Phase 2 unless coverage is already solid"); `forward_only_knn.md:192` (Phase 2: "Add outage and renewable feature groups") |
| **Similarity / distance metric** | Per-group NaN-aware distance, then weighted sum across groups. Specific scalar form (Euclidean / Manhattan / Mahalanobis / cosine) **not specified by the spec**. | `forward_only_knn.md:107-115` ("Compute NaN-aware per-group distance (skip missing dims, do not impute 0). Weighted sum over groups.") |
| **Feature scaling / normalization** | **Pool-only** z-score-style scaling per feature group. Fit normalization stats on candidate pool only; transform query with those pool stats; never include query in scaler fitting. | `forward_only_knn.md:32-37` (Guardrail 2: "Pool-only scaling"); `forward_only_knn.md:111` ("Normalize per feature group using pool-only stats") |
| **Neighbor pool / candidate window** | Strict **calendar filter** first; **fallback ladder** if pool too small: same DOW+holiday → same DOW → DOW group → no DOW hard filter. Minimum pool size enforced. | `forward_only_knn.md:39-43` (Guardrail 3 + ladder example); `forward_only_knn.md:108-110` ("Apply strict calendar filter. If pool too small, run fallback ladder to reach minimum size") |
| **Minimum pool size** | `MIN_POOL_SIZE = 150` after fallback. | `forward_only_knn.md:147` |
| **Number of neighbors K** | `DEFAULT_N_ANALOGS = 20`. | `forward_only_knn.md:146` |
| **K selection method** | Hand-set default in MVP. CV tuning is **explicitly out of scope** for Phase 1. | `forward_only_knn.md:21` (out of scope: "CV tuning workflows"); `forward_only_knn.md:146` (default 20); contrast `pjm-like-day-research.md:296-304` which recommends LOO-CV on CRPS — explicitly deferred here. |
| **Neighbor weighting** | Distances converted to analog weights summing to 1 ("Convert distance to analog weights (sum to 1)"). Specific functional form (e.g. `1/d`, `exp(-d)`) not pinned by spec. | `forward_only_knn.md:115` ("Convert distance to analog weights (sum to 1)") |
| **Aggregation across neighbors** | **Hourly point forecast = weighted mean**; quantiles = **weighted quantile**. | `forward_only_knn.md:131` ("hourly point forecast (weighted mean)"); `forward_only_knn.md:132` ("hourly quantiles (weighted quantile)") |
| **Quantile levels** | `QUANTILES = [0.10, 0.25, 0.50, 0.75, 0.90]`. | `forward_only_knn.md:149` |
| **Recency penalty** | "Optional recency penalty" applied before top-N selection. | `forward_only_knn.md:113` ("Apply optional recency penalty") |
| **Tie-breaking / determinism** | Stable sort on `(distance, date)` so ties are reproducible. | `forward_only_knn.md:118-119` |
| **Look-ahead / forward-only constraint** | Query row may use only inputs available at model run time for each target date (as-of discipline). | `forward_only_knn.md:29-31` (Guardrail 1) — see §3 below for full enumeration. |
| **Horizon feature gating** | Features explicitly enabled/disabled by horizon based on reliable availability. Missing groups must be **disabled explicitly, not silently zero-filled**. | `forward_only_knn.md:43-45` (Guardrail 4); `forward_only_knn.md:87` ("Missing groups for a horizon must be disabled explicitly, not silently zero-filled") |
| **Initial feature weights** | Hand-tuned: `load_level: 3.0`, `load_ramps: 1.0`, `weather_level: 2.0`, `weather_hdd_cdd: 2.0`, `calendar_dow: 1.0`, `gas_level: 2.0` (when enabled), `ice_forward_level: 2.0` (when enabled). | `forward_only_knn.md:150-158` |
| **Train/test split methodology** | **Not in MVP scope.** No train/test split, no backtest harness, no CV. Phase 1 ships live forecast path only. | `forward_only_knn.md:20-23` (out of scope: backtesting harness, CV tuning, evaluation notebooks) |
| **Evaluation metrics & benchmark** | **Not in MVP scope.** Only sanity-range checks ("no NaN/inf, no exploded outliers"). Formal metrics deferred to Phase 2. | `forward_only_knn.md:174-187` (MVP acceptance checks — schema/sanity only); `forward_only_knn.md:192-195` (Phase 2: "Add formal evaluation metrics and CV tuning. Add side-by-side benchmark reporting versus Like-Day") |
| **Post-processing (clipping/calibration/blending)** | Not specified. No conformal calibration, no blending, no clipping in MVP. | Absent from `forward_only_knn.md`. Contrast `pjm-like-day-research.md:371-386` (Method C conformal wrapper) and §5.3 (QRA blending) — both treated as future enhancements only. |
| **Output schema compatibility** | Output keys/tables align with existing like-day consumer expectations. | `forward_only_knn.md:133` ("Return same output schema used by like-day API consumers"); `forward_only_knn.md:184-185` (acceptance check 3) |
| **Feature namespace contract** | Pool/query feature names must be identical for all enabled feature groups. | `forward_only_knn.md:85-86` |

---

## 3. Forward-only constraints — explicit list

The spec's "forward-only" constraints, enumerated for Agent 2's data-leakage
audit:

1. **As-of data discipline (Guardrail 1).** "Query row may use only inputs
   available at model run time for each target date." (`forward_only_knn.md:30`)
2. **Pool feature consistency.** "Pool features must follow one consistent
   construction rule per feature group." (`forward_only_knn.md:31`) — i.e.,
   pool rows can't mix realized and forecast values within the same group
   without an explicit rule.
3. **Pool-only scaler fit.** "Fit normalization stats on the candidate pool
   only. Transform query with those pool stats. **Never include query in
   scaler fitting.**" (`forward_only_knn.md:33-36`)
4. **Horizon gating of unreliable feeds.** "Explicitly gate features by
   horizon based on reliable availability. If gas/ICE are not reliably
   available for D+k, disable those groups for that horizon."
   (`forward_only_knn.md:44-46`)
5. **No silent zero-fill.** "Missing groups for a horizon must be disabled
   explicitly, not silently zero-filled." (`forward_only_knn.md:87`) — silent
   zero-filling would equate to a leak/contamination of the distance metric.
6. **Pool/query feature namespace identity.** "Pool/query feature names must
   be identical for all enabled feature groups." (`forward_only_knn.md:86`)
7. **No label columns in query.** "No label columns in query."
   (`forward_only_knn.md:84`) — query row must not carry any `lmp_h*` columns.
8. **Query row is target-day-only.** "One row for target delivery date `T`."
   (`forward_only_knn.md:82`) — no leakage of `T+1` or beyond into a single
   query.
9. **No synthetic reference row in strip mode.** "No synthetic reference row."
   (`forward_only_knn.md:141`) — each day in a strip is forecast independently,
   not by chaining forward off a synthetic prior.
10. **Pool features represent that-day conditions.** "Feature columns
    represent that day conditions under a consistent rule."
    (`forward_only_knn.md:78`) — pool rows are descriptors of `D`, not
    forecasts produced earlier *for* `D`. (Note: this is silent on whether to
    use realized D-conditions or what-was-known-the-day-before for D — see
    Ambiguities §4.)
11. **NaN-aware distance (no zero imputation).** "Compute NaN-aware per-group
    distance (skip missing dims, do not impute 0)."
    (`forward_only_knn.md:111-112`) — preventing zero-fill from masquerading
    as similarity is a leakage/integrity guard.

Not strictly leakage-related but listed in the same guardrails block:

12. **Fallback ladder, not silent shrinkage.** Pool relaxation is via a
    fixed, declared ladder, never an ad-hoc resize.
    (`forward_only_knn.md:39-42`)

---

## 4. Ambiguities & unresolved choices

The spec defers or leaves open the following decisions:

1. **Distance metric scalar form.** The spec says "Compute NaN-aware
   per-group distance" and "Weighted sum over groups"
   (`forward_only_knn.md:111-114`) but never names whether the per-group
   distance is Euclidean, Manhattan, Mahalanobis, or cosine. The general doc
   recommends cosine for shape + Euclidean for level, or Mahalanobis
   (`pjm-like-day-research.md:282-291`), but the forward-only spec does not
   pick one.

2. **Analog-weight functional form.** "Convert distance to analog weights
   (sum to 1)" (`forward_only_knn.md:115`) — `1/d`, `exp(-d/τ)`, kernel-based,
   and softmax-on-negative-distance all satisfy this. The general doc shows
   `weights = 1.0 / (distances + 1e-8)` in example code
   (`pjm-like-day-research.md:483-484`) but the forward-only spec does not
   pin this.

3. **Recency penalty form and default.** "Apply optional recency penalty"
   (`forward_only_knn.md:113`) — neither shape (linear / exponential /
   step), nor decay constant, nor default on/off state is specified.

4. **K selection.** Default `DEFAULT_N_ANALOGS = 20` (`forward_only_knn.md:146`)
   but no method to override or tune. CV tuning is out of scope
   (`forward_only_knn.md:21`); the general doc recommends
   "leave-one-out cross-validation … evaluate CRPS"
   (`pjm-like-day-research.md:301-303`) — explicitly punted to Phase 2.

5. **Per-group feature weights are hand-tuned.** "Initial feature weights
   (hand-tuned)" (`forward_only_knn.md:150`) — no procedure to refine them.
   Contrast Lora et al. genetic-algorithm weight optimization referenced in
   `pjm-like-day-research.md:48-51`.

6. **Holiday handling.** The fallback ladder mentions "same DOW+holiday"
   (`forward_only_knn.md:42`) but `calendar_dow` is the only listed calendar
   feature group (`forward_only_knn.md:97`). Whether holidays are encoded as
   a feature, used as a hard filter, or only via the fallback ladder is not
   pinned. The general doc strongly recommends "Holiday indicator … Use as
   **hard filter**" (`pjm-like-day-research.md:412`); the forward-only spec
   does not adopt this explicitly.

7. **Realized vs forecast load on the pool side.** Pool spec says "Feature
   columns represent that day conditions under a consistent rule"
   (`forward_only_knn.md:78`). Whether that rule uses realized load (the
   value that actually happened on `D`) or vintage-matched forecast load
   (what was forecast for `D` at the time-equivalent horizon) is left open.
   `pjm_data_sources.md:60` describes a vintage-archived
   `historical_load_forecasts` feed but the forward-only spec does not
   commit to vintage matching for the MVP.

8. **Missing-weather-forecast handling for query.** Guardrail 4 says disable
   groups when not reliably available (`forward_only_knn.md:44-46`), but
   "reliably available" is not quantified — no threshold on coverage,
   freshness, or per-hour completeness.

9. **Pool size for strip mode beyond D+1.** `MIN_POOL_SIZE=150` is stated
   once (`forward_only_knn.md:147`); it is unclear whether this floor varies
   with horizon when feature groups are disabled.

10. **Output schema definition.** "Return same output schema used by like-day
    API consumers" (`forward_only_knn.md:133`) — the schema is referenced but
    not enumerated in this spec, so what counts as "schema-compatible" is
    by-reference and may drift.

11. **Sanity range thresholds.** "Forecast HE values and on-peak/off-peak
    aggregates are numerically plausible (no NaN/inf, no exploded outliers
    from weighting bugs)" (`forward_only_knn.md:186-187`) — "plausible" and
    "exploded outliers" are not numerically defined.

12. **Cache invalidation.** "Wire minimal logging and cache hooks"
    (`forward_only_knn.md:170`) and "Build pool (cached)"
    (`forward_only_knn.md:127`) — no cache key, TTL, or invalidation policy
    specified.

13. **Quantile aggregation method.** "hourly quantiles (weighted quantile)"
    (`forward_only_knn.md:132`) — there are several conventions for weighted
    quantiles (interpolation between order statistics vs the
    cumulative-weight threshold approach in `pjm-like-day-research.md:332-339`).
    The spec does not pick one.

14. **Mean vs P50 for the headline forecast.** Spec lists weighted mean as
    the point forecast (`forward_only_knn.md:131`) and weighted quantile
    P50 separately (`forward_only_knn.md:149`). The cross-reference doc
    `mean_vs_median.md:1-7, 90-95` notes these can diverge significantly on
    skewed analog distributions — the spec does not say which to surface as
    "the forecast" to consumers.

**Total ambiguities flagged: 14.**

---

## 5. Cross-doc disagreements

Notable places `pjm-like-day-research.md` and `forward_only_knn.md` differ.
Surfaced as conflicts; not adjudicated.

1. **Matching framing — outcomes vs drivers, with respect to LMP as a feature.**
   - `pjm-like-day-research.md:259` says: "the literature strongly recommends
     matching on the **drivers** of price rather than price itself, because
     the goal is to find days whose conditions were similar … Matching on
     outcomes creates a circular dependency when forecasting." It then
     includes "Recent DA LMP level" only as a Tier 2 *contextual* feature
     (`pjm-like-day-research.md:411`).
   - `forward_only_knn.md:91-99` lists no LMP-based feature groups in the MVP
     feature set at all (consistent with drivers-only). **No actual
     disagreement on the inclusion of LMP** — both align to drivers — but
     the framing differences are worth noting: the general doc names "Recent
     DA LMP level" as Tier 2, the forward-only spec drops it. Treat as
     **silent restriction**, not contradiction.

2. **K default.**
   - `pjm-like-day-research.md:303` recommends "Start with k=20 as a
     default, test range [5, 10, 15, 20, 25, 30, 40, 50]." Also
     `pjm-like-day-research.md:678`: "Increase default k from 5 to 20 …"
   - `forward_only_knn.md:146` sets `DEFAULT_N_ANALOGS = 20`.
   - **Aligned**, no disagreement.

3. **Hard filters: holidays and day-type.**
   - `pjm-like-day-research.md:425-427` (Hard Filters section): "Same
     day-type: weekday-to-weekday, weekend-to-weekend, holiday-to-holiday";
     `pjm-like-day-research.md:412`: "Holiday indicator … Use as **hard
     filter**".
   - `forward_only_knn.md:42` mentions "same DOW+holiday" as the *strictest
     rung of a fallback ladder*, not as a baseline filter.
   - **Tension**: the general doc treats holiday-matching as a non-negotiable
     pre-filter; the forward-only spec treats it as the strict end of a
     ladder that gets relaxed if pool is too small. Quote both for the
     reviewer:
     - General: "Holiday indicator … Reduce commercial/industrial demand
       significantly. Use as **hard filter** (match holidays to holidays)."
       (`pjm-like-day-research.md:412`)
     - Forward-only: "If strict calendar filter returns too few rows,
       relax in a fixed ladder until minimum pool size is met. Example
       ladder: same DOW+holiday -> same DOW -> DOW group -> no DOW hard
       filter." (`forward_only_knn.md:39-42`)

4. **Feature scope (Tier 1 features missing from MVP).**
   - `pjm-like-day-research.md:399-403` Tier 1 includes **natural gas price**
     as "Must Include": "Gas plants set the marginal price ~60% of the time
     in PJM."
   - `forward_only_knn.md:98` lists `gas_level` as **optional**: "only when
     reliable for that horizon".
   - **Disagreement on tier**: literature says must-include, MVP says
     optional. The forward-only spec is more conservative because of the
     horizon-gating discipline.

5. **Renewables and outages.**
   - `pjm-like-day-research.md:410-413` Tier 2 "Should Include" — renewable
     generation forecast and (Tier 3) reserve margin / outages.
   - `forward_only_knn.md:99` defers outages and renewables to Phase 2:
     "Defer outage/renewables to Phase 2 unless coverage is already solid."
   - **Disagreement on inclusion timing**: the general doc places these in
     Tier 2/3 of the recommended feature set; the spec defers them.

6. **Probabilistic output method.**
   - `pjm-like-day-research.md:308-386` recommends an explicit menu (Method
     A: empirical quantiles → Method B: KDE → Method C: conformal prediction)
     in implementation order.
   - `forward_only_knn.md:131-132` specifies only "hourly point forecast
     (weighted mean)" + "hourly quantiles (weighted quantile)" — i.e.
     Method A only, no KDE, no conformal.
   - **Aligned** on starting with Method A, but the forward-only spec is
     silent on whether KDE/conformal are roadmap items at all.

7. **Evaluation: CRPS, reliability diagrams, etc.**
   - `pjm-like-day-research.md` §6 (lines 618–651) makes evaluation a core
     part of the methodology (CRPS, pinball loss, PIT histograms, reliability
     diagrams).
   - `forward_only_knn.md:174-187` MVP acceptance is sanity checks only;
     formal metrics deferred (line 194).
   - **Disagreement in priority**: general doc treats evaluation as
     methodology-essential; spec deprioritizes for MVP.

8. **Distance metric.**
   - `pjm-like-day-research.md:282-291` recommends "cosine similarity as the
     default for hourly profile shape matching. Add Mahalanobis distance as
     an option for daily aggregate feature matching. Use cosine for 'shape
     similarity' and Euclidean for 'level similarity' — then blend."
   - `forward_only_knn.md:111-114` is silent on the scalar metric, only
     specifying "NaN-aware per-group distance" + "weighted sum over groups."
   - **Disagreement by silence**: general doc gives a concrete prescription;
     spec leaves the choice unstated.

9. **Inverse-distance weighting form.**
   - `pjm-like-day-research.md:50, 327-339, 483-484` specifies
     `weights = 1.0 / distances` then normalize.
   - `forward_only_knn.md:115` only requires "Convert distance to analog
     weights (sum to 1)."
   - **Disagreement by silence**, see Ambiguity §4.2.

10. **Mean vs median framing.**
    - `pjm-like-day-research.md:319-322` shows median (P50) as the headline
      "median forecast" and uses `np.average(...)` for a separate "mean"
      summary statistic (lines 503-505).
    - `forward_only_knn.md:131-132` lists "weighted mean" as the hourly
      point forecast and quantiles separately, with P50 just one of five
      quantile levels. The cross-doc `mean_vs_median.md` notes these can
      differ materially on skewed distributions.
    - **Tension**: which statistic is "the forecast" is presented
      differently. Forward-only: weighted mean. General doc: median, with
      mean as supplemental.

---

## End-of-document references

Inline `file.md:line` and `file.md § Section` citations appear throughout.
Files cited:

- `modelling/@TODO/pjm-research-for-modelling/pjm-like-day-research.md`
- `modelling/@TODO/pjm-research-for-modelling/forward_only_knn.md`
- `modelling/@TODO/pjm-research-for-modelling/backward_vs_forward_looking.md`
- `modelling/@TODO/pjm-research-for-modelling/hourly_vs_daily_features.md`
- `modelling/@TODO/pjm-research-for-modelling/mean_vs_median.md`
- `modelling/@TODO/pjm-research-for-modelling/pjm_data_sources.md`

Cross-references to `historical_forecasts.md` (mentioned by
`hourly_vs_daily_features.md:81-82, 96` and `pjm_data_sources.md:9, 150`)
were noted but not chased — load-bearing only for the vintage-matching
question called out in Ambiguity §4.7.
