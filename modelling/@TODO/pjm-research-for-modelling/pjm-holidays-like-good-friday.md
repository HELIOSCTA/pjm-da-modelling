# Soft Holidays in the Like-Day Pipeline

## Problem

The like-day model has no mechanism to distinguish holidays from normal weekdays. On 2026-04-03 (Good Friday), all 15 selected analogs were normal weekdays. The model forecast On-Peak at $54.66; the DA cleared at $47.30 — a +$7.36 error driven almost entirely by the holiday mismatch.

Good Friday is not a NERC holiday, so the existing `is_nerc_holiday` flag (which is already in the DB and feature matrix but unused in distance computation) doesn't help. PJM has ~10-15 "soft holidays" per year that suppress load 3-12% without being NERC-recognized.

## Current State

| Component | File | What Exists | Gap |
|-----------|------|-------------|-----|
| DB column | `pjm_dates_daily` table | `is_nerc_holiday` binary | No soft holidays; no holiday type/name |
| Calendar features | `calendar_features.py:42` | `is_nerc_holiday` hardcoded to 0 in `compute_for_date()` | Not used in distance; no soft holiday flag |
| Distance computation | `engine.py:85-88` | `calendar_dow` group uses only `is_weekend` | `is_nerc_holiday` not included |
| DOW groups | `configs.py:112-116` | 3 groups: weekday / saturday / sunday | No holiday group |
| Pre-filtering | `filtering.py:67-114` | Filters by DOW group + season window | No holiday-aware filtering |
| Scenarios | `scenarios.py:59-67,92-98` | `holiday` (pool override) + `good_friday_load` (-5% load) | Manual invocation only; not auto-detected |
| Regression | `regression_adjusted_forecast.py` | Linear sensitivities | No holiday-specific coefficients |

## PJM Soft Holiday Calendar

These dates are not NERC holidays but consistently suppress PJM load and flatten the price shape. Load impact estimates are approximate based on historical PJM metered load vs forecasts.

| Holiday | Rule | Typical Load Impact | Shape Effect |
|---------|------|-------------------|--------------|
| Good Friday | Friday before Easter | -5% to -8% on-peak | Flat morning, suppressed midday, weaker evening peak |
| Day after Thanksgiving | 4th Friday of November | -8% to -12% on-peak | Very flat; resembles Sunday |
| Christmas Eve (weekday) | Dec 24 if Mon-Fri | -5% to -10% on-peak | Early afternoon exodus; evening ramp weakened |
| New Year's Eve (weekday) | Dec 31 if Mon-Fri | -3% to -6% on-peak | Similar to Christmas Eve but less extreme |
| Easter Monday | Monday after Easter | -2% to -4% on-peak | Slight; some firms closed |
| Day before Christmas | Dec 23 if Mon-Fri | -2% to -4% on-peak | Minor; some early closures |
| Week between Christmas and New Year's | Dec 26-30 (weekdays) | -5% to -8% on-peak | Cumulative low-demand week; behaves like extended weekend |
| MLK Day (Mon) | 3rd Monday of January | NERC holiday | Already flagged — included for completeness |
| Presidents Day (Mon) | 3rd Monday of February | NERC holiday | Already flagged |
| Veterans Day (weekday) | Nov 11 if Mon-Fri | -1% to -3% on-peak | Mild; federal but not universal |
| Columbus Day (Mon) | 2nd Monday of October | -1% to -2% on-peak | Minimal; only federal offices |

### Holiday-adjacent effects

The day *before* and *after* major holidays can also shift load. For example, the Wednesday before Thanksgiving sees elevated morning load (early travel) but depressed afternoon load. The Monday after a Sunday holiday (e.g., July 5 when July 4 is Sunday) often carries a holiday hangover. These second-order effects are smaller (~1-2%) but can compound with other factors.

## Proposed Implementation

### Tier 1: Holiday calendar + auto-detection (config-only, no model changes)

**Goal:** When the forecast date is a soft holiday, automatically apply the existing `holiday` scenario overrides instead of requiring manual invocation.

**Changes:**

1. **Add `PJM_SOFT_HOLIDAYS` to `configs.py`** — a dict mapping holiday names to their detection rules and load impact factors:

```python
from dateutil.easter import easter

def _good_friday(year: int) -> date:
    return easter(year) - timedelta(days=2)

def _day_after_thanksgiving(year: int) -> date:
    # 4th Thursday of November + 1
    nov1 = date(year, 11, 1)
    first_thu = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    return first_thu + timedelta(days=22)  # 4th Thu = first + 21, +1 for Friday

PJM_SOFT_HOLIDAYS: dict[str, dict] = {
    "good_friday": {
        "date_fn": _good_friday,
        "load_impact_pct": -0.06,
        "dow_group_override": "sunday",  # treat as Sunday-like for analog matching
    },
    "day_after_thanksgiving": {
        "date_fn": _day_after_thanksgiving,
        "load_impact_pct": -0.10,
        "dow_group_override": "sunday",
    },
    "christmas_eve": {
        "date_fn": lambda y: date(y, 12, 24),
        "load_impact_pct": -0.07,
        "dow_group_override": "saturday",
        "weekday_only": True,  # only applies when Dec 24 falls on Mon-Fri
    },
    "new_years_eve": {
        "date_fn": lambda y: date(y, 12, 31),
        "load_impact_pct": -0.05,
        "dow_group_override": "saturday",
        "weekday_only": True,
    },
    # ... add more as needed
}
```

2. **Add `detect_soft_holiday()` to `calendar_features.py`:**

```python
def detect_soft_holiday(d: date) -> str | None:
    """Return the soft holiday name if d is a PJM soft holiday, else None."""
    for name, spec in configs.PJM_SOFT_HOLIDAYS.items():
        holiday_date = spec["date_fn"](d.year)
        if d == holiday_date:
            if spec.get("weekday_only") and d.weekday() >= 5:
                continue
            return name
    return None
```

3. **Auto-apply in `forecast.py:run()`** — at the top of the pipeline, before analog selection:

```python
holiday_name = detect_soft_holiday(target_date)
if holiday_name:
    spec = configs.PJM_SOFT_HOLIDAYS[holiday_name]
    logger.info(f"Soft holiday detected: {holiday_name} — applying pool overrides")
    # Override DOW group matching to pull in holiday-like analogs
    config.same_dow_group = False
    config.season_window_days = max(config.season_window_days, 90)
```

This is the minimal viable change. It makes the existing `holiday` scenario behavior automatic for known soft holidays.

### Tier 2: Add `is_holiday` to distance computation (feature change)

**Goal:** Make the similarity engine prefer holiday analogs over non-holiday analogs, even when the pool is mixed.

**Changes:**

1. **Add `is_soft_holiday` column to the feature matrix** in `calendar_features.py:build()`:

```python
result["is_soft_holiday"] = df["date"].apply(
    lambda d: 1 if detect_soft_holiday(d) else 0
)
# Also include NERC holidays
result["is_any_holiday"] = (
    (result["is_nerc_holiday"] == 1) | (result["is_soft_holiday"] == 1)
).astype(int)
```

2. **Update `compute_for_date()` to compute `is_soft_holiday`:**

```python
result["is_soft_holiday"] = 1 if detect_soft_holiday(d) else 0
result["is_any_holiday"] = max(result["is_nerc_holiday"], result["is_soft_holiday"])
```

3. **Add `is_any_holiday` to the `calendar_dow` feature group** in `engine.py`:

```python
"calendar_dow": {
    "columns": ["is_weekend", "is_any_holiday"],
    "default_metric": "euclidean",
},
```

4. **Increase the `calendar_dow` weight** when forecasting a holiday — either by bumping the static weight from 0.5 to 2.0 in configs, or by adding a dynamic weight override in the pipeline when a holiday is detected.

This ensures that when forecasting Good Friday (is_any_holiday=1), the distance metric penalizes non-holiday analogs (is_any_holiday=0) proportionally.

### Tier 3: Holiday-specific regression adjustment (model change)

**Goal:** When a soft holiday is detected, apply a separate load sensitivity coefficient that accounts for the nonlinear holiday effect on the price-load relationship.

**Problem this solves:** On Good Friday, the regression found a -1,847 MW load delta (bearish, -$1.48) and a -2,279 MW renewable delta (bullish, +$1.82). These nearly canceled to +$0.04 net. But the actual error was +$7.36 — the linear model undersized the holiday load impact because:

1. The load sensitivity of 0.8 $/MWh per 1,000 MW assumes a normal demand curve. On a holiday, the marginal unit shifts down the stack, and the same MW of load reduction has a larger price impact.
2. The renewable offset is less meaningful on a holiday — load is already low enough that incremental renewables compress prices less.

**Changes:**

1. **Add holiday-specific sensitivities** to `regression_adjusted_forecast.py`:

```python
HOLIDAY_SENSITIVITIES: dict[str, dict[str, float]] = {
    "load_mw": {"onpeak": 1.5, "offpeak": 0.6},       # 2x normal — holiday load drops hit harder
    "renewable_mw": {"onpeak": -0.4, "offpeak": -0.3},  # halved — less price impact on low-load days
    # nuclear, outage, congestion: unchanged
}
```

2. **Detect holiday in regression pipeline and swap coefficients:**

```python
holiday_name = detect_soft_holiday(target_date)
if holiday_name:
    sensitivities = {**DEFAULT_SENSITIVITIES, **HOLIDAY_SENSITIVITIES}
```

3. **Apply a shape correction** — this is the most impactful change. The regression adjusts the level but not the hourly shape. On holidays, the shape is flatter (weaker morning ramp, suppressed midday, softer evening peak). Options:

   **Option A (simple):** Apply a scalar dampening to the on-peak hours. When a holiday is detected, multiply the forecast at HE7-8 by 0.85 and HE19-21 by 0.90 (based on historical Good Friday shape ratios vs weekday averages).

   **Option B (data-driven):** Build a lookup of historical holiday shapes as ratios to the weekday average. When a holiday is detected, blend the analog forecast shape with the historical holiday shape template:
   ```
   adjusted_shape[h] = (1 - alpha) * analog_shape[h] + alpha * holiday_template[h]
   ```
   where `alpha` is calibrated per holiday type (e.g., 0.3 for Good Friday, 0.5 for day-after-Thanksgiving).

### Tier 4: Holiday-type matching (future enhancement)

**Goal:** Match holidays to same-type holidays. Good Friday should prefer past Good Fridays; day-after-Thanksgiving should prefer past day-after-Thanksgivings.

**Changes:**

1. **Add `holiday_type` as a categorical feature** (one-hot encoded) in the feature matrix.
2. **Add a `holiday_type` feature group** to the similarity engine with significant weight (e.g., 5.0) — this makes same-holiday-type analogs much closer in distance.
3. **Only activate when the forecast date is a holiday** — on normal weekdays, the holiday_type features are all zero and don't affect distance.

This is the most powerful approach but requires enough historical data per holiday type. Good Friday has ~3-4 years of data in the pipeline (2023-2026), so the pool would be only 3-4 analogs. Combine with Tier 2 (general holiday preference) as a fallback when same-type analogs are insufficient.

## Recommended Implementation Order

| Phase | What | Files Changed | Effort | Impact |
|-------|------|---------------|--------|--------|
| **1** | Holiday calendar + auto-detection | `configs.py`, `calendar_features.py`, `forecast.py` | Small | Eliminates the "no holiday analogs" failure mode. Would have fixed the Good Friday miss by pulling in weekend/holiday-like analogs automatically. |
| **2** | `is_any_holiday` in distance computation | `calendar_features.py`, `engine.py`, `configs.py` | Small | Makes holiday analogs preferred over non-holidays in mixed pools. Complements Phase 1. |
| **3** | Holiday-specific regression sensitivities | `regression_adjusted_forecast.py` | Medium | Fixes the "bullish renewable offset cancels bearish holiday load" problem. Estimated improvement: $3-4 of the $7.36 error. |
| **4** | Shape correction layer | `forecast.py` or new `shape_adjustment.py` | Medium | Fixes the morning ramp (HE7-8: +$13-15) and evening peak (HE20-21: +$13) errors. Highest single-hour impact. |
| **5** | Holiday-type matching | `calendar_features.py`, `engine.py` | Medium | Best long-term solution. Requires 3+ years of data per holiday type to be effective. |

## DB Schema Change

The `pjm_dates_daily` table currently has `is_nerc_holiday` (binary). To support this feature cleanly, add two columns:

```sql
ALTER TABLE pjm_cleaned.pjm_dates_daily
ADD COLUMN is_soft_holiday BOOLEAN DEFAULT FALSE,
ADD COLUMN holiday_name VARCHAR(50) DEFAULT NULL;
```

Alternatively, compute soft holidays purely in Python (no DB change needed) using the `date_fn` approach in the config. This avoids a migration and keeps the holiday logic in one place. **Recommend the Python-only approach for Phase 1-3**, with a DB migration only if Phase 5 (holiday-type matching across historical data) is pursued.

## Validation Plan

1. **Backtest on known soft holidays:** Run the pipeline on the last 3 Good Fridays (2024, 2025, 2026), day-after-Thanksgivings, and Christmas Eves. Compare error with and without the holiday enhancement.
2. **Check analog pool composition:** On each holiday, verify that the enhanced pool includes at least 3-5 holiday analogs.
3. **Regression sensitivity test:** Compare the regression adjustment magnitude on holidays with normal vs holiday sensitivities.
4. **Shape comparison:** On backtested holidays, plot the hourly error profile with and without shape correction. The morning ramp (HE7-8) and evening peak (HE19-21) should improve.

## Appendix: 2026-04-03 Good Friday Error Breakdown

From the Post-DA briefing, the error decomposition showed:

- **Total on-peak error:** +$7.36 (model forecast $54.66, actual $47.30)
- **Regression adjustment:** +$0.04 (wrong direction — should have been ~-$7.36)
- **Load delta contribution:** -$1.48 (correct direction but undersized)
- **Renewable offset:** +$1.82 (incorrectly canceled the load signal)
- **Worst hours:** HE20 (+$16.83), HE7 (+$14.68), HE8 (+$13.21)
- **ICE market:** $47.23 (error -$0.07 — nearly perfect)
- **Meteologica:** $39.29 (error -$8.01 — overcorrected)

With the proposed changes:
- **Phase 1** (auto-detection + holiday pool): Would have pulled in weekend/holiday analogs, likely reducing the base forecast by $3-5 on-peak.
- **Phase 3** (holiday sensitivities): Would have increased the load adjustment from -$1.48 to approximately -$2.77, and reduced the renewable offset from +$1.82 to +$0.91, netting ~-$1.86 vs +$0.04. Combined with Phase 1, total error would drop to ~$2-3.
- **Phase 4** (shape correction): Would have specifically addressed the HE7-8 and HE20-21 errors.
