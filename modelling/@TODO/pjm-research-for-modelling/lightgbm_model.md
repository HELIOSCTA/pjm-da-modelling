# LightGBM Quantile Regression -- Research & Improvement Plan

## Problem Statement

The LightGBM QR model (Western Hub DA LMP) is biased to the downside, particularly on high-outage and weekday transition days. Root causes identified on 2026-04-13 forecasting 2026-04-14:

1. **P50 as point forecast on a right-skewed distribution** -- DA LMP is bounded below (~$0) and unbounded above. The conditional median is structurally below the conditional mean. On 2026-04-14, P50 OnPeak = $50.56 vs. ICE DA market = $83.00.
2. **Even P90 is below market** -- P90 OnPeak = $74.14, still $9 below the $83 market clearing level. The model's entire conditional distribution is mis-located on stress days. This is not just a point-estimate problem.
3. **Reference-date LMP anchoring across day types** -- Top features by importance include `lmp_offpeak_avg` (#2), `dart_spread_daily` (#3), and `lmp_daily_min` (#4). When forecasting Monday from a Sunday reference, these anchor to a low-price weekend regime.
4. **Missing nonlinear scarcity interactions** -- `reserve_margin_pct` is the #1 feature but captures supply-demand balance linearly. The multiplicative effect of high outages (57.5 GW) x moderate load is not modeled. Outages and load are seen as independent predictors.
5. **Flat 730-day training window** -- All samples weighted equally. Recent shoulder-season price dynamics are diluted by 2 years of unrelated regimes.
6. **Silent NaN to 0.0 imputation at inference** -- `build_X()` in `utils.py` replaces missing features with 0.0. If `tgt_load_daily_avg` (~30,000 MW) or `gas_m3_daily_avg` (~$2-4) is NaN, the model sees 0.0, dragging forecasts sharply down.

### Changes Already Implemented (2026-04-13)

- **Point forecast**: Replaced P50 with trapezoidal integration of quantile function (EV estimate) with right-tail linear extrapolation. See `utils.py:expected_value_from_quantiles()`.
- **Quantile set**: Expanded from `[P10, P25, P50, P75, P90]` to `[P01, P05, P10, P25, P50, P75, P90, P95, P99]` for better tail resolution. Config in `configs.py`, labels updated in both pipeline files and views.
- **Right-tail extrapolation**: Instead of capping at the highest quantile, the EV integrator linearly extrapolates from the last two quantiles into `[tau_k, 1.0]`.

---

## Tier 1: Quick Wins (hours)

### 1. Asinh Variance-Stabilizing Transformation on Target

Apply `np.arcsinh(y)` before training, `np.sinh(y_pred)` after prediction.

**Why it works**: When LightGBM minimizes pinball loss on raw prices, it under-allocates tree splits to the sparse high-price regime. The 90% of normal-price days dominate gradient updates, pushing all quantiles toward the mode. Asinh compresses tails so the optimizer can find genuinely different split points for each quantile. **Up to 17.7% MAE reduction** reported for electricity price models.

**Implementation**: In `trainer.py`, transform `y_train = np.arcsinh(y_train)` before `model.fit()`. In `forecast.py`, apply `np.sinh(model.predict(X))` after prediction. Asinh handles negative prices and requires no parameter tuning in its basic form.

**Parametrized variant** (better for volatile markets):
```python
def asinh_transform(p, c=0.5):
    offset = np.sqrt(1/c**2 - 1)
    return np.sign(p) * (np.arcsinh(np.abs(p) + offset) - np.arcsinh(offset))
```

Use **rolling selection** (AVG_roll from VST paper): maintain a 56-day evaluation window and pick the best-performing transform each day. Can also average predictions from 2-3 different transforms.

**Literature**:
- Uniejewski, Weron & Ziel (2018), "Variance Stabilizing Transformations for Electricity Spot Price Forecasting", *IEEE Trans. Power Systems* -- [link](https://ieeexplore.ieee.org/document/7997921/)
- Chec, Uniejewski & Weron (2025), "Variance Stabilizing Transformations for Electricity Price Forecasting in Periods of Increased Volatility" -- [arxiv](https://arxiv.org/abs/2511.13603)

**Reference impl**: [epftoolbox](https://github.com/jeslago/epftoolbox) -- asinh-median VST in LEAR model

---

### 2. Exponential Recency Weighting + Spike Emphasis

LightGBM's `model.fit()` supports per-observation `sample_weight`. Combine temporal decay with spike upweighting:

```python
def compute_sample_weights(dates, y_train, reference_date, half_life_days=180):
    days_ago = np.array([(reference_date - d).days for d in dates])
    temporal_w = np.exp(-np.log(2) * days_ago / half_life_days)
    spike_w = np.where(y_train > np.percentile(y_train, 90), 3.0, 1.0)
    return temporal_w * spike_w
```

**Why it works**: Recent market structure (current gas prices, outage patterns) dominates over stale data. Spike days get 3x gradient contribution, directly countering the downside bias where the 90% of normal days overwhelm the tail signal.

**Tuning**: Start with `half_life_days=180`. More aggressive 90-day half-life for shoulder seasons. Spike threshold at 85th-90th percentile of training target.

**Literature**:
- Practical guide: [Upweighting Recent Observations](https://jackbakerds.com/posts/upweight-recent-observations-regression-classification/)

---

### 3. Asymmetric Custom Objective Function

Replace LightGBM's built-in `objective="quantile"` with a custom loss that penalizes underestimation of high prices more heavily:

```python
def asymmetric_quantile_loss(alpha, spike_penalty=2.0, spike_threshold_pctile=85):
    def objective(y_true, y_pred):
        residual = y_true - y_pred
        grad = np.where(residual > 0, -alpha, -(alpha - 1))
        hess = np.ones_like(y_true)
        threshold = np.percentile(y_true, spike_threshold_pctile)
        spike_mask = (y_true > threshold) & (residual > 0)
        grad[spike_mask] *= spike_penalty
        return grad, hess
    return objective
```

Directly implementable via `lgb.LGBMRegressor(objective=asymmetric_quantile_loss(alpha=q))`.

**Literature**:
- Nima Sarang (2024), "Custom Loss Functions for LightGBM" -- [link](https://nimasarang.com/blog/2024-08-11-gbt-custom-loss/)
- Maciejowska et al. (2024), "Novel Custom Loss Functions for Reinforced Forecasting of High and Low Day-Ahead Electricity Prices", *Energies* -- [link](https://www.mdpi.com/1996-1073/17/19/4885)

---

### 4. Early Stopping (currently missing)

The trainer builds all `n_estimators` trees with no validation check. Add early stopping with a time-ordered validation split:

```python
model.fit(
    X_train, y_train,
    eval_set=[(X_val, y_val)],
    callbacks=[lgb.early_stopping(50), lgb.log_evaluation(100)],
)
```

Split the last 90 days of the training window as validation. Prevents overfitting and often improves quantile calibration.

---

### 5. Hyperparameter Grid Expansion

Current grid in `_select_hyperparams_cv()` is missing critical LightGBM parameters:

```python
# Current (too narrow)
depth_grid = [4, 6, 8]
n_estimators_grid = [300, 500, 800]
learning_rate_grid = [0.03, 0.05, 0.1]

# Recommended
param_grid = {
    "max_depth": [4, 6, 8],
    "n_estimators": [300, 500, 800, 1200],
    "learning_rate": [0.01, 0.03, 0.05, 0.1],
    "num_leaves": [15, 31, 63],          # MISSING -- most important LGBM param
    "reg_alpha": [0.0, 0.1, 1.0],        # MISSING -- L1 regularization
    "reg_lambda": [0.0, 0.1, 1.0, 10.0], # MISSING -- L2 regularization
    "min_child_samples": [10, 20, 50],    # Currently fixed at 20
}
```

`num_leaves` is the **most important** LightGBM parameter -- it controls complexity directly since LightGBM grows leaf-wise, not level-wise. Currently defaults to 31 (unset).

Consider switching from grid search to **Optuna** for Bayesian optimization -- explores the space more efficiently with the same compute budget.

---

## Tier 2: Post-Hoc Calibration (no retraining required)

### 6. Conformalized Quantile Regression (CQR)

Wraps existing quantile predictions and adjusts them to achieve guaranteed coverage using a calibration set. **No retraining required.**

```python
def cqr_calibrate(y_cal, q_lower_cal, q_upper_cal, alpha=0.1):
    scores = np.maximum(q_lower_cal - y_cal, y_cal - q_upper_cal)
    n = len(scores)
    q_level = np.ceil((1 - alpha) * (n + 1)) / n
    correction = np.quantile(scores, min(q_level, 1.0))
    return correction  # $/MWh to widen intervals by
```

Apply separately to each symmetric pair (P05/P95, P10/P90, P25/P75) using a rolling 182-day calibration window. The correction term tells you exactly how many $/MWh your intervals need to widen.

**Online adaptive variant** (handles regime changes): Tracks coverage in real-time using PID control:
```python
q_hat = initial_quantile
for t in range(T):
    score_t = compute_conformity_score(y_t, prediction_t)
    q_hat += learning_rate * ((1 - alpha) if score_t > q_hat else -alpha)
```

**Libraries**:
- [MAPIE](https://github.com/scikit-learn-contrib/MAPIE) (`pip install mapie`) -- `MapieQuantileRegressor` for CQR
- [conformal-time-series](https://github.com/aangelopoulos/conformal-time-series) -- PID-based adaptive conformal for non-exchangeable time series

**Literature**:
- Romano, Patterson & Candes (2019), "Conformalized Quantile Regression", *NeurIPS* -- [arxiv](https://arxiv.org/abs/1905.03222)
- Kath & Ziel (2024), "On-line conformalized neural networks ensembles for probabilistic forecasting of day-ahead electricity prices" -- [arxiv](https://arxiv.org/abs/2404.02722)
- Conformal Prediction for EPF (2025) -- [link](https://www.sciencedirect.com/science/article/pii/S266654682500103X)

---

### 7. Quantile Regression Averaging (QRA / iQRA)

Use existing quantile predictions as inputs to a second-stage linear quantile regression that recalibrates the distribution:

```python
from sklearn.linear_model import QuantileRegressor

# X_qra = [lgbm_P50, lgbm_P10, lgbm_P90, ...] on calibration set
for tau in [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]:
    qr = QuantileRegressor(quantile=tau, alpha=0.01)
    qr.fit(X_qra_cal, y_qra_cal)
    adjusted_quantile = qr.predict(X_qra_test)
```

The QRA layer learns that P90 is systematically too low and adjusts the mapping.

**Isotonic QRA (iQRA)** is the newest and best-performing variant -- enforces stochastic order constraints (non-negative coefficients). No hyperparameter tuning, 30x faster than LQRA.

**Library**: [ReModels](https://github.com/zakrzewow/remodels) (`pip install remodels`) -- implements QRA, LQRA, SQRA, FQRA, iQRA specifically for electricity price forecasting

**Literature**:
- Lipiecki & Weron (2025), "Isotonic Quantile Regression Averaging" -- [arxiv](https://arxiv.org/abs/2507.15079)
- Uniejewski (2023), "Smoothing Quantile Regression Averaging" -- [arxiv](https://arxiv.org/abs/2302.00411)

---

### 8. Adaptive Conformal Inference (AgACI) for Regime Changes

Combines conformal prediction with online aggregation of experts, specifically designed for electricity prices during regime changes:

1. Maintain K experts with different adaptation rates
2. Each expert runs ACI with proportional coverage tracking
3. Aggregate using Bernstein Online Aggregation with independent upper/lower bounds
4. Upper bounds favor short windows (faster spike adaptation), lower bounds favor longer windows (stability)

**Literature**:
- Elie et al. (2024), "Adaptive probabilistic forecasting of French electricity spot prices" -- [arxiv](https://arxiv.org/abs/2405.15359)

---

## Tier 3: Feature Engineering

### 9. Missing High-Impact Features

| Feature | Formula / Source | Why |
|---------|-----------------|-----|
| `outage_x_load` | `tgt_outage_total_mw * tgt_load_daily_peak` | Captures nonlinear scarcity -- outages matter MORE when load is high. This is likely the single biggest missing signal. On 2026-04-14: 57.5 GW outages x elevated Monday load |
| `weekend_to_weekday_flag` | `1 if ref is Sat/Sun and target is Mon` | Directly addresses Sunday-to-Monday anchoring problem |
| Friday LMP as Monday anchor | For Monday forecasts, use `lmp_lag3_*` (Friday) instead of `lmp_lag1_*` (Sunday) | Friday better represents weekday price regime |
| `outage_acceleration` | `tgt_outage_total_mw - outage_total_mw` | Whether supply is tightening or loosening vs. yesterday |
| `gas_price_momentum` | `gas_m3_daily - gas_m3_7d_rolling_avg` | Captures trending gas markets affecting marginal cost |
| `net_load_ramp_HE{h}` | `tgt_load_HE{h} - tgt_load_HE{h-1} - delta_renewable_HE{h}` | Steep ramps trigger expensive peakers; current daily aggregates miss this |
| `temp_deviation_from_normal` | `tgt_temp - seasonal_normal_temp` | Extreme cold/heat events, not just absolute temperature |
| `forced_outage_ratio` | `tgt_outage_forced_mw / tgt_outage_total_mw` | High forced-to-total ratio signals unexpected stress (not scheduled maintenance) |
| ICE forward price signal | DA peak/offpeak forward price from ICE | Forward market consensus; anchors forecast to market expectations |
| `monday_premium` | Rolling 8-week `avg_monday_price / avg_sunday_price` | Explicit regime-shift signal for weekday transitions |

### 10. Sunday-to-Monday Reference Fix

The `DAY_TYPE_LGBM_PROFILES` in `configs.py` has weekday/saturday/sunday profiles. Add a **Monday** profile:

```python
DAY_TYPE_MONDAY = "monday"

DAY_TYPE_LGBM_PROFILES = {
    ...
    DAY_TYPE_MONDAY: {
        "include_lagged_lmp": True,  # re-enable with Friday lag
    },
}
```

For Monday forecasts, use Friday's LMP as the reference anchor (lag=3):
```python
if target_date.weekday() == 0:  # Monday
    reference_lag = 3  # Use Friday
else:
    reference_lag = 1
```

---

### 11. Fix NaN-to-Zero Imputation

Replace the silent `0.0` default in `build_X()` with training-set medians:

```python
# In build_X(), replace:
#   X[0, i] = val if pd.notna(val) else 0.0
# With:
#   X[0, i] = val if pd.notna(val) else feature_medians[col]
```

Alternatively, raise an error when critical features (`tgt_load_daily_avg`, `gas_m3_daily_avg`, `tgt_outage_total_mw`) are NaN so the data gap is caught rather than silently producing a bad forecast.

---

## Tier 4: Architecture Changes (days)

### 12. Two-Stage Spike Model

**Stage 1**: Train a LightGBM binary classifier to predict `P(spike)` per hour, where spike = price > 85th percentile. Use oversampling (SMOTE) or `is_unbalance=True` to fix class imbalance.

**Stage 2**: Feed `spike_prob` as a feature to the quantile model:
```python
spike_model = lgb.LGBMClassifier(objective="binary", is_unbalance=True)
spike_model.fit(X_train, (y_train > np.percentile(y_train, 85)).astype(int))
df[f"spike_prob_HE{h}"] = spike_model.predict_proba(X)[:, 1]
```

This gives the high-price regime its own gradient signal in the quantile model.

**Literature**:
- "An effective Two-Stage Electricity Price forecasting scheme" -- [link](https://www.sciencedirect.com/science/article/abs/pii/S0378779621003977)

---

### 13. Multi-Window Ensemble + Bernstein Online Aggregation (BOA)

Train separate model sets on 365d, 730d, 1095d windows. Combine predictions using BOA, which dynamically assigns weights based on recent performance.

**Why it works**: No single training window is optimal across all market conditions. Short windows capture current regime but lack data; long windows are stable but dilute recent dynamics. BOA achieves 4-11% MAE improvement over individual models.

**Implementation**: Your existing per-(hour, quantile) architecture makes this straightforward. Train three sets, average at forecast time. Start with simple average, move to performance-weighted.

**Literature**:
- Marcjasz, Serafin & Weron (2018), "Selection of Calibration Windows for Day-Ahead Electricity Price Forecasting" -- [link](https://www.mdpi.com/1996-1073/11/9/2364)
- Marcjasz et al. (2025), "Electricity Price Forecasting: Bridging Linear Models, Neural Networks" -- [arxiv](https://arxiv.org/abs/2601.02856)

---

### 14. Residual Boosting Hybrid (LASSO QR + LightGBM)

Train LASSO QR first (captures linear gas-to-LMP relationship), then LightGBM on the residuals:

```python
# Stage 1: LASSO captures linear structure
lasso_pred = lasso_model.predict(X)

# Stage 2: LightGBM captures nonlinear residual patterns
residual = y - lasso_pred
lgbm_model.fit(X, residual)  # or X + lasso_pred as feature

# Final
final_pred = lasso_pred + lgbm_model.predict(X)
```

This is the "MLP with RLin" architecture pattern -- 14-17% MAE reductions over standalone models.

**Literature**:
- Marcjasz et al. (2025) -- [arxiv](https://arxiv.org/abs/2601.02856)

---

### 15. LTSC Decomposition (Long-Term Seasonal Component)

Decompose LMP into a long-term seasonal component + residual. Forecast each separately, recombine.

Removes the annual seasonal pattern that tree-based models struggle to extrapolate beyond the training window. The LTSC can be a 365-day rolling median or wavelet decomposition.

**Literature**:
- Marcjasz, Uniejewski & Weron (2021), "Importance of the Long-Term Seasonal Component in Day-Ahead Electricity Price Forecasting Revisited" -- [link](https://www.mdpi.com/1996-1073/14/11/3249)
- Chec, Uniejewski & Weron (2025), "Extrapolating the long-term seasonal component of electricity prices for forecasting in the day-ahead market" -- [arxiv](https://arxiv.org/abs/2503.02518)

---

## Key Reference Repositories

| Repo | What it implements | Link |
|------|--------------------|------|
| epftoolbox | LEAR + DNN benchmarks, PJM dataset, Diebold-Mariano tests, asinh VST | [github](https://github.com/jeslago/epftoolbox) |
| ReModels | QRA, LQRA, SQRA, iQRA + 8 VSTs for electricity prices | [github](https://github.com/zakrzewow/remodels) |
| MAPIE | Conformal prediction, `MapieQuantileRegressor` for CQR | [github](https://github.com/scikit-learn-contrib/MAPIE) |
| conformal-time-series | Online adaptive conformal (PID control) for non-exchangeable series | [github](https://github.com/aangelopoulos/conformal-time-series) |
| EpexPredictor | LightGBM electricity price prediction with weather features | [github](https://github.com/b3nn0/EpexPredictor) |
| skforecast | LightGBM forecasters with built-in conformal calibration | [skforecast.org](https://skforecast.org/) |
| PostForecasts.jl | QRA, LQRA, IDR, conformal prediction (Julia) | [github](https://github.com/lipiecki/PostForecasts.jl) |

---

## Implementation Priority

| Order | Change | Effort | Fixes |
|-------|--------|--------|-------|
| 1 | Asinh VST on target | ~2 hrs | Distribution compression, quantile spread |
| 2 | Sample weighting (temporal decay + spike emphasis) | ~2 hrs | Regime anchoring, spike underpricing |
| 3 | CQR post-hoc calibration via MAPIE | ~4 hrs | Coverage guarantees, no retraining |
| 4 | Missing features: `outage_x_load`, `weekend_to_weekday_flag`, Friday Monday anchor | ~4 hrs | Scarcity nonlinearity, day-type transitions |
| 5 | Hyperparameter grid: add `num_leaves`, `reg_alpha`, `reg_lambda` | ~2 hrs | Model complexity tuning |
| 6 | Early stopping | ~1 hr | Overfitting prevention |
| 7 | Fix NaN-to-zero imputation in `build_X()` | ~1 hr | Silent data gap errors |
| 8 | QRA/iQRA recalibration layer via ReModels | ~4 hrs | Meta-calibration of distribution |
| 9 | Two-stage spike model | ~1-2 days | Extreme day pricing |
| 10 | Multi-window ensemble + BOA (365d, 730d, 1095d) | ~1-2 days | Robust across regime changes |
| 11 | Residual boosting hybrid (LASSO QR + LightGBM) | ~1 day | Linear + nonlinear decomposition |
| 12 | LTSC decomposition | ~4 hrs | Seasonal baseline extraction |

---

## Observation: 2026-04-14 Case Study

**Market**: ICE DA PJM WH OnPeak trading $83.00 (Hits at $83, sweeps $83.25-$84.00). RT trading $86.75.

**Model**: OnPeak P50 = $50.56, EV estimate ~$52.70, P90 = $74.14.

**Key drivers the model underweights**:
- Total outages: 57,520 MW (31% of installed capacity). Forced outages 11,105 MW -- more than doubled from ~5,000 MW a week ago.
- Monday transition from Sunday reference: LMP anchoring features reflect $30-40 Sunday prices.
- `reserve_margin_pct` is #1 feature by gain but captures outage/load as a ratio, not their multiplicative scarcity interaction.
- Gas market conditions and ICE forward signals not captured as features.
