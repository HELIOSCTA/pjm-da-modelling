# New Models to Implement — DA LMP Forecasting

Comparison models for the like-day analog forecast. All should output the same
format (`Date | Type | HE1-24 | OnPeak | OffPeak | Flat` with quantile bands)
and be served via the API alongside the like-day and LASSO QR endpoints.

---

## 1. LASSO Quantile Regression (ARX-LASSO) — IMPLEMENTED

**Status:** Implemented in `backend/src/lasso_quantile_regression/`

**Literature:**
- Uniejewski, Nowotarski & Weron (2016) — *Automated variable selection and shrinkage for day-ahead electricity price forecasting*
- Ziel & Weron (2018) — *Day-ahead electricity price forecasting with high-dimensional structures*

**Why:** Direct regression on forward fundamentals (load forecast, outages, gas).
Extrapolates along a learned curve — does not need matching analog days.
Captures the load-price relationship that the like-day model misses during
regime transitions (e.g., 105 GW April heat event).

**Key features:** 24 independent models per hour, StandardScaler + LASSO penalty,
time-series CV for alpha, interaction terms (net_load, reserve_margin, load^2,
load x gas, outage^2).

---

## 2. Gradient Boosted Trees (LightGBM)

**Priority:** HIGH — expected best single model for this problem.

**Literature:**
- Lago et al. (2021) — *Forecasting day-ahead electricity prices: A review*
- Hubicka, Marcjasz & Weron (2019) — tree-based models dominated GEFCom2014/EPF competitions

**Why:** Natively learns nonlinear interactions without specifying functional form.
Can discover that {load > 95k AND outages > 50k AND gas > 3.0} = price spike.
Feature importance output explains *why* the forecast is high.

**Implementation plan:**
- Same 24-model-per-hour structure as LASSO QR
- Same feature matrix from `build_daily_features()` + interaction terms
- Native quantile loss: `objective='quantile'`, `alpha=0.10/0.50/0.90`
- Time-series expanding-window CV for hyperparameters (n_estimators, max_depth, learning_rate, min_child_samples)
- SHAP values for per-prediction feature attribution
- `pip install lightgbm` (check conda env)

**Effort:** 2-3 days. Structure mirrors LASSO QR almost exactly — swap
`QuantileRegressor` for `lightgbm.LGBMRegressor` with quantile objective.

---

## 3. Simplified Supply Stack / Merit Order Model

**Priority:** HIGH for spike forecasting, MEDIUM overall.

**Literature:**
- Ventosa et al. (2005) — *Electricity market modeling trends*
- Coulon et al. (2014) — *Short-term electricity price modeling with a structural approach*
- Carmona & Coulon (2014) — structural models of electricity prices

**Why:** Directly models the convex supply curve. When load moves from 80 GW to
105 GW on a depleted stack (55k MW outages), the model naturally produces
$40 -> $100+ without needing training data for that regime. Most interpretable
model for traders: "price is $100 because net load is 98 GW and you're
dispatching CTs at $95 heat rate x $3.50 gas."

**Implementation plan:**
- Aggregate PJM generation fleet into cost-ordered blocks (nuclear -> coal -> CC gas -> CT gas -> oil/peaker) using EIA-860 data + heat rate estimates
- Block cost = heat_rate x fuel_price + VOM
- Subtract outage MW from available capacity (pro-rata or by fuel type if data available)
- For each hour: find marginal block where cumulative_capacity >= (load - renewables)
- Marginal cost + adder for congestion/losses = price estimate
- Uncertainty bands from Monte Carlo forced outage draws on the stack
- Need: EIA-860 fleet database, fuel-specific heat rates, VOM estimates

**Effort:** ~1 week. Model logic is simple; data assembly (fleet database) is the work.

**Data sources needed:**
- EIA-860 generator inventory (capacity, fuel type, heat rate)
- Daily fuel prices (gas already available; coal/oil may need new sources)
- Hourly load forecast (already available)
- Outage MW by fuel type (currently only aggregate — may need to estimate split)

---

## 4. Regime-Switching Model (Markov-Switching ARX)

**Priority:** MEDIUM — most useful as an ensemble component for spike detection.

**Literature:**
- Janczura & Weron (2010) — *An empirical comparison of alternate regime-switching models for electricity spot prices*
- Mount et al. (2006) — *Predicting price spikes in electricity markets*
- Karakatsani & Bunn (2008) — *Forecasting electricity prices: The impact of fundamentals and time-varying coefficients*

**Why:** Explicitly models "normal" vs "stress" price regimes. When load -> 105k
and outages -> 55k, transition probability to the spike regime -> ~1.0.
Produces the cleanest answer to "when does the model think we're in a spike
regime?" — extremely valuable for trading context.

**Implementation plan:**
- 2-3 regime model (base / elevated / spike)
- Each regime has its own linear model: `price_h = alpha_regime + beta_regime x features + epsilon_regime`
- Transition probabilities parameterized by reserve margin, load percentile, outage level
- Estimate via EM algorithm or Hamilton filter
- `statsmodels.tsa.regime_switching.MarkovRegression` handles basics
- Could also implement custom with `hmmlearn` for more control

**Effort:** 3-5 days. EM convergence and label switching are the tricky parts.

---

## 5. Neural Network (LSTM or Temporal Fusion Transformer)

**Priority:** LOW — only worth it if simpler models plateau.

**Literature:**
- Lago et al. (2018) — *Forecasting spot electricity prices: Deep learning approaches*
- Lim et al. (2021) — *Temporal Fusion Transformers for interpretable multi-horizon time series forecasting*
- Marcjasz et al. (2020) — DNNs for EPF

**Why:** End-to-end learning of temporal patterns and nonlinear interactions.
TFT provides attention-based feature importance and built-in quantile outputs.
Can learn that "load ramping 25 GW in 3 days during outage season" is a
distinct pattern.

**Implementation plan:**
- Input: rolling window of features (7-30 days history + forward forecasts)
- Output: 24-hour price vector with quantiles
- TFT via `pytorch-forecasting` or simpler LSTM via `torch`
- Needs GPU for reasonable training time
- More hyperparameter sensitivity than tree/regression models

**Effort:** 1-2 weeks. Literature shows DNNs beat linear models by 5-15% on
average, but gains are inconsistent across markets and periods.

---

## 6. Ensemble (Forecast Combination)

**Priority:** HIGH — implement after at least 2-3 individual models are running.

**Literature:**
- Nowotarski et al. (2014) — *Computing electricity spot price prediction intervals using quantile regression and forecast averaging*
- Marcjasz, Uniejewski & Weron (2020) — forecast combination for EPF

**Why:** The most robust finding in the EPF literature: simple averages of 3-4
structurally different models consistently outperform any single model.
The like-day model is good at shape/pattern but bad at level during regime
transitions. LightGBM is good at level and nonlinearity. The stack model
is good at extremes. Together they cover all regimes.

**Implementation plan:**
- Collect quantile forecasts from each model (like-day, LASSO QR, LightGBM, stack)
- Weighting methods to test:
  - Equal weight (surprisingly hard to beat)
  - Inverse-MAE weighted (recalibrated monthly on rolling 30-day window)
  - Quantile regression on model forecasts (Nowotarski approach)
- Apply quantile recalibration (isotonic regression) to combined bands
- Serve as its own endpoint: `/views/ensemble_forecast_results`

**Effort:** 2-3 days once individual models exist. The combination logic is simple;
the value is in having diverse base models.

---

## Recommended Implementation Order

| Order | Model | Effort | Expected Impact |
|-------|-------|--------|-----------------|
| 1 | LASSO QR | done | Baseline regression, uses forward fundamentals |
| 2 | LightGBM | 2-3 days | Nonlinear interactions, likely best single model |
| 3 | Supply Stack | 1 week | Spike forecasting, physical interpretability |
| 4 | Ensemble | 2-3 days | Combine 1+2+3, most robust overall |
| 5 | Regime-Switching | 3-5 days | Spike regime detection for trading context |
| 6 | TFT/LSTM | 1-2 weeks | Diminishing returns, only if others plateau |

---

## Key References

- Weron (2014) — *Electricity price forecasting: A review of the state-of-the-art* (comprehensive survey)
- Nowotarski & Weron (2018) — *Recent advances in electricity price forecasting* (updated survey)
- Hong et al. (2016) — *Probabilistic energy forecasting: GEFCom2014* (competition results, quantile format)
- Delle Monache et al. (2013) — *Probabilistic weather prediction with an analog ensemble* (AnEn framework, methodological ancestor of like-day)
- Beyer et al. (1999) — curse of dimensionality in distance metrics (relevant to like-day with 40+ feature groups)
