# LASSO Quantile Regression -- Research & Improvement Plan

## Problem Statement

The LASSO QR model (Western Hub DA LMP) is biased to the downside during extreme shoulder-season events (e.g., April heat). Root causes identified on 2026-04-13 forecasting 2026-04-14:

1. **Collapsed quantile bands** -- P10 through P90 are identical for most hours. The model cannot express uncertainty.
2. **Backward-looking LMP anchoring** -- Top 3 features by importance are all reference-day LMP statistics (`lmp_onpeak_avg`, `lmp_daily_min`, `lmp_per_load`). Forward-looking load signal is suppressed.
3. **Load features regularized away** -- `tgt_load_daily_avg` and `tgt_load_daily_peak` are not in the top 15 importances despite the load forecast being meaningfully elevated (~99 GW OnPeak vs typical April ~85-92 GW). LASSO distributes load signal across correlated derived features (`load_x_gas`, `reserve_margin_pct`) with diluted coefficients.
4. **Single 730-day flat window** -- All training samples weighted equally. January 2024 winter data has the same influence as yesterday's shoulder-season data.

---

## Tier 1: Quick Wins

### 1. Asinh Variance-Stabilizing Transformation on Target

Apply `np.arcsinh(y)` before training, `np.sinh(y_pred)` after prediction.

**Why it works**: When LMP is heavy-tailed, the pinball loss at tail quantiles (P10, P90) is dominated by the bulk of "normal" observations. The optimizer finds that a near-median prediction minimizes penalized pinball loss for all quantiles. Asinh compresses tails, letting each quantile model find genuinely different coefficient vectors.

**Implementation**: In `trainer.py`, transform `y_train = np.arcsinh(y_train)` before `pipe.fit()`. In `forecast.py`, apply `np.sinh(pipe.predict(X))` after prediction. Asinh handles negative prices gracefully and requires no parameter tuning in its basic form.

**Literature**:
- Uniejewski, Weron & Ziel (2018), "Variance Stabilizing Transformations for Electricity Spot Price Forecasting", *IEEE Trans. Power Systems* -- [link](https://ieeexplore.ieee.org/document/7997921/)
- Chec, Uniejewski & Weron (2025), "Variance Stabilizing Transformations for Electricity Price Forecasting in Periods of Increased Volatility" -- [arxiv](https://arxiv.org/abs/2511.13603) -- parameterized asinh with up to 14.6% MAE reduction for LEAR models
- Uniejewski, Marcjasz & Weron (2018), "Efficient Forecasting of Electricity Spot Prices with Expert and LASSO Models", *Energies* -- [link](https://www.mdpi.com/1996-1073/11/8/2039)

**Reference impl**: [epftoolbox](https://github.com/jeslago/epftoolbox) -- asinh-median VST in their LEAR model

---

### 2. Exponential Recency Weighting via `sample_weight`

Pass exponentially decaying weights to `QuantileRegressor.fit()` so recent observations dominate.

```python
weights = gamma ** np.arange(n_samples - 1, -1, -1)
pipe.fit(X_train, y_train, qr__sample_weight=weights)
```

**Why it works**: With a flat 730-day window, the model anchors to the full-window average load-price relationship. During an April heat event, only recent shoulder-season data is relevant. Exponential decay naturally downweights stale patterns without explicit seasonal filtering.

**Tuning**: Start with `gamma=0.997` (half-life ~231 days). More aggressive `gamma=0.99` (half-life ~69 days) may help during shoulder seasons. Tune via existing time-series CV in `_select_alpha_cv`.

**Literature**:
- Practical guide: [Upweighting Recent Observations](https://jackbakerds.com/posts/upweight-recent-observations-regression-classification/)
- sklearn `QuantileRegressor` supports `sample_weight` natively

---

### 3. Quantile-Specific Alpha (Lower Regularization for Tails)

Use a different L1 penalty per quantile level. Tail quantiles have fewer informative training points, so the same alpha that works for P50 over-regularizes P10/P90.

```python
alpha_scale = {0.10: 0.5, 0.25: 0.75, 0.50: 1.0, 0.75: 0.75, 0.90: 0.5}
effective_alpha = base_alpha * alpha_scale[q]
```

**Why it works**: At P10, only ~10% of training observations "pull" the quantile downward. The L1 penalty has an outsized effect because the gradient signal is weaker. Reducing alpha for tail quantiles allows more features to remain active, producing wider spread.

**Literature**:
- Google Research (2021), "Regularization Strategies for Quantile Regression" -- [arxiv](https://arxiv.org/abs/2102.05135)

---

## Tier 2: Moderate Effort, High Payoff

### 4. Calibration Window Averaging (Multi-Window Ensemble)

Train separate model sets on short (56, 84 day) and long (714, 728 day) windows. Average their quantile predictions.

**Why it works**: This is the most validated technique in the EPF literature. Short windows capture the current regime (April load-price dynamics) without winter contamination. Long windows provide seasonal stability. The recommended scheme is WAW(56:28:112, 714:7:728) -- six window lengths, performance-weighted averaging.

A 56-day window trained in April would contain ONLY recent shoulder-season data. Load would naturally be a dominant feature because recent LMP-load correlation in spring is strong and uncontaminated by summer/winter dynamics.

**Implementation**: Your existing per-(hour, quantile) model architecture makes this straightforward. Train three sets of models (56d, 84d, 728d), average predictions at forecast time. Can start with simple average and move to performance-weighted.

**Literature**:
- Marcjasz, Serafin & Weron (2018), "Selection of Calibration Windows for Day-Ahead Electricity Price Forecasting", *Energies* -- [link](https://www.mdpi.com/1996-1073/11/9/2364)
- Hubicka, Marcjasz & Weron (2018), "A Note on Averaging Day-Ahead Electricity Price Forecasts Across Calibration Windows" -- [link](https://ideas.repec.org/p/wuu/wpaper/hsc1803.html)
- Marcjasz, Serafin & Weron (2019), "Averaging Predictive Distributions Across Calibration Windows", *Energies* -- [link](https://www.mdpi.com/1996-1073/12/13/2561)

**Reference impl**: [epftoolbox](https://github.com/jeslago/epftoolbox) -- LEAR model with daily recalibration and multi-window support

---

### 5. Conformal Prediction Post-Processing (CQR via MAPIE)

Wrap the existing LASSO QR pipeline with Conformalized Quantile Regression for distribution-free coverage guarantees.

**Why it works**: Even if the base QR produces collapsed bands, CQR adjusts predictions using calibration residuals from a held-out set. If actuals frequently fall outside the raw P10-P90 band, CQR will widen the band to achieve the target coverage.

**Implementation**: Install `mapie`. Hold out a calibration set (e.g., last 30 days of training window). `MapieQuantileRegressor` wraps the existing sklearn pipeline directly.

**Caveat**: Requires careful time-series splitting to avoid data leakage.

**Literature**:
- Romano, Patterson & Candes (2019), "Conformalized Quantile Regression", *NeurIPS* -- [arxiv](https://arxiv.org/abs/1905.03222)
- Conformal Prediction for EPF (2025) -- [arxiv](https://arxiv.org/abs/2502.04935)
- Conformal prediction interval estimation for day-ahead and intraday markets (2021) -- [link](https://www.sciencedirect.com/science/article/abs/pii/S0169207020301473)

**Reference impl**: [MAPIE](https://github.com/scikit-learn-contrib/MAPIE) -- `MapieQuantileRegressor`, sklearn-compatible

---

### 6. LTSC Decomposition (Long-Term Seasonal Component)

Decompose LMP into a long-term seasonal component + residual. Forecast each separately, recombine.

**Why it works**: Removes the annual seasonal pattern that LASSO struggles to capture via calendar features alone (`month_sin`, `month_cos`, `day_of_year_sin/cos`). The LTSC can be a 365-day rolling median or wavelet decomposition.

**Literature**:
- Marcjasz, Uniejewski & Weron (2021), "Importance of the Long-Term Seasonal Component in Day-Ahead Electricity Price Forecasting Revisited", *Energies* -- [link](https://www.mdpi.com/1996-1073/14/11/3249)
- Chec, Uniejewski & Weron (2025), "Extrapolating the long-term seasonal component of electricity prices for forecasting in the day-ahead market" -- [arxiv](https://arxiv.org/abs/2503.02518)

---

## Tier 3: Deeper Structural Improvements

### 7. Smoothing Quantile Regression Averaging (SQRA)

Replace the non-differentiable pinball loss with a kernel-smoothed version. Produces "significantly wider" prediction intervals.

**Literature**:
- Uniejewski (2023), "Smoothing Quantile Regression Averaging", *Energy Systems* -- [arxiv](https://arxiv.org/abs/2302.00411)

**Reference impl**: [remodels](https://github.com/zakrzewow/remodels) -- all 9 QRA variants + 8 VSTs, sklearn conventions

---

### 8. Elastic Net (L1+L2) Instead of Pure LASSO

Preserves groups of correlated features (e.g., multiple load measures) instead of arbitrarily zeroing all but one. sklearn's `QuantileRegressor` only supports L1, so needs `asgl` or statsmodels.

**Reference impl**: [asgl](https://github.com/alvaromc317/asgl) -- adaptive sparse group LASSO for quantile regression

---

### 9. Isotonic Distributional Regression (IDR) Post-Processing

Converts point forecasts into well-calibrated probabilistic forecasts. Enforces monotonic CDF, prevents quantile crossing, improves reliability.

**Literature**:
- Lipiecki, Uniejewski & Weron (2024), "Postprocessing of point predictions for probabilistic forecasting of day-ahead electricity prices", *Energy Economics* -- [arxiv](https://arxiv.org/abs/2404.02270)
- Lipiecki & Weron (2025), "Isotonic Quantile Regression Averaging" -- [arxiv](https://arxiv.org/abs/2507.15079)

**Reference impl**: [PostForecasts.jl](https://github.com/lipiecki/PostForecasts.jl) (Julia)

---

### 10. Change-Point Detection for Adaptive Window Selection

Use change-point detection to identify regime boundaries, then train only on data from the current regime.

**Literature**:
- Nasiadka, Nitka & Weron (2022), "Calibration Window Selection Based on Change-Point Detection for Forecasting Electricity Prices", *ICCS* -- [arxiv](https://arxiv.org/abs/2204.00872)

---

## Key Reference Repositories

| Repo | What it implements | Link |
|------|--------------------|------|
| epftoolbox | LEAR model, asinh VST, multi-window, benchmarks (incl. PJM) | [github](https://github.com/jeslago/epftoolbox) |
| remodels | 9 QRA variants, 8 VSTs, sklearn-compatible | [github](https://github.com/zakrzewow/remodels) |
| MAPIE | Conformal prediction, `MapieQuantileRegressor` | [github](https://github.com/scikit-learn-contrib/MAPIE) |
| asgl | Adaptive sparse group LASSO for quantile regression | [github](https://github.com/alvaromc317/asgl) |
| PostForecasts.jl | QRA, LQRA, IDR, conformal prediction (Julia) | [github](https://github.com/lipiecki/PostForecasts.jl) |

---

## Implementation Priority

| Order | Change | Effort | Fixes |
|-------|--------|--------|-------|
| 1 | Asinh VST on target | ~2 hrs | Collapsed bands |
| 2 | Exponential sample_weight | ~1 hr | LMP anchoring, regime change |
| 3 | Quantile-specific alpha | ~30 min | Collapsed bands |
| 4 | Multi-window ensemble (56d + 728d) | ~4 hrs | Seasonal adaptation, all problems |
| 5 | CQR via MAPIE | ~4 hrs | Coverage guarantees |
| 6 | LTSC decomposition | ~4 hrs | Seasonal baseline |
