# DA Electricity Price Forecasting -- Model Families Beyond Like-Day

Companion to [pjm-like-day-research.md](./pjm-like-day-research.md). That note
covered the analog / similar-day / KNN family in depth. This note surveys the
rest of the methodological landscape -- statistical, regularized-linear, tree-
ensemble, neural, structural, probabilistic, and ensemble families that have
produced strong next-day price forecasts in the academic and practitioner
literature for PJM and comparable wholesale electricity markets.

Cross-links to the per-family deep-dive notes already in this folder:
[lasso_model.md](./lasso_model.md),
[lightgbm_model.md](./lightgbm_model.md),
[supply_stack_model.md](./supply_stack_model.md),
[new_models_to_implement.md](./new_models_to_implement.md). Where a family is
already covered in a sibling note, this survey gives a one- to two-paragraph
methodological framing and links out rather than duplicating the tuning content.

Citation convention: each cited paper or repo includes authors, year, title,
venue, and a hyperlink. PJM-domain evidence is preferred; EU (Nord Pool, EPEX,
Iberian) and CAISO are the main secondary evidence base.

---

## 1. Statistical / Econometric Models

The oldest and still-competitive baseline class. The Lago, Marcjasz, De Schutter,
Weron (2021) benchmark established LEAR (a parsimonious LASSO-estimated
autoregressive model) as the reference statistical model that any new method
must beat to be taken seriously in EPF.

### 1.1 AR / ARX / ARIMA / ARIMAX

The linear autoregressive family with exogenous regressors (load forecast, gas,
calendar) was the workhorse of the 2000s EPF literature and is still a strong
baseline today.

- **Conejo, Plazas, Espinola, Molina (2005). "Day-Ahead Electricity Price
  Forecasting Using the Wavelet Transform and ARIMA Models."** IEEE Trans. Power
  Systems, 20(2). [IEEE Xplore](https://ieeexplore.ieee.org/document/1425563)
  - Establishes ARIMA on log prices for mainland-Spain DA. Combines wavelet
    decomposition with ARIMA on each frequency band.
  - Why this matters for PJM Western Hub: ARIMA-on-log-price with a calendar
    dummy set is the canonical "first model" against which new approaches are
    benchmarked; we should keep one in our model zoo.

- **Misiorek, Trueck, Weron (2006). "Point and Interval Forecasting of Spot
  Electricity Prices: Linear vs. Non-Linear Time Series Models."** Studies in
  Nonlinear Dynamics & Econometrics, 10(3).
  [De Gruyter](https://www.degruyter.com/document/doi/10.2202/1558-3708.1362/html)
  - Direct comparison of AR, ARX, TARX, regime-switching, and threshold AR on
    California ISO data. Finds ARX with load and calendar dummies is hard to
    beat for point forecasts; non-linear variants help only on interval coverage.
  - Why this matters for PJM Western Hub: validates that adding load-forecast as
    an exogenous regressor to an AR model is the single highest-value step --
    the same structural insight underpins LEAR and our LASSO QR model.

- **Cuaresma, Hlouskova, Kossmeier, Obersteiner (2004). "Forecasting Electricity
  Spot-Prices Using Linear Univariate Time-Series Models."** Applied Energy,
  77(1).
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0306261903001137)
  - Compares AR, ARMA, AR-with-jumps, and unobserved-components models on EEX
    German prices. Hour-by-hour models outperform a single 24-step model.
  - Why this matters for PJM Western Hub: motivates the "24 separate models per
    hour" design pattern we already use in LASSO QR and LightGBM QR.

### 1.2 LEAR -- LASSO-Estimated AutoRegressive (modern statistical benchmark)

LEAR is the canonical EPF benchmark since 2018: a high-dimensional ARX with
LASSO selecting from ~250 lagged price/load/calendar regressors per hour.

- **Uniejewski, Nowotarski, Weron (2016). "Automated Variable Selection and
  Shrinkage for Day-Ahead Electricity Price Forecasting."** Energies, 9(8).
  [MDPI](https://www.mdpi.com/1996-1073/9/8/621)
  - Establishes that LASSO over a rich autoregressive feature set beats hand-
    crafted ARX models across NEPOOL, GEFCom, and EEX data.
  - Why this matters for PJM Western Hub: this is the methodological foundation
    of our [lasso_model.md](./lasso_model.md) implementation.

- **Lago, Marcjasz, De Schutter, Weron (2021). "Forecasting Day-Ahead Electricity
  Prices: A Review of State-of-the-Art Algorithms, Best Practices and an
  Open-Access Benchmark."** Applied Energy, 293.
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0306261921004529)
  - The most-cited recent review. Defines LEAR and DNN as the two reference
    models, releases [epftoolbox](https://github.com/jeslago/epftoolbox) with
    PJM, NordPool, EPEX-BE/FR/DE data, and the DM/GW test infrastructure.
  - Why this matters for PJM Western Hub: PJM is in the benchmark; LEAR scores
    on PJM are publicly published, giving us a direct accuracy floor.

- **Marcjasz, Uniejewski, Weron (2020). "Beating the Naive -- Combining LASSO
  with Naive Time Series Forecasting Methods."** International Journal of
  Forecasting.
  [arXiv](https://arxiv.org/abs/2007.02466)
  - Shows that combining LEAR with the EPF-naive baseline (last-week same-hour)
    beats either alone, with the naive picking up regime changes LEAR misses.

### 1.3 Regime-Switching Models

Markov-switching ARX, threshold AR, and HMM-based methods explicitly model
"normal" vs "spike" regimes. Useful when spike density is the key forecasting
question.

- **Janczura, Weron (2010). "An Empirical Comparison of Alternate Regime-
  Switching Models for Electricity Spot Prices."** Energy Economics, 32(5).
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988310000642)
  - Two- and three-regime Markov-switching models on Nord Pool, EEX, and PJM.
    Three-regime (base / drop / spike) outperforms two-regime; identification
    of the spike regime is the dominant contributor to interval-forecast skill.
  - Why this matters for PJM Western Hub: direct PJM evidence that regime-
    switching catches the spike component our linear models miss.

- **Mount, Ning, Cai (2006). "Predicting Price Spikes in Electricity Markets
  Using a Regime-Switching Model with Time-Varying Parameters."** Energy
  Economics, 28(1).
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988305000307)
  - PJM-specific evidence: a two-regime model with reserve-margin-driven
    transition probabilities materially improves spike prediction.
  - Why this matters for PJM Western Hub: PJM-domain evidence; the structural
    intuition (transition prob driven by reserve margin) is exactly what our
    desk uses to talk about scarcity pricing.

- **Karakatsani, Bunn (2008). "Forecasting Electricity Prices: The Impact of
  Fundamentals and Time-Varying Coefficients."** International Journal of
  Forecasting, 24(4).
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0169207008000666)
  - Time-varying-coefficient regression on UK APX data; coefficients on demand
    and capacity-margin shift across regimes.

### 1.4 GARCH and Heteroscedastic Variants

Useful for interval forecasts when the level model is reasonable but residual
variance is regime-dependent.

- **Garcia, Contreras, van Akkeren, Garcia (2005). "A GARCH Forecasting Model
  to Predict Day-Ahead Electricity Prices."** IEEE Trans. Power Systems, 20(2).
  [IEEE Xplore](https://ieeexplore.ieee.org/document/1425583)
  - GARCH applied to Spanish and California prices; captures volatility
    clustering that homoscedastic AR models miss.

- **Diongue, Guegan, Vignal (2009). "Forecasting Electricity Spot Market
  Prices with a k-Factor GIGARCH Process."** Applied Energy, 86(4).
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S030626190800261X)
  - Long-memory GARCH variant for EEX prices.

### 1.5 Variance-Stabilizing Transformations (VST)

Not a model family per se but a near-mandatory preprocessing step for any of
the above on heavy-tailed price data.

- **Uniejewski, Weron, Ziel (2018). "Variance Stabilizing Transformations for
  Electricity Spot Price Forecasting."** IEEE Trans. Power Systems, 33(2).
  [IEEE Xplore](https://ieeexplore.ieee.org/document/7997921)
  - Compares Box-Cox, area-hyperbolic-sine (asinh), polynomial, and probability-
    integral-transform variance stabilizers. Asinh-mean-abs is the most robust
    across markets.
  - Why this matters for PJM Western Hub: directly applied in the asinh tier-1
    fix in our [lasso_model.md](./lasso_model.md) and
    [lightgbm_model.md](./lightgbm_model.md).

- **Chec, Uniejewski, Weron (2025). "Variance Stabilizing Transformations for
  Electricity Price Forecasting in Periods of Increased Volatility."**
  [arXiv](https://arxiv.org/abs/2511.13603)
  - Parameterized asinh variants reduce LEAR MAE up to 14.6% in volatile
    sub-periods.

GitHub repos for this section:

| Repo | Stars-rounded | Description | Relevance |
|------|---------------|-------------|-----------|
| [jeslago/epftoolbox](https://github.com/jeslago/epftoolbox) | ~250 | LEAR + DNN reference implementations, PJM benchmark data, DM/GW tests | The canonical EPF benchmark; PJM data + LEAR scores are directly comparable to our models |
| [statsmodels/statsmodels](https://github.com/statsmodels/statsmodels) | ~10k | ARIMA, ARIMAX, MarkovRegression, GARCH | Standard implementations of all models in this section |
| [pmdarima](https://github.com/alkaline-ml/pmdarima) | ~1.6k | Auto-ARIMA in scikit-learn API | Quick automated baseline |

---

## 2. Regularized Linear Models

### 2.1 LASSO ARX (LEAR) -- see [lasso_model.md](./lasso_model.md)

The dominant regularized-linear EPF model. Already implemented in this repo as
LASSO Quantile Regression with 24 per-hour models, asinh VST, and exponential
recency weighting (see [lasso_model.md](./lasso_model.md) for tuning details).
Methodological framing: a high-dimensional ARX with ~150-300 candidate
features (lagged prices, load forecast, gas, calendar) where LASSO selects
~10-30 relevant features per hour, automating the variable-selection step
that hand-crafted ARX models leave to the modeller.

### 2.2 Ridge and Elastic Net

Less common in EPF than pure LASSO because LASSO's sparsity is itself a
feature -- traders want to read the selected coefficients. Elastic Net is
useful when groups of correlated regressors (lagged prices at neighbouring
hours, multi-day lags) should all be retained.

- **Ziel, Weron (2018). "Day-Ahead Electricity Price Forecasting with High-
  Dimensional Structures: Univariate vs. Multivariate Modeling Frameworks."**
  Energy Economics, 70.
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988317304218)
  - Systematic comparison of LASSO, Ridge, and Elastic Net under univariate
    (24-models) and multivariate (one-model-with-24-outputs) frameworks across
    GEFCom, NordPool, and EPEX data. LASSO + univariate wins on average.
  - Why this matters for PJM Western Hub: confirms the design choice in our
    [lasso_model.md](./lasso_model.md) of 24 independent per-hour models.

### 2.3 Quantile Regression (linear and LASSO)

Native probabilistic output by training one regression per quantile level.
Pinball loss replaces squared loss.

- **Uniejewski, Weron (2021). "Regularized Quantile Regression Averaging for
  Probabilistic Electricity Price Forecasting."** Energy Economics, 95.
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0140988320303200)
  - LASSO-penalized quantile regression with QRA; the foundation of our QR
    model design.

### 2.4 Improvements on the LEAR Baseline

- **Marcjasz, Uniejewski, Weron (2023). "Smoothing Quantile Regression
  Averaging: A New Approach to Probabilistic Forecasting of Electricity
  Prices."**
  [arXiv](https://arxiv.org/abs/2302.00411)
  - Kernel-smoothed QRA (SQRA); 3.5% trading-profit improvement vs standard
    QRA on EPEX data.

GitHub repos:

| Repo | Stars-rounded | Description | Relevance |
|------|---------------|-------------|-----------|
| [scikit-learn](https://github.com/scikit-learn/scikit-learn) | ~60k | `Lasso`, `ElasticNet`, `QuantileRegressor` | Drop-in implementations |
| [jeslago/epftoolbox](https://github.com/jeslago/epftoolbox) | ~250 | LEAR reference | Benchmark scores on PJM |
| [statsmodels/statsmodels](https://github.com/statsmodels/statsmodels) | ~10k | `QuantReg` for linear quantile regression | Used in QRA combinators |

---

## 3. Tree Ensembles

### 3.1 Random Forest

Solid out-of-the-box baseline. Less flexible than gradient boosting but
trivially parallelizable and produces native quantile output via Quantile
Regression Forests.

- **Mei, He, Harley, Habetler, Qu (2014). "A Random Forest Method for Real-Time
  Price Forecasting in New York Electricity Market."** IEEE PES General Meeting.
  [IEEE Xplore](https://ieeexplore.ieee.org/document/6939932)
  - Early demonstration that RF beats SVM and ANN on NYISO real-time prices.

- **Meinshausen (2006). "Quantile Regression Forests."** Journal of Machine
  Learning Research, 7.
  [JMLR](https://www.jmlr.org/papers/v7/meinshausen06a.html)
  - The canonical QRF reference. Stores per-leaf empirical CDFs to produce
    quantile predictions at inference.

### 3.2 Gradient Boosting -- see [lightgbm_model.md](./lightgbm_model.md)

The strongest single-model class on most EPF benchmarks pre-2021. XGBoost,
LightGBM, and CatBoost natively support quantile loss (`objective='quantile'`,
`alpha=tau`), which is what our [lightgbm_model.md](./lightgbm_model.md)
implementation uses across nine quantile levels.

- **Hubicka, Marcjasz, Weron (2019). "A Note on Averaging Day-Ahead Electricity
  Price Forecasts Across Calibration Windows."** IEEE Trans. Power Systems,
  34(4).
  [IEEE Xplore](https://ieeexplore.ieee.org/document/8709763)
  - Shows that averaging tree-ensemble forecasts across multiple calibration
    windows (e.g., 56-day, 364-day, 728-day) consistently outperforms any
    single window. This is the literature basis for the multi-window calibration
    discussion in [lightgbm_model.md](./lightgbm_model.md).

- **Lago, Marcjasz, De Schutter, Weron (2021). "Forecasting Day-Ahead
  Electricity Prices."** Applied Energy, 293.
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0306261921004529)
  - In the PJM benchmark, the DNN slightly edges LEAR; gradient boosting was
    not the headline reference but is reproducible from the toolbox.

- **Oksuz, Ugurlu (2019). "Neural Network Based Model Comparison for
  Intraday Electricity Price Forecasting."** Energies, 12(23).
  [MDPI](https://www.mdpi.com/1996-1073/12/23/4557)
  - Compares LSTM, GRU, MLP, and gradient boosting on Turkish balancing
    market. GBM is competitive with the deep models at a fraction of the
    training cost.

### 3.3 Quantile Regression Forests for EPF

QRF is rarely top-of-class on EPF benchmarks vs gradient-boosted quantile
regression, but its honest empirical conditional CDF is useful as a
non-parametric reference for calibration diagnostics.

GitHub repos:

| Repo | Stars-rounded | Description | Relevance |
|------|---------------|-------------|-----------|
| [microsoft/LightGBM](https://github.com/microsoft/LightGBM) | ~16k | Gradient boosting with quantile loss | Backs our [lightgbm_model.md](./lightgbm_model.md) |
| [dmlc/xgboost](https://github.com/dmlc/xgboost) | ~26k | Gradient boosting; quantile loss since 2.0 | Alternative GBM backend |
| [catboost/catboost](https://github.com/catboost/catboost) | ~8k | GBM with native categorical handling | Useful when calendar features are categorical |
| [scikit-learn-contrib/scikit-garden](https://github.com/scikit-learn-contrib/scikit-garden) | ~280 | Quantile Regression Forests | Reference QRF implementation |
| [zillow/quantile-forest](https://github.com/zillow/quantile-forest) | ~250 | Modern QRF in scikit-learn API | More actively maintained than scikit-garden |

---

## 4. Neural Network Families

The Lago et al. (2021) benchmark was the inflection point: a well-tuned 4-layer
DNN slightly beats LEAR on PJM, NordPool, and EPEX-DE. The headline finding is
that the marginal accuracy gain over LEAR is small (1-3% MAE) and not always
worth the engineering cost for a single-hub forecast.

### 4.1 Feed-Forward DNNs

- **Lago, De Ridder, De Schutter (2018). "Forecasting Spot Electricity Prices:
  Deep Learning Approaches and Empirical Comparison of Traditional Algorithms."**
  Applied Energy, 221.
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0306261918302058)
  - First systematic deep-learning comparison on EPF. Establishes the 4-hidden-
    layer DNN that becomes the epftoolbox reference.

- **Lago, Marcjasz, De Schutter, Weron (2021). "Forecasting Day-Ahead
  Electricity Prices."** Applied Energy, 293.
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0306261921004529)
  - Confirms DNN as the deep-learning benchmark. PJM-domain evidence: DNN
    sMAPE on PJM is competitive with LEAR but not dominant.

### 4.2 LSTM / GRU / Seq2Seq

- **Ugurlu, Oksuz, Tas (2018). "Electricity Price Forecasting Using Recurrent
  Neural Networks."** Energies, 11(5).
  [MDPI](https://www.mdpi.com/1996-1073/11/5/1255)
  - GRU narrowly beats LSTM and MLP on Turkish day-ahead prices.

### 4.3 Temporal Fusion Transformer (TFT)

Attention-based architecture with built-in quantile output and per-feature
attention weights -- explicitly designed to be interpretable.

- **Lim, Arik, Loeff, Pfister (2021). "Temporal Fusion Transformers for
  Interpretable Multi-Horizon Time Series Forecasting."** International Journal
  of Forecasting, 37(4).
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0169207021000637)
  - The canonical TFT reference. Quantile output is native; variable-selection
    networks produce per-feature importance.

### 4.4 N-BEATS / N-HiTS

Pure deep architectures with no hand-designed time-series components; basis-
expansion blocks learn trend and seasonality directly.

- **Oreshkin, Carpov, Chapados, Bengio (2020). "N-BEATS: Neural Basis Expansion
  Analysis for Interpretable Time Series Forecasting."** ICLR.
  [arXiv](https://arxiv.org/abs/1905.10437)

- **Challu, Olivares, Oreshkin, Garza, Mergenthaler, Dubrawski (2023). "N-HiTS:
  Neural Hierarchical Interpolation for Time Series Forecasting."** AAAI.
  [arXiv](https://arxiv.org/abs/2201.12886)
  - Beats N-BEATS at lower compute; multi-rate sampling for long horizons.

### 4.5 Transformers, PatchTST, and Foundation Models

Early evidence; treat as a watch list rather than a recommended family for
production EPF today.

- **Nie, Nguyen, Sinthong, Kalagnanam (2023). "A Time Series is Worth 64 Words:
  Long-Term Forecasting with Transformers."** ICLR.
  [arXiv](https://arxiv.org/abs/2211.14730)
  - PatchTST: patch-based vanilla transformer; strong on multi-horizon
    benchmarks but no PJM-specific result.

- **Ansari, Stella, Turkmen, et al. (2024). "Chronos: Learning the Language of
  Time Series."**
  [arXiv](https://arxiv.org/abs/2403.07815)
  - Pre-trained foundation model for general time series. Zero-shot competitive
    on M-competition data; EPF performance not yet established.

- **Garza, Mergenthaler-Canseco (2023). "TimeGPT-1."**
  [arXiv](https://arxiv.org/abs/2310.03589)
  - Commercial foundation model with zero-shot forecasting. PJM evidence absent.

### 4.6 Distributional Deep Networks for EPF

- **Marcjasz, Narajewski, Weron, Ziel (2023). "Distributional Neural Networks
  for Electricity Price Forecasting."** Energy Economics, 125.
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0140988323003419)
  - Output layer parameterizes a Johnson SU or normal distribution rather than
    point or quantile. Natively probabilistic, well-calibrated, and avoids the
    quantile-crossing problem.
  - Why this matters for PJM Western Hub: most attractive deep-learning option
    today for the desk -- single model, native CRPS-optimized output, no
    post-hoc calibration needed.

GitHub repos:

| Repo | Stars-rounded | Description | Relevance |
|------|---------------|-------------|-----------|
| [Nixtla/neuralforecast](https://github.com/Nixtla/neuralforecast) | ~3k | NBEATS, NHITS, TFT, PatchTST in unified API | Single dependency for the deep family |
| [jdb78/pytorch-forecasting](https://github.com/jdb78/pytorch-forecasting) | ~4k | TFT, NBEATS, DeepAR | The canonical TFT reference impl |
| [unit8co/darts](https://github.com/unit8co/darts) | ~7k | Wide model zoo, native quantile loss | Drop-in baseline framework |
| [osllogon/epf-transformers](https://github.com/osllogon/epf-transformers) | 54 | Transformer-specific EPF | Closest to in-domain |
| [runyao-yu/PriceFM](https://github.com/runyao-yu/PriceFM) | 39 | EPF foundation model | Watch-list reference |
| [amazon-science/chronos-forecasting](https://github.com/amazon-science/chronos-forecasting) | ~3k | Chronos foundation models | Zero-shot baseline |

---

## 5. Structural / Hybrid / Supply-Stack Models

Already covered as a research draft in [supply_stack_model.md](./supply_stack_model.md).
Methodological framing only here.

A structural model treats price as the marginal cost of the marginal generator
needed to clear the residual demand. Inputs are physical: fleet capacity,
heat rates, fuel prices, outages, load, and renewables. Output is a price
estimate plus an explicit "which fuel set the price" attribution. This is the
class of models traders intuitively trust because the answer to "why is price
$100" is "because gas CTs at $95 heat rate are dispatching."

### 5.1 Bid-Stack and Supply-Curve Models

- **Coulon, Howison (2009). "Stochastic Behaviour of the Electricity Bid Stack:
  From Fundamental Drivers to Power Prices."** Journal of Energy Markets, 2(1).
  [Risk.net](https://www.risk.net/journal-of-energy-markets/2160621/stochastic-behaviour-electricity-bid-stack-fundamental-drivers-power-prices)
  - The canonical exponential-bid-stack model. Captures convexity of the
    supply curve in a small number of parameters.

- **Carmona, Coulon (2014). "A Survey of Commodity Markets and Structural
  Models for Electricity Prices."** In Quantitative Energy Finance, Springer.
  [Springer](https://link.springer.com/chapter/10.1007/978-1-4614-7248-3_2)
  - Comprehensive survey of structural EPF; recommended reading for anyone
    extending [supply_stack_model.md](./supply_stack_model.md).

- **Aid, Campi, Langrene (2013). "A Structural Risk-Neutral Model for Pricing
  and Hedging Power Derivatives."** Mathematical Finance, 23(3).
  [Wiley](https://onlinelibrary.wiley.com/doi/10.1111/j.1467-9965.2011.00507.x)
  - Multi-fuel structural model with stochastic demand and fuel; intended for
    derivatives but the dispatch logic transfers.

### 5.2 Fundamental Dispatch / Merit-Order

- **Burger, Klar, Muller, Schindlmayr (2004). "A Spot Market Model for Pricing
  Derivatives in Electricity Markets."** Quantitative Finance, 4(1).
  [Taylor & Francis](https://www.tandfonline.com/doi/abs/10.1088/1469-7688/4/1/010)
  - Early structural model with explicit demand-supply curve crossing.

### 5.3 Hybrid Structural-Statistical

The pragmatic compromise: use a structural model for the "physical price level"
and an econometric model for the residual (congestion, behavioural premia,
strategic bidding).

- **Howison, Schwarz (2012). "Risk-Neutral Pricing of Financial Instruments in
  Emission Markets: A Structural Approach."** SIAM Journal on Financial
  Mathematics.
  [SIAM](https://epubs.siam.org/doi/10.1137/100815219)
  - Structural model with a stochastic residual.

GitHub repos:

| Repo | Stars-rounded | Description | Relevance |
|------|---------------|-------------|-----------|
| [PyPSA/PyPSA](https://github.com/PyPSA/PyPSA) | ~1.5k | Power-system dispatch + capacity expansion | Reference for fleet+dispatch data structures |
| [Critical-Infrastructure-Systems-Lab/PowNet](https://github.com/Critical-Infrastructure-Systems-Lab/PowNet) | ~50 | Production-cost-style dispatch | Pattern source per [supply_stack_model.md](./supply_stack_model.md) |
| [UNSW-CEEM/nempy](https://github.com/UNSW-CEEM/nempy) | ~150 | NEM dispatch logic, transparent assumptions | Pattern source for explicit-assumption design |

---

## 6. Probabilistic-Output Methods (cross-cutting)

Probabilistic output is no longer optional in EPF. Nowotarski & Weron (2018)
established the paradigm of "maximize sharpness subject to reliability." The
methods below cut across the model families above.

### 6.1 Quantile Regression Averaging (QRA)

- **Nowotarski, Weron (2015). "Computing Electricity Spot Price Prediction
  Intervals Using Quantile Regression and Forecast Averaging."** Computational
  Statistics, 30(3).
  [Springer](https://link.springer.com/article/10.1007/s00180-014-0523-0)
  - QRA: regress the actual price at each quantile level on a vector of point
    forecasts from a pool of base models. Won the GEFCom2014 price track.
  - Why this matters for PJM Western Hub: directly applicable to combine our
    like-day, LASSO QR, LightGBM, and supply-stack point forecasts into a
    calibrated ensemble.

- **Nowotarski, Weron (2018). "Recent Advances in Electricity Price
  Forecasting: A Review of Probabilistic Forecasting."** Renewable and
  Sustainable Energy Reviews, 81(1).
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S1364032117308808)
  - The canonical probabilistic-EPF review.

### 6.2 Smoothed QRA (SQRA)

- **Marcjasz, Uniejewski, Weron (2023). "Smoothing Quantile Regression
  Averaging."**
  [arXiv](https://arxiv.org/abs/2302.00411)
  - Kernel-smoothed QRA densities; 3.5% profit lift in trading simulations
    over standard QRA.

### 6.3 Conformal Prediction

Distribution-free, model-agnostic prediction intervals with formal coverage
guarantees. Layers on top of any point or quantile forecaster.

- **Kath, Ziel (2021). "Conformal Prediction Interval Estimation, Applied to
  Electricity Price Forecasting."** International Journal of Forecasting, 37(2).
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0169207020301606)
  - First systematic application of CP to EPF. Adaptive variants needed for
    non-stationary markets.

- **Conformal Prediction for Electricity Price Forecasting in the Day-Ahead
  and Real-Time Balancing Market (2025).**
  [arXiv](https://arxiv.org/abs/2502.04935)
  - Sequential Predictive CP variants tuned for EPF; battery-trading
    simulations show financial-return improvements.

### 6.4 Distributional Regression

Output is a parametric or non-parametric distribution, not a quantile vector.
Avoids quantile-crossing and produces a single self-consistent CDF.

- **Berrisch, Ziel (2024). "Multivariate Probabilistic CRPS Learning with an
  Application to Day-Ahead Electricity Prices."** International Journal of
  Forecasting.
  [arXiv](https://arxiv.org/abs/2303.10019)
  - Distributional regression with CRPS-optimized loss; multivariate across
    24 hours.

- **Hirsch et al. (2025). "Online Distributional Regression."**
  [arXiv](https://arxiv.org/abs/2504.02518)
  - Online / streaming distributional regression for non-stationary EPF
    targets.

### 6.5 Bayesian Methods

Rare in production EPF (slower, more fragile to specification) but the
literature exists.

- **Brusaferri, Matteucci, Portolani, Vitali (2019). "Bayesian Deep Learning
  Based Method for Probabilistic Forecast of Day-Ahead Electricity Prices."**
  Applied Energy, 250.
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0306261919309559)
  - Bayesian dropout DNN for probabilistic EPF; competitive but
    computationally heavy.

- **Cottet, Smith (2003). "Bayesian Modeling and Forecasting of Intraday
  Electricity Load."** Journal of the American Statistical Association,
  98(464).
  [Taylor & Francis](https://www.tandfonline.com/doi/abs/10.1198/016214503000000774)
  - Foundational Bayesian load (not price) forecasting; methodology transfers.

GitHub repos:

| Repo | Stars-rounded | Description | Relevance |
|------|---------------|-------------|-----------|
| [scikit-learn-contrib/MAPIE](https://github.com/scikit-learn-contrib/MAPIE) | ~1.5k | Conformal prediction wrapper | Layer on top of any base model |
| [yromano/cqr](https://github.com/yromano/cqr) | ~300 | Conformalized quantile regression | Bridges QR and CP |
| [FilippoMB/Ensemble-Conformalized-Quantile-Regression](https://github.com/FilippoMB/Ensemble-Conformalized-Quantile-Regression) | ~100 | EnCQR for non-stationary series | Adaptive intervals |
| [ciaranoc123/PEPF_Conformal](https://github.com/ciaranoc123/PEPF_Conformal) | 3 | EPF-specific conformal | Domain-tuned CP |
| [statsmodels/statsmodels](https://github.com/statsmodels/statsmodels) | ~10k | `QuantReg` for QRA combinator | Standard QRA backend |

---

## 7. Hybrid and Ensemble Combinations

The most robust empirical finding in the EPF literature: a simple average of
3-4 structurally different models consistently outperforms any single model.
The like-day model is good at shape and pattern; LASSO/LightGBM are good at
level and nonlinear interactions; the supply-stack model is good at extremes.
Ensembling them covers all regimes.

### 7.1 Forecast Averaging and Committee Machines

- **Bordignon, Bunn, Lisi, Nan (2013). "Combining Day-Ahead Forecasts for
  British Electricity Prices."** Energy Economics, 35.
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988312000965)
  - Equal-weight averaging of structurally diverse models beats any single
    model on UK APX data.

- **Nowotarski, Raviv, Trueck, Weron (2014). "An Empirical Comparison of
  Alternative Schemes for Combining Electricity Spot Price Forecasts."**
  Energy Economics, 46.
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988314002461)
  - Compares equal-weight, inverse-MAE, OLS-combination, and constrained-
    least-squares. Equal-weight is hard to beat; the gains from sophisticated
    weighting are small.

- **Maciejowska, Nowotarski, Weron (2016). "Probabilistic Forecasting of
  Electricity Spot Prices Using Factor Quantile Regression Averaging."**
  International Journal of Forecasting, 32(3).
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0169207015001016)
  - Factor-QRA: PCA-reduce a large pool of base forecasts before QRA.

### 7.2 GEFCom2014 and EPF Competition Results

- **Hong, Pinson, Fan, Zareipour, Troccoli, Hyndman (2016). "Probabilistic
  Energy Forecasting: GEFCom2014 and Beyond."** International Journal of
  Forecasting, 32(3).
  [ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0169207016000133)
  - Competition retrospective. Price-track winners used QRA on top of
    diverse base models; pure single-model entries did not place.

### 7.3 Stacking and Meta-Learners

- **Smyl (2020). "A Hybrid Method of Exponential Smoothing and Recurrent
  Neural Networks for Time Series Forecasting."** International Journal of
  Forecasting, 36(1).
  [ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0169207019301153)
  - The M4 competition winner; ES-RNN combines a parametric statistical layer
    with an RNN residual learner. Methodology transfers to EPF.

GitHub repos:

| Repo | Stars-rounded | Description | Relevance |
|------|---------------|-------------|-----------|
| [Nixtla/mlforecast](https://github.com/Nixtla/mlforecast) | ~1.2k | ML forecasting with conformal + averaging | Plug-and-play ensembling |
| [statsmodels/statsmodels](https://github.com/statsmodels/statsmodels) | ~10k | `QuantReg` for QRA combinator | Standard QRA backend |

---

## 8. Comparative Benchmarks

The Lago et al. (2021) benchmark and the GEFCom2014 retrospective are the two
load-bearing references when choosing what to try next. Headline findings:

- LEAR + DNN are the two reference models. Anything new must beat both on
  the published PJM, NordPool, EPEX-BE/FR/DE data to be taken seriously.
- DNN beats LEAR by 1-3% MAE on average, with high variance across markets
  and periods. The improvement is real but small.
- Tree ensembles are competitive with both at a fraction of the engineering
  cost.
- Ensemble of LEAR + DNN + a third model (tree or analog) reliably beats
  any single member.

Comparison table -- rows are model families, columns capture the dimensions
that drive selection decisions on this desk.

| Family | Typical EPF accuracy (MAE rank) | Probabilistic-native? | Interpretability | Sample efficiency | PJM evidence |
|---|---|---|---|---|---|
| ARIMA / ARX | Mid (baseline) | No (needs resid bootstrap) | High | High | Y (epftoolbox) |
| LEAR (LASSO ARX) | Top tier | No (needs QR variant) | High (sparse coefs) | High | Y (epftoolbox, [lasso_model.md](./lasso_model.md)) |
| Regime-switching | Mid on point, top on spike interval | Mid (regime-conditional) | High (regime labels) | Mid | Y (Mount 2006 PJM) |
| GARCH variants | Mid on point, strong on interval | Yes (volatility) | Mid | Mid | Indirect |
| Random Forest | Mid-top | Yes (QRF) | Mid (feature importance) | High | Indirect |
| Gradient Boosting (LightGBM/XGBoost/CatBoost) | Top tier | Yes (quantile loss) | Mid (SHAP) | High | Y ([lightgbm_model.md](./lightgbm_model.md)) |
| Feed-forward DNN | Top tier (per Lago 2021) | With QR or DistRNN heads | Low | Low (needs many samples) | Y (Lago 2021 PJM) |
| LSTM/GRU | Top-mid | With QR head | Low | Low | Indirect |
| TFT | Top-mid | Yes (native quantile) | Mid (attention weights) | Low | Indirect |
| N-BEATS / N-HiTS | Top-mid | Add-on quantile head | Low | Low | Indirect |
| PatchTST / Transformers / Foundation models | Watch list (early) | Varies | Low | Pre-trained low; fine-tune low | N |
| Distributional NN (DDNN) | Top tier on probabilistic | Yes (native distribution) | Low | Low | Indirect (EU evidence) |
| Structural / Supply-stack | Mid on point, top on extremes | Yes via Monte Carlo on inputs | Highest | High (no training) | Direct (PJM fleet data; [supply_stack_model.md](./supply_stack_model.md)) |
| Like-day / KNN | Mid; strong on shape | Yes via empirical analogs | High (analog dates) | High | Y (this repo, [pjm-like-day-research.md](./pjm-like-day-research.md)) |
| QRA / Smoothed-QRA / Stacking ensembles | Top tier (combiner) | Yes (native quantile combo) | Mid | High | Y (GEFCom2014 winner) |
| Conformal prediction wrapper | Coverage-calibrated; not a base model | Yes (formal coverage) | High | High | Indirect |

"PJM evidence" means a paper or a sibling note in this repo that benchmarks
the family directly on PJM data. "Indirect" means the family is well-
established in EPF but the published benchmarks are EU or CAISO. "N" means
the family has no published EPF result yet.

---

## 9. Recommendations and Gaps

What this repo already covers (recap):

- Like-day / KNN -- implemented; deep dive in [pjm-like-day-research.md](./pjm-like-day-research.md).
- LASSO QR (LEAR family) -- implemented; deep dive in [lasso_model.md](./lasso_model.md).
- LightGBM QR (gradient boosting) -- implemented; deep dive in [lightgbm_model.md](./lightgbm_model.md).
- Supply-stack / structural -- research drafted in [supply_stack_model.md](./supply_stack_model.md), implementation pending.

Highest-expected-lift families NOT yet covered, ordered by recommended
priority for Western Hub DA:

1. **QRA / SQRA ensemble combiner**. Cheapest engineering, highest empirical
   payoff in EPF history. Once supply-stack lands the desk has four
   structurally diverse base models -- combining them via QRA is a 2-3 day
   build that consistently beats any single member in the literature
   (Nowotarski 2015, Hong 2016, Marcjasz 2023).
2. **Distributional Neural Network (DDNN)**. The most attractive deep-
   learning option today: single model, native CRPS-optimized output, no
   post-hoc calibration. Marcjasz, Narajewski, Weron, Ziel (2023).
3. **Regime-switching ARX**. Direct PJM evidence (Mount 2006); produces the
   cleanest answer to "are we in a spike regime?" which is itself a tradable
   signal regardless of point-forecast accuracy.
4. **Conformal prediction wrapper**. Layer on top of any existing model for
   formal coverage guarantees; cheapest interval-calibration tool in the
   literature.

Families where the literature suggests the engineering cost is NOT justified
for a single-hub forecast:

- TFT / N-BEATS / N-HiTS -- gains over LightGBM are inconsistent and small
  on EPF; high training and inference cost; interpretability lower.
- Foundation models (Chronos, TimeGPT, PriceFM) -- watch list. Useful as
  zero-shot baselines but no published advantage over LEAR/DNN on PJM data
  yet.
- Bayesian deep learning -- accuracy parity with QR-DNN at much higher
  computational cost.
- Multivariate / cross-zone joint models -- the production target is Western
  Hub single-output; cross-zone joint modelling is a strict superset of work
  with diminishing return given current scope.

Open-question commitments (per the prompt brief, defaults adopted):

- Foundation models (TimeGPT, PatchTST, Chronos, PriceFM) covered as a watch
  list at the end of section 4 rather than a recommended family.
- Hybrid structural-statistical models folded into section 5.3 rather than
  given their own section.
- Bayesian methods covered in a short subsection of section 6.5.
- Multivariate / cross-zone models flagged as a gap here (section 9) but
  not surveyed in depth -- single-hub focus.

---

## 10. References Summary

### Papers cited

1. Conejo, Plazas, Espinola, Molina (2005). [IEEE Trans. Power Systems](https://ieeexplore.ieee.org/document/1425563)
2. Misiorek, Trueck, Weron (2006). [Studies in Nonlinear Dynamics & Econometrics](https://www.degruyter.com/document/doi/10.2202/1558-3708.1362/html)
3. Cuaresma, Hlouskova, Kossmeier, Obersteiner (2004). [Applied Energy](https://www.sciencedirect.com/science/article/abs/pii/S0306261903001137)
4. Uniejewski, Nowotarski, Weron (2016). [Energies](https://www.mdpi.com/1996-1073/9/8/621)
5. Lago, Marcjasz, De Schutter, Weron (2021). [Applied Energy](https://www.sciencedirect.com/science/article/pii/S0306261921004529)
6. Marcjasz, Uniejewski, Weron (2020). [arXiv](https://arxiv.org/abs/2007.02466)
7. Janczura, Weron (2010). [Energy Economics](https://www.sciencedirect.com/science/article/abs/pii/S0140988310000642)
8. Mount, Ning, Cai (2006). [Energy Economics](https://www.sciencedirect.com/science/article/abs/pii/S0140988305000307)
9. Karakatsani, Bunn (2008). [International Journal of Forecasting](https://www.sciencedirect.com/science/article/abs/pii/S0169207008000666)
10. Garcia, Contreras, van Akkeren, Garcia (2005). [IEEE Trans. Power Systems](https://ieeexplore.ieee.org/document/1425583)
11. Diongue, Guegan, Vignal (2009). [Applied Energy](https://www.sciencedirect.com/science/article/abs/pii/S030626190800261X)
12. Uniejewski, Weron, Ziel (2018). [IEEE Trans. Power Systems](https://ieeexplore.ieee.org/document/7997921)
13. Chec, Uniejewski, Weron (2025). [arXiv](https://arxiv.org/abs/2511.13603)
14. Ziel, Weron (2018). [Energy Economics](https://www.sciencedirect.com/science/article/abs/pii/S0140988317304218)
15. Uniejewski, Weron (2021). [Energy Economics](https://www.sciencedirect.com/science/article/pii/S0140988320303200)
16. Marcjasz, Uniejewski, Weron (2023) SQRA. [arXiv](https://arxiv.org/abs/2302.00411)
17. Mei, He, Harley, Habetler, Qu (2014). [IEEE PES GM](https://ieeexplore.ieee.org/document/6939932)
18. Meinshausen (2006) QRF. [JMLR](https://www.jmlr.org/papers/v7/meinshausen06a.html)
19. Hubicka, Marcjasz, Weron (2019). [IEEE Trans. Power Systems](https://ieeexplore.ieee.org/document/8709763)
20. Oksuz, Ugurlu (2019). [Energies](https://www.mdpi.com/1996-1073/12/23/4557)
21. Lago, De Ridder, De Schutter (2018). [Applied Energy](https://www.sciencedirect.com/science/article/pii/S0306261918302058)
22. Ugurlu, Oksuz, Tas (2018). [Energies](https://www.mdpi.com/1996-1073/11/5/1255)
23. Lim, Arik, Loeff, Pfister (2021) TFT. [International Journal of Forecasting](https://www.sciencedirect.com/science/article/pii/S0169207021000637)
24. Oreshkin, Carpov, Chapados, Bengio (2020) N-BEATS. [arXiv](https://arxiv.org/abs/1905.10437)
25. Challu et al. (2023) N-HiTS. [arXiv](https://arxiv.org/abs/2201.12886)
26. Nie, Nguyen, Sinthong, Kalagnanam (2023) PatchTST. [arXiv](https://arxiv.org/abs/2211.14730)
27. Ansari et al. (2024) Chronos. [arXiv](https://arxiv.org/abs/2403.07815)
28. Garza, Mergenthaler-Canseco (2023) TimeGPT. [arXiv](https://arxiv.org/abs/2310.03589)
29. Marcjasz, Narajewski, Weron, Ziel (2023) DDNN. [Energy Economics](https://www.sciencedirect.com/science/article/pii/S0140988323003419)
30. Coulon, Howison (2009). [Risk.net](https://www.risk.net/journal-of-energy-markets/2160621/stochastic-behaviour-electricity-bid-stack-fundamental-drivers-power-prices)
31. Carmona, Coulon (2014). [Springer](https://link.springer.com/chapter/10.1007/978-1-4614-7248-3_2)
32. Aid, Campi, Langrene (2013). [Wiley](https://onlinelibrary.wiley.com/doi/10.1111/j.1467-9965.2011.00507.x)
33. Burger, Klar, Muller, Schindlmayr (2004). [Quantitative Finance](https://www.tandfonline.com/doi/abs/10.1088/1469-7688/4/1/010)
34. Howison, Schwarz (2012). [SIAM Journal on Financial Mathematics](https://epubs.siam.org/doi/10.1137/100815219)
35. Nowotarski, Weron (2015) QRA. [Computational Statistics](https://link.springer.com/article/10.1007/s00180-014-0523-0)
36. Nowotarski, Weron (2018) review. [Renewable and Sustainable Energy Reviews](https://www.sciencedirect.com/science/article/abs/pii/S1364032117308808)
37. Kath, Ziel (2021) CP for EPF. [International Journal of Forecasting](https://www.sciencedirect.com/science/article/abs/pii/S0169207020301606)
38. Conformal Prediction for EPF (2025). [arXiv](https://arxiv.org/abs/2502.04935)
39. Berrisch, Ziel (2024) Distributional CRPS. [arXiv](https://arxiv.org/abs/2303.10019)
40. Hirsch et al. (2025) Online Distributional Regression. [arXiv](https://arxiv.org/abs/2504.02518)
41. Brusaferri, Matteucci, Portolani, Vitali (2019) Bayesian DNN. [Applied Energy](https://www.sciencedirect.com/science/article/pii/S0306261919309559)
42. Cottet, Smith (2003) Bayesian load. [JASA](https://www.tandfonline.com/doi/abs/10.1198/016214503000000774)
43. Bordignon, Bunn, Lisi, Nan (2013). [Energy Economics](https://www.sciencedirect.com/science/article/abs/pii/S0140988312000965)
44. Nowotarski, Raviv, Trueck, Weron (2014). [Energy Economics](https://www.sciencedirect.com/science/article/abs/pii/S0140988314002461)
45. Maciejowska, Nowotarski, Weron (2016) Factor-QRA. [International Journal of Forecasting](https://www.sciencedirect.com/science/article/abs/pii/S0169207015001016)
46. Hong et al. (2016) GEFCom2014. [International Journal of Forecasting](https://www.sciencedirect.com/science/article/pii/S0169207016000133)
47. Smyl (2020) ES-RNN. [International Journal of Forecasting](https://www.sciencedirect.com/science/article/abs/pii/S0169207019301153)

### GitHub repositories cited

- [jeslago/epftoolbox](https://github.com/jeslago/epftoolbox) -- LEAR + DNN benchmark (PJM data)
- [statsmodels/statsmodels](https://github.com/statsmodels/statsmodels) -- ARIMA, ARIMAX, MarkovRegression, GARCH, QuantReg
- [pmdarima](https://github.com/alkaline-ml/pmdarima) -- auto-ARIMA
- [scikit-learn](https://github.com/scikit-learn/scikit-learn) -- Lasso, ElasticNet, QuantileRegressor
- [microsoft/LightGBM](https://github.com/microsoft/LightGBM) -- gradient boosting
- [dmlc/xgboost](https://github.com/dmlc/xgboost) -- gradient boosting
- [catboost/catboost](https://github.com/catboost/catboost) -- gradient boosting
- [scikit-learn-contrib/scikit-garden](https://github.com/scikit-learn-contrib/scikit-garden) -- QRF
- [zillow/quantile-forest](https://github.com/zillow/quantile-forest) -- QRF
- [Nixtla/neuralforecast](https://github.com/Nixtla/neuralforecast) -- NBEATS, NHITS, TFT, PatchTST
- [jdb78/pytorch-forecasting](https://github.com/jdb78/pytorch-forecasting) -- TFT, NBEATS, DeepAR
- [unit8co/darts](https://github.com/unit8co/darts) -- broad time-series model zoo
- [osllogon/epf-transformers](https://github.com/osllogon/epf-transformers) -- transformer EPF
- [runyao-yu/PriceFM](https://github.com/runyao-yu/PriceFM) -- EPF foundation model
- [amazon-science/chronos-forecasting](https://github.com/amazon-science/chronos-forecasting) -- foundation model
- [PyPSA/PyPSA](https://github.com/PyPSA/PyPSA) -- power-system dispatch
- [Critical-Infrastructure-Systems-Lab/PowNet](https://github.com/Critical-Infrastructure-Systems-Lab/PowNet) -- production-cost dispatch
- [UNSW-CEEM/nempy](https://github.com/UNSW-CEEM/nempy) -- NEM dispatch
- [scikit-learn-contrib/MAPIE](https://github.com/scikit-learn-contrib/MAPIE) -- conformal prediction
- [yromano/cqr](https://github.com/yromano/cqr) -- conformalized quantile regression
- [FilippoMB/Ensemble-Conformalized-Quantile-Regression](https://github.com/FilippoMB/Ensemble-Conformalized-Quantile-Regression) -- EnCQR
- [ciaranoc123/PEPF_Conformal](https://github.com/ciaranoc123/PEPF_Conformal) -- EPF-specific CP
- [Nixtla/mlforecast](https://github.com/Nixtla/mlforecast) -- ML forecasting + conformal

### Sibling research notes in this repo

- [pjm-like-day-research.md](./pjm-like-day-research.md) -- like-day / analog / KNN family
- [lasso_model.md](./lasso_model.md) -- LASSO QR (LEAR) implementation deep dive
- [lightgbm_model.md](./lightgbm_model.md) -- LightGBM QR implementation deep dive
- [supply_stack_model.md](./supply_stack_model.md) -- structural / supply-stack research
- [new_models_to_implement.md](./new_models_to_implement.md) -- comparison-model short list
