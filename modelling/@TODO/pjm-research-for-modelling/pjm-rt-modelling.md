# PJM Real-Time & Balancing Day Modelling Research

---

## 1. Academic Papers and Key References

### 1.1 Foundational Reviews and Benchmarks

**Weron (2014). "Electricity price forecasting: A review of the state-of-the-art with a look into the future."**
International Journal of Forecasting, 30(4), 1030-1081.
[ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0169207014001083)

- Comprehensive taxonomy of EPF methods: statistical (ARIMA, GARCH, regime-switching), computational intelligence (neural networks, SVM), and fundamental/structural approaches.
- Identified probabilistic forecasting, combining forecasts, and multivariate models as key future directions.
- The canonical reference for electricity price forecasting. Provides the intellectual framework for all subsequent work.

**Nowotarski & Weron (2018). "Recent advances in electricity price forecasting: A review of probabilistic forecasting."**
Renewable and Sustainable Energy Reviews, 81(1), 1548-1568.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S1364032117308808)

- Tutorial review of probabilistic EPF methods following the paradigm of "maximizing sharpness subject to reliability."
- Point forecasts are insufficient for risk-aware trading. Probabilistic forecasts (prediction intervals, quantile regression, distributional forecasts) are essential for position sizing and risk management.

**Lago, Marcjasz, De Schutter & Weron (2021). "Forecasting day-ahead electricity prices: A review of state-of-the-art algorithms, best practices and an open-access benchmark."**
Applied Energy, 293, 116983.
[ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0306261921004529)

- Comprehensive comparison of LEAR (LASSO-Estimated AutoRegressive) vs. DNN across multiple markets including PJM.
- LEAR is a strong, computationally cheap baseline. DNN provides marginal improvements at higher complexity. Combining forecasts from multiple methods improves accuracy.
- The LEAR model is **the standard benchmark to beat**. Any PJM RT forecasting model should be evaluated against it.

**Yu et al. (2026). "Deep Learning for Electricity Price Forecasting: A Review of Day-Ahead, Intraday, and Balancing Electricity Markets."**
arXiv:2602.10071.
[arXiv](https://arxiv.org/abs/2602.10071)

- Unified taxonomy decomposing DL models into backbone, head, and loss components across DA, intraday, and balancing markets.
- DA forecasting has evolved toward probabilistic, multi-market, and foundation-style models. Intraday research remains sparse. Balancing markets favor **price-formation-aware design** due to strong regime-switching rules (ORDC thresholds).
- Provides a roadmap for RT/balancing market forecasting. The observation that balancing markets need models that understand reserve pricing rules is directly applicable to PJM.

**A Review of Electricity Price Forecasting Models in the Day-Ahead, Intra-Day, and Balancing Markets (2025).**
Energies, 18(12), 3097.
[MDPI](https://www.mdpi.com/1996-1073/18/12/3097)

- XGBoost and LSTM are standout performers for electricity price forecasting. Hybrid models (LEAR-DNN) combining linear econometric with deep learning achieve best results.
- Key input features: weather, fuel prices, demand trends, and renewable generation forecasts.

### 1.2 Deep Learning and ML for RT LMP Forecasting

**Polson & Sokolov (2019). "Deep Learning for Energy Markets."**
Applied Stochastic Models in Business and Industry, 36(1), 5-21.
[arXiv](https://arxiv.org/abs/1808.05527)

- Combines deep LSTM with **Extreme Value Theory (EVT)** for energy price forecasting across 4,719 PJM nodes.
- DL-EVT outperforms traditional Fourier time series methods. EVT models highly volatile price spikes exceeding a predetermined threshold, addressing the heavy-tailed distribution of RT prices.
- **Directly applicable to PJM.** The EVT component is particularly relevant for RT spike modelling. The 4,719-node scope demonstrates scalability.

**Zheng & Xu (2020). "Locational marginal price forecasting in a day-ahead power market using spatiotemporal deep learning network."**
Sustainable Energy, Grids and Networks, 24, 100387.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S2352467720303374)

- CNN-based architecture treating all 21 PJM zonal prices as a 2D spatial image, applying convolution to extract spatiotemporal features.
- CNN outperformed RNN and LSTM on PJM data. **Spatial correlations between zones carry significant predictive information.** The "image-like" representation of zonal prices is an effective feature engineering approach.
- Cross-zonal price relationships contain exploitable signals -- important for congestion-driven RT price deviations.

**Jami et al. (2023). "AI Driven Near Real-time Locational Marginal Pricing Method."**
arXiv:2306.10080.
[arXiv](https://arxiv.org/abs/2306.10080)

- Compares Decision Tree, Gradient Boosting, Random Forest, and DNN for LMP prediction using demand, supply, and generator cost curves.
- ML models predict LMP **4-5 orders of magnitude faster** than traditional OPF solvers with 5-6% error rate.
- Speed advantage is critical for intraday trading decisions.

**Marcjasz, Narajewski, Weron & Ziel (2023). "Distributional neural networks for electricity price forecasting."**
Energy Economics, 125, 106843.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988323003419)

- DNN with a "probability layer" outputting parameters of Normal or Johnson's SU distribution for full distributional forecasting.
- Outperforms state-of-the-art by **7%+ in CRPS and 8% in per-transaction profits**. The distributional approach directly translates to better trading outcomes.
- Probabilistic RT price forecasts are essential for position sizing. The profit-linked evaluation is directly relevant.

**Llorente & Portela (2024). "A Transformer approach for Electricity Price Forecasting."**
arXiv:2403.16108.
[arXiv](https://arxiv.org/abs/2403.16108)

- Pure Transformer model (no recurrent components) relying solely on attention mechanisms.
- The attention layer alone is sufficient for capturing temporal patterns in electricity prices. Outperforms conventional methods without hybrid LSTM/CNN components.

**Ziel & Weron (2018). "Day-ahead electricity price forecasting with high-dimensional structures."**
Energy Economics, 70, 396-420.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988317303651)

- Autoregressive models with **200+ potential explanatory variables** using LASSO regularization.
- Both uni- and multivariate LASSO structures significantly outperform AR benchmarks. Combining forecasts from multiple LASSO models brings further improvements.

**Kapoor (2023). "Analyzing and forecasting electricity price using regime-switching models."**
Journal of Forecasting, 42(8), 2028-2045.
[Wiley](https://onlinelibrary.wiley.com/doi/full/10.1002/for.3004)

- Markov regime-switching GARCH models with multiple regimes for electricity spot prices.
- Strong evidence of nonlinear regime shifts in volatility dynamics. Regime-switching GARCH has lower AIC and superior Value-at-Risk prediction. Estimated switching probability from low to high volatility predicts spikes well when reserve margin is measured accurately.
- **Regime-switching models naturally capture the bimodal nature of RT prices (normal vs. scarcity).**

### 1.3 DA-RT Spread / Convergence Forecasting

**Sahraei-Ardakani et al. (2021). "Forecasting Nodal Price Difference Between Day-Ahead and Real-Time Electricity Markets Using LSTM and Seq2Seq Networks."**
IEEE Access, 10, 3522-3533.
[IEEE Xplore](https://ieeexplore.ieee.org/document/9641818/)

- Bidirectional LSTM and Sequence-to-Sequence architectures to forecast DA-RT nodal LMP differences in PJM. Also develops classification models to predict price difference bands/ranges.
- Both DL methods outperform ARIMA, XGBoost, SVR, and Random Walk. **Seq2Seq outperforms even Bidirectional LSTM.** Notes that little research exists on cross-market price difference forecasting.
- **Directly targets PJM DA-RT spread prediction.** The classification approach (predicting spread bands) is useful for binary INC/DEC decisions.

**Wang et al. (2024). "Deep Learning-Based Electricity Price Forecast for Virtual Bidding in Wholesale Electricity Market."**
arXiv:2412.00062.
[arXiv](https://arxiv.org/abs/2412.00062)

- Transformer-based model to predict RT-DA price spread in ERCOT. Walk-forward weekly retraining. Features include load, solar/wind generation, and temporal variables.
- Trading only at peak hours with precision >50% produces **nearly consistent profit**. Introduces evaluation paradigm linking forecast accuracy to trading profitability. Weather-driven renewable forecast features are among the most important predictors.

**Baltaoglu, Tong & Zhao (2018). "Algorithmic Bidding for Virtual Trading in Electricity Markets."**
arXiv:1802.03010.
[arXiv](https://arxiv.org/abs/1802.03010)

- Online learning algorithm for optimal virtual bidding that maximizes cumulative payoff with Sharpe ratio risk management. Tested on a decade of **NYISO and PJM data**.
- Strategy outperforms standard benchmarks and the S&P 500 over the same period. Converges toward optimal performance without prior knowledge of price distributions. **PJM presents better trading opportunities than NYISO.**

**Li, Yu & Wang (2021). "Machine Learning-Driven Virtual Bidding with Electricity Market Efficiency Analysis."**
arXiv:2104.02754.
[arXiv](https://arxiv.org/abs/2104.02754)

- RNN for LMP spread forecasting combined with constrained gradient boosting trees for **price sensitivity** (how virtual bids move the market). Portfolio optimization across PJM, ISO-NE, and CAISO.
- Strategies incorporating explicit price sensitivity outperform those ignoring it. Virtual bid portfolios achieve higher Sharpe ratios than S&P 500. CAISO shows lower efficiency (more opportunity) than PJM.
- **Price sensitivity modelling (how your bids move DA clearing prices) is critical for large virtual traders.**

**Samani, Kohansal & Mohsenian-Rad (2021). "A Data-Driven Convergence Bidding Strategy Based on Reverse Engineering of Market Participants' Performance."**
arXiv:2109.09238.
[arXiv](https://arxiv.org/abs/2109.09238)

- Uses three years of CAISO public data to reverse-engineer real-world convergence bidding strategies via feature selection and density-based clustering. Identifies three main strategy clusters.
- Discovered a common real-world strategy that **does not match any existing academic methods**. Proposed strategy increases annual net profit by over 40%.
- The reverse-engineering methodology could be applied to PJM's public virtual bidding data.

### 1.4 DART Spike Prediction

**Galarneau-Vincent, Gauthier & Godin (2023). "Foreseeing the worst: Forecasting electricity DART spikes."**
Energy Economics, 119, 106548.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988323000191)

- Statistical learning models for predicting probability of DART (DA minus RT) spread spikes in NYISO. Uses leakage-free feature sets with zone-specific logistic regressions.
- Custom-engineered features achieve strong predictive performance. Trading exercise demonstrates consistent profits from model-predicted spike probabilities.
- **DART spike framework is directly transferable to PJM.** Leakage-free, zone-specific feature engineering is methodologically rigorous.

**Forgetta, Godin & Augustyniak (2025). "Distributional forecasting of electricity DART spreads with a covariate-dependent mixture model."**
Energy Economics, 144, 108325.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988325001562)

- **Three-regime** covariate-dependent mixture model: regular DART regime (Gaussian), positive spike regime (Generalized Pareto), negative spike regime (Generalized Pareto). Covariates include load, weather, and gas price forecasts.
- Covariates in the severity component are crucial (not just frequency). Neural network quantile regression cannot improve on the mixture model.
- **The three-regime structure maps naturally to PJM trading:** normal conditions, positive spikes (go DEC), negative spikes (go INC). GPD tail modelling is important for extreme event risk management.

**Hubert, Lolas & Sircar (2026). "Trading Electrons: Predicting DART Spread Spikes in ISO Electricity Markets."**
arXiv:2601.05085.
[arXiv](https://arxiv.org/abs/2601.05085)

- Multi-zone spike prediction with unified statistical model. Structural price impact model based on DA bid stacks with closed-form solutions for optimal zonal quantities.
- ~76% of DEC trades coincide with negative spikes; ~80% correctly predict spread sign. **DEC signals exhibit stronger persistence and higher accuracy than INC signals.** Substantial heterogeneity across markets and seasons.
- State-of-the-art for DART spike trading. The asymmetric INC/DEC accuracy finding (DECs more predictable) is directly actionable.

### 1.5 Virtual Bidding Strategies

**Tang, Qin, et al. (2022). "Machine Learning Analytics for Virtual Bidding in the Electricity Market."**
Computers & Chemical Engineering.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S014206152200494X)

- Formulates optimal virtual bidding as a Markov Decision Process with **CVaR** risk constraint. Uses deep reinforcement learning. Tested on PJM data 2016-2018.
- DRL-based bidding significantly outperforms naive spread-chasing. CVaR is critical for avoiding catastrophic losses on tail events. Spatio-temporal features (cross-node correlations and lagged spreads) improve profitability.
- **Directly applicable framework for automated virtual bidding in PJM.**

**Sarfati, Hesamzadeh & Holmberg (2021). "Strategic Convergence Bidding in Nodal Electricity Markets."**
IEEE Transactions on Power Systems.
[NSF](https://par.nsf.gov/servlets/purl/10283180)

- Bi-level optimization: upper level maximizes convergence bidder profit, lower level is ISO economic dispatch. Stochastic optimization with PJM case studies.
- Strategic convergence bidders exploit systematic DA-RT gaps from unit commitment inflexibility and load forecast bias. **Optimal bid placement is highly location-dependent** -- nodes near congestion interfaces and generation pockets yield highest returns.
- **Node selection is as important as directional signal.**

**Hadsell & Shawky (2016). "Virtual Bidding and Electricity Market Design."**
The Electricity Journal.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S104061901630063X)

- Virtual bidding promotes DA-RT convergence but can increase uplift costs (make-whole payments).
- **Profitable virtual trades can trigger uplift charges that erode net returns.** Traders should monitor uplift exposure.

**Jha & Wolak (2015). "Efficiency Impact of Convergence Bidding in the California Electricity Market."**
Journal of Regulatory Economics, 48(3), 245-284.
[Springer](https://link.springer.com/article/10.1007/s11149-015-9281-3)

- Before/after study of CAISO convergence bidding. Profitability of trading strategies **decreased significantly post-implementation** as market became more efficient.
- **Alpha erodes as markets mature.** Early-mover advantage is real. Continuous model adaptation is required.

**Parsons, Colbert, Larrieu, Martin & Mastrangelo (2015). "Financial Arbitrage and Efficient Dispatch in Wholesale Electricity Markets."**
MIT CEEPR Working Paper 2015-002.
[MIT](https://www.mit.edu/~jparsons/publications/20150300_Financial_Arbitrage_and_Efficient_Dispatch.pdf)

- Virtual bidders can profit from **modelling artifacts** (differences between DA and RT algorithms) rather than genuine price convergence. In these cases, virtual bidding adds real costs to system operation.
- Some profitable trades exploit ISO modelling discrepancies (e.g., DA vs RT congestion model differences). Legitimate but may face regulatory scrutiny. Monitoring Analytics has flagged similar concerns in PJM.

**Giraldo (2025). "Welfare Impact of Virtual Trading on Wholesale Electricity Markets."**
Purdue University Dissertation.
[Purdue](https://www.purdue.edu/discoverypark/sufg/wp-content/uploads/2025/04/Juan-Giraldo-Dissertation.pdf)

- When virtual traders correctly anticipate load forecast bias, they improve market efficiency; when they amplify errors, they reduce it.
- Profitable virtual trading aligned with correcting load forecast bias is both more sustainable and less likely to attract regulatory attention.

### 1.6 Battery Storage / Flexible Asset Optimization for RT Markets

**Sioshansi, Denholm, Jenkin & Weiss (2009). "Estimating the Value of Electricity Storage in PJM: Arbitrage and Some Welfare Effects."**
Energy Economics, 31(2), 269-277.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988308001631)

- LP optimization of a price-taking storage device in PJM (2002-2007).
- Arbitrage value is highly sensitive to gas prices and peak/off-peak spread. **Value per MW degrades at scale** because the device flattens the price curve.
- Foundational PJM storage arbitrage paper. Size positions relative to nodal liquidity.

**Kim & Powell (2018). "Energy Storage Arbitrage in Real-Time Markets via Reinforcement Learning."**
arXiv:1711.03127.
[arXiv](https://arxiv.org/abs/1711.03127)

- Model-free RL for temporal arbitrage. Agent learns charge/discharge policy through repeated interaction with RT price environment.
- RL achieves near-optimal arbitrage **without requiring explicit price forecasts**. Naturally learns intraday price patterns.

**Krishnamurthy, Uckun, Zhou, Thimmapuram & Botterud (2018). "Energy Storage Arbitrage Under Day-Ahead and Real-Time Price Uncertainty."**
IEEE Transactions on Power Systems.
[OSTI](https://www.osti.gov/pages/servlets/purl/1358239)

- Stochastic formulation of storage arbitrage under DA and RT price uncertainty.
- Stochastic bidding significantly outperforms deterministic benchmarks during high-volatility periods. **The optionality value of storage (ability to wait and trade in RT after DA commitment) is substantial.**

**Cao et al. (2020). "Deep Reinforcement Learning-Based Energy Storage Arbitrage With Accurate Lithium-Ion Battery Degradation Model."**
IEEE Transactions on Smart Grid, 11(5), 4513-4521.
[IEEE Xplore](https://ieeexplore.ieee.org/document/9061038/)

- DRL with accurate battery degradation model (cycle aging, calendar aging).
- **Incorporating degradation costs reduces apparent profits by 10-30%** but produces more sustainable long-term strategy.
- Critical for physical battery traders: ignoring degradation overstates true arbitrage value.

**Risk-Aware Participation in Day-Ahead and Real-Time Balancing Markets for ESS (2024).**
Electric Power Systems Research.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0378779624006278)

- Two-stage stochastic model for ESS co-optimization across DA and RT markets with CVaR constraints.
- **Co-optimization yields 15-25% higher expected profits** than single-market participation. Risk-averse strategies sacrifice modest expected profit for large reductions in downside.

### 1.7 Congestion Forecasting for RT

**Zhao, Zheng & Litvinov (2021). "Forecasting Nodal Price Difference Between Day-Ahead and Real-Time Markets Using LSTM and Seq2Seq Networks."**
IEEE Transactions on Power Systems.
[ScholarsMine](https://scholarsmine.mst.edu/cgi/viewcontent.cgi?article=5574&context=ele_comeng_facwork)

- Bi-LSTM and Seq2Seq for DA-RT nodal LMP difference prediction.
- **Congestion-driven spread is more predictable than energy-driven spread.** Focus virtual bidding on congested nodes.

**Zhou, Tesfatsion & Liu (2011). "Short-Term Congestion Forecasting in Wholesale Power Markets."**
IEEE Transactions on Power Systems, 26(4), 2252-2262.
[Iowa State](https://faculty.sites.iastate.edu/tesfatsi/archive/tesfatsi/CongestionForecasting.ZTL.pdf)

- Combines time-series forecasting with agent-based structural models.
- Pure time-series works for near-term but fails under structural changes (outages, topology changes). **Incorporating transmission outage data and topology changes is essential.**

**Integrated Learning and Optimization for Congestion Management (2024).**
arXiv:2412.18003.
[arXiv](https://arxiv.org/abs/2412.18003)

- ILO jointly trains forecasting and dispatch decision-making. Decision-focused training minimizes costs from supply-demand imbalance and line congestion.
- ILO outperforms "predict then optimize" approaches. **Forecasts optimized for trading profit, not forecast accuracy, produce better economic outcomes.**

**PJM FTR Market Review Whitepaper (2020).**
PJM Interconnection.
[PJM](https://www.pjm.com/-/media/DotCom/library/reports-notices/special-reports/2020/ftr-market-review-whitepaper.pdf)

- FTR auction prices embed market participants' congestion forecasts. Systematic under-pricing suggests persistent congestion forecasting errors.
- **FTR auction clearing prices are a consensus congestion benchmark.** RT deviations from this consensus represent opportunities.

**Deng & Oren. "An Options Theory Method to Value Electricity Financial Transmission Rights."**
CEIC Working Paper 06-03, Carnegie Mellon.
[CMU](https://www.cmu.edu/ceic/assets/docs/publications/working-papers/ceic-06-03.pdf)

- Treats FTRs as options on congestion spreads with mean-reverting stochastic process.
- FTRs have significant optionality value beyond expected payout. **Mean-reverting congestion suggests contrarian strategies on extreme spreads.**

### 1.8 Weather-Driven RT Price Models

**Kiesel & Paraschiv (2019). "The Impact of Renewable Energy Forecast Errors on Imbalance Volumes and Electricity Spot Prices."**
Energy Policy, 134.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0301421519304057)

- Econometric analysis of how wind and solar forecast errors affect imbalance volumes and spot prices.
- **Wind forecast errors impact spot prices more than solar errors.** The relationship is asymmetric: under-forecasts of renewables cause larger price impacts than over-forecasts.
- Short positions are riskier than long positions when renewables are under-forecast.

**Ketterer (2014). "The Impact of Wind Power Generation on the Electricity Price in Germany."**
Energy Economics, 44, 270-280.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988314000875)

- GARCH model of wind generation's effect on price level and volatility.
- Wind reduces average price (merit-order effect) but **increases price volatility**. High expected wind = wider spreads and more trading opportunity, but also more risk.

**Millstein, Wiser, Mills et al. (2021). "The Cost of Day-Ahead Solar Forecasting Errors in the United States."**
Solar Energy, LBNL.
[ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0038092X21010616)

- Solar forecast error costs currently modest ($1/MWh) but increase nonlinearly with solar penetration and have localized impacts.
- Monitor solar forecast accuracy as a feature, especially in summer afternoon hours as PJM solar grows.

**Jonsson, Pinson & Madsen (2010). "On the Market Impact of Wind Energy Forecasts."**
Energy Economics, 32(2), 313-320.
[RePec](https://econpapers.repec.org/article/eeeeneeco/v_3a32_3ay_3a2010_3ai_3a2_3ap_3a313-320.htm)

- DA wind forecasts significantly affect spot price formation. The forecast with higher economic value is not necessarily the one with lower statistical error.
- **Economic value and statistical accuracy are different objectives.** A trader's price model should be trained on P&L, not RMSE. Probabilistic forecasts enable better position sizing.

**Manner, Turk & Gianfreda (2018). "The Effect of Wind and Solar Power Forecasts on Day-Ahead and Intraday Electricity Prices in Germany."**
Energy Economics, 73.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988318302512)

- Improved renewable forecasts lead to lower intraday price variations, reducing the trading opportunity set.
- **As renewable forecasting improves, RT volatility may decrease**, compressing opportunities. Seek alpha at the frontier of forecast accuracy or in structural events.

### 1.9 Operating Reserve and Scarcity Pricing

**Hogan (2013). "Electricity Scarcity Pricing Through Operating Reserves."**
Economics of Energy & Environmental Policy, 2(2), 65-86.
[Harvard](https://whogan.scholars.harvard.edu/sites/g/files/omnuum4216/files/whogan/files/hogan_ordc_110112r.pdf)

- **Seminal paper** on Operating Reserve Demand Curves. Optimal scarcity price = VOLL x P(loss of load | current reserve level). Creates smooth, convex demand curve for reserves.
- Scarcity pricing increases **exponentially** as reserves approach zero. This nonlinear relationship creates outsized returns for correctly predicting near-shortage events.

**Lavin, Murphy, Sergi & Apt (2020). "Dynamic operating reserve procurement improves scarcity pricing in PJM."**
Energy Policy, 147, 111857.
[ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0301421520305747)

- Conditions generator forced outage rates on ambient temperature to create a dynamic ORDC. **Directly PJM-focused.**
- Dynamic ORDC generated ~$17.1M in additional social welfare during a high-demand week. **Temperature-conditioned outage rates create predictable patterns** in RT scarcity pricing.

**PJM (2023). "Formation of Locational Marginal Pricing and its Components During Reserve Shortages."**
PJM Technical Paper.
[PJM](https://www.pjm.com/-/media/DotCom/markets-ops/energy/real-time/reserve-shortage-pricing-paper.pdf)

- PJM ORDC: **$850/MWh** penalty for Synchronized Reserve violations, **$300/MWh** for Primary Reserve violations. Shadow prices propagate through to energy LMPs via co-optimization.
- Essential reference for understanding the mechanical drivers of PJM RT price spikes. Five-minute shadow price data available from April 2018.

**Bajo-Buenestado (2021). "Operating reserve demand curve, scarcity pricing and intermittent generation: Lessons from ERCOT."**
Energy Policy, 149.
[ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0301421520307680)

- ORDC prices significantly negatively affected by wind generation. When wind >9 GW, ORDC price approaches zero.
- **High wind suppresses scarcity pricing; low wind + high load creates highest scarcity exposure.** Wind forecasts are a leading indicator of scarcity risk.

**Nicholson, Zarnikau et al. (2020). "Dynamic Operating Reserve Procurement Improves Scarcity Pricing in PJM."**
Energy Policy.
[ScienceDirect](https://www.sciencedirect.com/science/article/pii/S0301421520305747)

- Dynamic ORDC with temperature-dependent generator forced outage rates. Reserve procurement reforms reduce artificial scarcity while maintaining genuine signals.
- **Temperature-dependent outage rates are a predictable scarcity driver.** Model forced outage probability as a function of temperature during summer peaks and winter cold snaps.

### 1.10 Demand Response and Load Uncertainty

**Impacts of DA vs RT Market Prices on Wholesale Electricity Demand in Texas (2019).**
Energy Economics.
[ScienceDirect](https://www.sciencedirect.com/science/article/abs/pii/S0140988319301148)

- RT prices are the result of "forecast error" from a DA pricing model. One-day-ahead prediction errors range from **1.2% to 7%** across markets.
- Load forecast errors of 1-7% are the baseline driver of DA-RT spreads. Monitoring load forecast performance in real-time provides an edge.

**Zugno, Morales, Pinson & Madsen (2013). "Demand Response in a Real-Time Balancing Market Clearing With Pay-As-Bid Pricing."**
IEEE Transactions on Power Systems.
[ResearchGate](https://www.researchgate.net/publication/258629006)

- Flexible loads can deliver stable balancing support. Under tight supply, DR significantly decreases peak prices and overall volatility.
- **Growing DR participation creates a price ceiling effect** in RT markets: as prices spike, DR activates and suppresses further increases. Factor DR capacity as a damping factor on extreme prices.

### 1.11 Probabilistic RT Forecasting

**Wang, Zhao & Tong (2024). "Probabilistic Forecasting of Real-Time Electricity Market Signals via Interpretable Generative AI."**
arXiv:2403.05743.
[arXiv](https://arxiv.org/abs/2403.05743)

- WIAE-GPF (Weak Innovation AutoEncoder - Generative Probabilistic Forecasting) generates future samples of multivariate time series for LMPs, interregional price spreads, and demand-supply imbalances.
- Consistently outperforms classical and cutting-edge ML techniques. Provides interpretability through innovation representation with structural convergence guarantees.
- **State-of-the-art** probabilistic RT price forecasting. The multivariate approach (jointly forecasting prices, spreads, and imbalances) is ideal for RT trading.

**Uniejewski, Marcjasz & Weron (2019). "Understanding intraday electricity markets: Variable selection and very short-term price forecasting using LASSO."**
International Journal of Forecasting, 35(4), 1533-1547.
[Wiley](https://onlinelibrary.wiley.com/doi/abs/10.1002/for.3004)

- LASSO-based variable selection and very short-term price forecasting for intraday markets. Variable importance changes with forecast horizon.
- Provides the statistical baseline for short-term (1-6 hour ahead) price forecasting applicable to PJM's RT market.

---

## 2. Relevant GitHub Repositories

### 2.1 PJM Data Tools

**gridstatus** -- https://github.com/gridstatus/gridstatus
- Stars: ~403 | Language: Python
- **The most important data repo.** Unified Python API for electricity supply, demand, and pricing data from CAISO, SPP, ISONE, MISO, ERCOT, NYISO, and PJM. Methods like `get_fuel_mix()`, `get_load()`, `get_lmp()` with consistent interfaces.
- Direct source for PJM RT LMP, load, fuel mix data. Could supplement or replace custom PJM API code.

**pjm_dataminer** -- https://github.com/rzwink/pjm_dataminer
- Stars: ~44 | Language: Python
- Scripts wrapping PJM's DataMiner 2 API. Supports CSV/JSON/XLS output.
- Direct PJM DataMiner integration reference for building custom data pipelines.

**pyiso** -- https://github.com/WattTime/pyiso
- Stars: ~250 | Language: Python
- Python client libraries for ISO power grid data. Legacy but well-tested PJM client.

### 2.2 Electricity Price Forecasting

**epftoolbox** -- https://github.com/jeslago/epftoolbox
- Stars: ~336 | Language: Python
- **Reference benchmark** for EPF. Implements LEAR and DNN models. Ships with PJM benchmark datasets. Includes Diebold-Mariano testing, rMAE metrics.
- LEAR architecture is directly transferable to RT LMP forecasting. The evaluation framework is the academic standard.

**epf-transformers** -- https://github.com/osllogon/epf-transformers
- Stars: ~54 | Language: Python
- Transformer architecture for EPF. Handles irregular temporal patterns (spikes, ramps) better than RNNs.

**WholesaleElectrPricePredict** -- https://github.com/rpglab/WholesaleElectrPricePredict
- Stars: ~15 | Language: Jupyter Notebook
- Includes both day-ahead and **hour-ahead** price prediction. One of the few repos with an explicit hour-ahead horizon.

**ReModels** -- https://github.com/zakrzewow/remodels
- Stars: ~6 | Language: Python
- **Only open-source QRA implementation.** Quantile Regression Averaging for probabilistic EPF. Includes data acquisition, variance-stabilizing transforms, Kupiec/Christoffersen tests.

### 2.3 Time Series Forecasting Libraries

**Time-Series-Library (TSlib)** -- https://github.com/thuml/Time-Series-Library
- Stars: ~11,933 | Language: Python
- Comprehensive SOTA deep time series models: iTransformer, PatchTST, TimesNet, DLinear, FEDformer, Autoformer, Informer. From Tsinghua University.
- iTransformer and PatchTST have shown strong results on electricity benchmarks.

**Darts** -- https://github.com/unit8co/darts
- Stars: ~9,302 | Language: Python
- Comprehensive time series forecasting and anomaly detection. Unified API across ARIMA, N-BEATS, TFT, transformers. Probabilistic forecasting, backtesting, covariate support.
- **Best general-purpose library for rapid prototyping** of RT price models.

**StatsForecast** -- https://github.com/Nixtla/statsforecast
- Stars: ~4,735 | Language: Python
- Lightning-fast statistical models (AutoARIMA, ETS, Theta). Orders of magnitude faster than statsmodels.
- Fast baseline models. Speed matters when refitting hourly.

**NeuralForecast** -- https://github.com/Nixtla/neuralforecast
- Stars: ~4,028 | Language: Python
- Nixtla's neural library. N-BEATS, NHITS, TFT, PatchTST with auto HPO (Ray/Optuna). Supports exogenous regressors.
- Production-ready neural models. Exogenous regressor support is essential for weather/load/generation features.

**TimeGPT (Nixtla SDK)** -- https://github.com/Nixtla/nixtla
- Stars: ~3,819 | Language: Python
- Foundation model for time series. Pre-trained on 100B+ data points. Zero-shot forecasting. Fine-tuning supported.
- Zero-shot capability for quick baselines. Fine-tune on PJM RT prices for minimal-effort forecasts.

**PatchTST** -- https://github.com/yuqinie98/PatchTST
- Stars: ~2,503 | Language: Python
- "A Time Series is Worth 64 Words" (ICLR 2023). Channel-independent patching for transformer forecasting.
- SOTA on electricity benchmarks. Patching captures local temporal patterns for intra-hour RT dynamics.

**MLForecast** -- https://github.com/Nixtla/mlforecast
- Stars: ~1,202 | Language: Python
- Scalable ML forecasting with XGBoost, LightGBM, sklearn. Automatic lag/window feature generation.
- **XGBoost/LightGBM with engineered lag features is often the most practical RT approach.** This library automates feature engineering.

### 2.4 DA-RT Spread Trading

**EPEX-machine-learning** -- https://github.com/ekapope/EPEX-machine-learning
- Stars: ~43 | Language: Python/Jupyter
- Strategy to trade between DA and intraday markets using ML. Feature engineering for spread prediction, trading signal generation, P&L backtesting.
- **Closest open-source repo to a virtual bidding system.** Framework maps directly to DA-RT convergence bidding.

**MA-VPP-RL-bidding** -- https://github.com/jlp237/MA-VPP-RL-bidding
- Stars: ~37 | Language: Python
- Multi-agent RL for strategic bidding in the balancing market with a virtual power plant.
- Balancing market bidding optimization directly relevant to RT participation.

### 2.5 Battery Storage Optimization

**energy-py** -- https://github.com/ADGEfficiency/energy-py
- Stars: ~187 | Language: Python
- RL for energy systems. Battery storage Gymnasium environments with Stable Baselines 3.
- RL-based dispatch captures non-linear RT dynamics. Gym environments useful for backtesting.

**bess-optimizer** -- https://github.com/FlexPwr/bess-optimizer
- Stars: ~84 | Language: Python (Pyomo)
- Three Market BESS Optimization Model. Multi-market sequential optimization with realistic constraints.
- **Directly applicable to battery dispatch in DA and RT markets.**

**battery_model** -- https://github.com/gschivley/battery_model
- Stars: ~62 | Language: Jupyter
- BESS arbitrage P&L analysis for NYISO DA. Clean, well-documented. Adaptable to PJM RT.

**battery-optimisation-with-drl** -- https://github.com/RichardFindlay/battery-optimisation-with-drl
- Stars: ~22 | Language: Python (PyTorch)
- DQN, Double Dueling DQN, Noisy DQN for wholesale battery arbitrage.

### 2.6 Market Simulation

**PyPSA** -- https://github.com/PyPSA/PyPSA
- Stars: ~1,921 | Language: Python
- Full power flow, OPF, unit commitment, capacity expansion. Can simulate market clearing to understand RT price formation.

**Egret** -- https://github.com/grid-parity-exchange/Egret
- Stars: ~157 | Language: Python (Pyomo)
- SCUC and **SCED** (the algorithm ISOs run to clear RT markets). Understanding SCED formulation helps RT price forecasting.

**pomato** -- https://github.com/richard-weinhold/pomato
- Stars: ~95 | Language: Python/Julia
- Detailed nodal market modelling. Useful for understanding congestion-driven LMP decomposition.

### 2.7 Probabilistic Forecasting

**day-ahead-probabilistic-forecasting-with-quantile-regression** -- https://github.com/RichardFindlay/day-ahead-probablistic-forecasting-with-quantile-regression
- Stars: ~43 | Language: Python (Keras)
- Pinball-loss deep learning for simultaneous probabilistic forecasts of wind, solar, demand, and electricity prices.

**Energy-Schaake** -- https://github.com/FabianKaechele/Energy-Schaake
- Stars: ~16 | Language: Python
- Schaake shuffle for converting point forecasts to correlated multivariate probabilistic forecasts across hours/nodes.

**deepTCN** -- https://github.com/oneday88/deepTCN
- Stars: ~157 | Language: Python (MXNet)
- TCN architecture well-suited to RT forecasting due to efficient long-range dependency capture.

### 2.8 Feature Engineering and Production Systems

**OpenSTEF** -- https://github.com/OpenSTEF/openstef
- Stars: ~138 | Language: Python
- Automated ML pipelines for short-term energy forecasting (48h). Data quality checks, automated feature engineering, explainable forecasts.
- **Strong reference architecture for a production RT forecasting system.**

### 2.9 Summary Table -- Top Picks by Use Case

| Use Case | Top Pick | Why |
|---|---|---|
| PJM data ingestion | **gridstatus** (403★) | Unified API for all ISOs including PJM RT |
| RT price forecasting baseline | **epftoolbox** (336★) | Academic standard, PJM data, LEAR model |
| Neural RT forecasting | **NeuralForecast** (4,028★) | Production-ready, HPO, exogenous features |
| Rapid prototyping | **Darts** (9,302★) | Widest model zoo, probabilistic, easy API |
| SOTA architectures | **TSlib** (11,933★) | iTransformer, PatchTST benchmarked on electricity |
| DA-RT spread trading | **EPEX-machine-learning** (43★) | Only open-source DA-intraday trading strategy |
| BESS dispatch | **bess-optimizer** (84★) | Multi-market Pyomo optimization |
| RL battery trading | **energy-py** (187★) | Gym environments for battery arbitrage |
| Probabilistic forecasting | **ReModels** (6★) | Only open-source QRA implementation |
| Market clearing simulation | **PyPSA** (1,921★) | SCED/OPF for RT price formation understanding |
| Fast ML baselines | **MLForecast** (1,202★) | XGBoost/LightGBM with auto lag features |

---

## 3. PJM RT Market Mechanics

### 3.1 Market Structure

#### 5-Minute RT LMP Calculation and Settlement

PJM's RT market operates on **5-minute intervals**. The RT Security-Constrained Economic Dispatch (SCED) runs every 5 minutes, producing 5-minute LMPs at each pricing node.

- **Settlement**: Hourly RT LMPs are the simple arithmetic average of twelve 5-minute LMPs.
- **Components**: Energy (system marginal price) + Congestion (shadow prices of binding constraints) + Losses (marginal loss factors).
- **Deviations**: Participants are settled on deviations from DA positions at the RT LMP.

#### RT vs DA Price Formation

| Dimension | Day-Ahead | Real-Time |
|-----------|-----------|-----------|
| Timeframe | Cleared ~1:30 PM day before | Continuous 5-min dispatch |
| Engine | SCUC + SCED (co-optimized) | SCED only (units already committed) |
| Demand | Virtual + physical bids | Actual metered load |
| Supply | Offers + virtual supply | Dispatched generation |
| Volatility | Lower | Higher (weather, outages, ramps) |

DA is a financial market (cleared with virtuals); RT is a physical market (actual dispatch). RT prices are more volatile because the system operator has fewer degrees of freedom.

#### Operating Reserve Demand Curves (ORDC) and Scarcity Pricing

- Reserves priced on a demand curve, not a vertical requirement. As reserves deplete, a penalty factor increases.
- **Synchronized Reserve ORDC**: Steps up from $0 to $850/MWh at the Minimum Reserve Requirement.
- **Primary Reserve ORDC**: $300/MWh extending 190 MW beyond.
- **System-wide cap**: ~$3,700/MWh for reserve pricing penalty factors.
- When reserves drop below requirements, penalty factor adders flow into RT LMPs causing price spikes of hundreds of dollars even without load shedding.

**Actionable**: Monitor PJM's real-time reserve levels. When synchronized reserves drop below ~2,000 MW, expect significant LMP adders.

#### Extended LMP (eLMP)

Fast-start pricing allows commitment costs (start-up, no-load) of fast-start resources (start time <= 2h, min run <= 2h) to be reflected in LMPs via integer relaxation. Raises LMPs during tight conditions, reducing uplift.

#### Reserve Markets

| Product | Response Time | Eligible Resources |
|---------|--------------|-------------------|
| Regulation | Continuous (AGC) | Units on AGC, storage, DR |
| Synchronized Reserve | 10 minutes | Online units with headroom, DR |
| Non-Synchronized Reserve | 10 minutes | Offline quick-start units |
| Secondary Reserve (30-min) | 30 minutes | Broader set |

Energy and reserves are co-optimized in RT SCED.

#### Balancing Operating Reserve (BOR) Credits/Charges

- **BOR credits**: Paid to generators dispatched out of merit for reliability.
- **BOR charges**: Allocated to participants who caused the need for out-of-merit dispatch. Allocated to **deviations** from DA positions.
- **BOR is a hidden cost** of RT positions. Profitable DA-RT spread trades may lose profit to BOR charges. This is the "uplift tax."

### 3.2 PJM-Specific Trading Patterns

#### Seasonal RT Premium/Discount Patterns

| Season | Pattern |
|--------|---------|
| **Summer (Jun-Aug)** | RT premium to DA during peak hours on high-load days. Generator derates reduce supply. Highest RT volatility. |
| **Winter (Dec-Feb)** | RT premiums during cold snaps (gas supply constraints). Mild winters see persistent RT discounts. |
| **Shoulder (Apr-May, Sep-Oct)** | RT generally discounts to DA. Lower demand, abundant supply. |
| **Annual average** | RT slightly below DA (risk premium to virtuals). |

#### Common Congestion Patterns

| Constraint | Direction | When It Binds |
|-----------|-----------|---------------|
| AP South interface | West → East | High eastern load |
| COMED-to-East transfers | West → East | Dominant flow pattern |
| 5004/5005 interface | AEP → DOM | High eastern demand |
| Bedington-Black Oak (500kV) | AP South area | Summer peaks |
| Eastern interface (MAAC-East) | Into NJ, eastern PA | Summer peaks |
| PSEG zone | Local congestion | Summer, import constrained |
| BGE/PEPCO | Local congestion | Summer peaks |
| DPL South (Delmarva) | Load pocket | Summer |

Summer: west-to-east constraints dominate. Winter: unusual patterns during polar vortex. Shoulder: congestion driven by transmission outage schedules.

#### Solar Duck Curve and Wind Ramp Effects

**Solar**:
- Midday RT LMP compression (HE10-HE15) on sunny days, sometimes negative at solar-heavy nodes.
- Sharp price increases HE17-HE20 as solar ramps down. Net load peak shifting to HE18-20.
- Most impact in DOM (Dominion Virginia), growing in JCPL, PSEG, PECO.
- **Trade**: Short DA midday at solar-heavy nodes; long DA during evening ramp.

**Wind**:
- Concentrated in western PJM (COMED, AEP, ATSI, PENELEC).
- Wind forecast errors of 2,000-4,000 MW are common. Over-forecast → RT premium; under-forecast → RT discount.
- Wind peaks overnight, driving off-peak RT prices lower, sometimes negative at western nodes.
- High wind in western PJM can reduce or reverse congestion patterns.

#### Weekend vs Weekday

- Weekday: higher loads, more congestion, higher average RT.
- Weekend: loads drop 15-25%, flatter prices, fewer constraints.
- Sunday is typically lowest-load day. Sunday night → Monday morning can see very low RT.
- Weekend maintenance creates unexpected congestion.

### 3.3 Key Data Sources for RT Trading

#### PJM Data Miner II Endpoints

| Endpoint | Content | Use |
|----------|---------|-----|
| `rt_hrl_lmps` | Hourly RT LMPs | Settlement, historical analysis |
| `rt_fivemin_hrl_lmps` | 5-minute RT LMPs | Intra-hour monitoring |
| `da_hrl_lmps` | DA LMPs | DA-RT spread analysis |
| `gen_outages_by_type` | Generator outages | Supply risk |
| `load_frcstd_7_day` | 7-day load forecast | Demand outlook |
| `inst_load` | Instantaneous load (5-min) | Real-time demand |
| `very_short_load_frcst` | Very short-term load forecast | Near-term load |
| `wind_gen` | Wind forecast + actual | Renewable supply |
| `solar_gen` | Solar forecast + actual | Renewable supply |
| `sr_reserve` | Synchronized reserve | Scarcity risk |
| `reg_market_results` | Regulation market | Ancillary prices |
| `transfer_limits_and_flows` | Interface flows and limits | Congestion monitoring |

#### Weather Providers

| Provider | Strengths |
|----------|-----------|
| **DTN** | Excellent short-term (1-3 day) point forecasts; energy-specific |
| **Maxar** | Strong medium-range (6-15 day); probabilistic temp/precip/wind |
| **Commodity Weather Group** | Gas/power weather focus, seasonal outlooks |
| **ECMWF/GFS** | Raw NWP model outputs (free/cheap) for quant shops |

Key variables: temperature (HDD/CDD), dew point/humidity (summer cooling), wind speed, cloud cover/irradiance, precipitation.

#### Market Data

- **ICE**: PJM Western Hub DA and RT basis swaps (peak/off-peak), zone-to-hub basis.
- **CME/NYMEX**: PJM Western Hub DA LMP futures.
- **Nodal Exchange**: PJM nodal power contracts.
- **Wood Mackenzie / Genscape**: Real-time generation output and transmission flows (minutes ahead of official data).

---

## 4. Industry Resources

### PJM Market Monitor (Monitoring Analytics)

- **State of the Market Report**: Published annually with quarterly updates. Most comprehensive analysis of PJM market performance: price formation, uplift, FTR revenue adequacy, ancillary services.
  - URL: https://www.monitoringanalytics.com/reports/PJM_State_of_the_Market/
- **Data downloads**: Extensive historical market data tables.
- **Special reports**: Uplift, congestion, fuel cost policies.

### PJM Resources

- **PJM Learning Center** (https://www.pjm.com/training): Free e-learning on markets, operations.
- **PJM Manual 11**: Energy & Ancillary Services Market Operations -- definitive technical reference.
- **PJM Manual 28**: Operating Agreement Accounting -- settlement rules including BOR.
- **MRC/MIC materials**: Market design change presentations.
- All manuals: https://www.pjm.com/library/manuals

### Industry News and Analysis

- **RTO Insider** (https://www.rtoinsider.com): Best PJM market rule change coverage.
- **S&P Global / SNL Energy**: Market analysis and data.
- **Megawatt Daily (Platts)**: Daily power market price reporting.
- **Utility Dive** (https://www.utilitydive.com): Broader energy news with PJM coverage.

### Consultancies and Research

- **Brattle Group**: Public reports on PJM market design.
- **Customized Energy Solutions (CES)**: PJM-focused consulting and market updates.
- **Potomac Economics**: Market monitor for other RTOs, publishes comparative analysis.

---

## 5. PJM Reserve Market Reforms

### Capacity Performance (CP)

Implemented after the 2014 Polar Vortex. All capacity resources must be available year-round. Non-performance penalties: ~$3,600/MW-day during Performance Assessment Hours (PAH). Bonus payments for over-performers. Has improved winter generator availability.

### Reserve Market Redesign

- **ORDC implementation (~2019)**: Moved from vertical reserve requirements (cliff pricing) to downward-sloping demand curves.
- **30-Minute Reserves (Secondary)**: New product pricing 30-minute response capability.
- **DA Reserve Market**: Proposed/implemented to complement RT reserve market, improving DA-RT alignment.

### Fast-Start Pricing (eLMP)

Fast-start unit commitment costs incorporated into LMP via integer relaxation. Higher RT LMPs during tight conditions, reduced uplift, better price signals.

### FERC Order 2222 (DER Participation)

DER aggregations allowed in wholesale markets. Reduced minimum size thresholds. Full implementation likely 2026+. More supply-side flexibility from DERs could dampen RT spikes over time.

### Offer Cap Increase

Traditional $1,000/MWh cap raised to **$2,000/MWh** for cost-verified offers (FERC Order 831). Offers above $1,000 require Market Monitor verification.

---

## 6. Recommended Methodology for RT Modelling

### 6.1 Architecture Overview

```
Phase 1: RT Price Forecasting (1-6 hour ahead)
    |-- LEAR/LASSO baseline (benchmark)
    |-- XGBoost/LightGBM with engineered features
    |-- Transformer or LSTM for temporal dynamics
    |-- Regime-switching overlay (normal vs. scarcity)

Phase 2: DA-RT Spread Prediction
    |-- Seq2Seq or Transformer for nodal spread
    |-- DART spike classifier (three-regime mixture)
    |-- Node selection optimization (congestion-based)

Phase 3: Position Optimization
    |-- CVaR-constrained virtual bidding
    |-- Battery DA-RT co-optimization
    |-- Portfolio diversification across nodes

Phase 4: Risk Management
    |-- BOR/uplift charge estimation
    |-- Price impact modelling for large positions
    |-- Conformal prediction for calibrated intervals
```

### 6.2 Feature Hierarchy for RT Price Modelling

#### Tier 1: Critical Features

| Feature | Why It Matters | Source |
|---------|---------------|--------|
| **Reserve levels** | Nonlinear ORDC pricing drives spikes. Most important for extreme events. | PJM `sr_reserve` |
| **Load forecast error** (actual vs DA forecast) | Primary driver of DA-RT spread. 1-7% errors are baseline. | PJM `inst_load`, `load_frcstd_7_day` |
| **Wind/solar forecast error** | Under-forecast renewables → RT premium (asymmetric impact). | PJM `wind_gen`, `solar_gen` |
| **Temperature** (actual vs forecast) | Drives load forecast error AND generator forced outage rates. | Weather providers |

#### Tier 2: Important Features

| Feature | Why It Matters | Source |
|---------|---------------|--------|
| **Generator outages** (forced, planned) | Tight supply drives scarcity. Temperature-dependent outage rates. | PJM `gen_outages_by_type` |
| **Interface flows vs limits** | Congestion-driven spreads are more predictable than energy-driven. | PJM `transfer_limits_and_flows` |
| **Gas prices** (spot, intraday) | Marginal fuel cost. Gas supply constraints amplify winter spikes. | ICE, CME |
| **DA LMP** (as anchor) | RT deviations from DA are the trading signal. | PJM `da_hrl_lmps` |
| **Transmission outages** | Unplanned outages create RT congestion not in DA. | PJM OASIS |

#### Tier 3: Refinement Features

| Feature | Why It Matters | Source |
|---------|---------------|--------|
| **Lagged RT LMP** (t-1, t-2, ..., t-24) | Autoregressive structure. | PJM `rt_hrl_lmps` |
| **Cross-zonal prices** | Spatial correlations carry predictive power. | PJM nodal LMPs |
| **Hour-of-day / day-of-week** | Structural demand patterns. | Calendar |
| **Regulation market clearing** | Tight regulation signals system stress. | PJM `reg_market_results` |
| **Virtual bid volumes** | Aggregate INC/DEC positions at key nodes. | PJM public data |

### 6.3 Key Modelling Insights from the Literature

1. **Optimize for P&L, not RMSE.** Models trained on economic objectives outperform models trained on statistical accuracy (Jonsson et al., ILO paper). A $2/MWh forecast error during a spike hour costs more than a $5/MWh error during a flat hour.

2. **Probabilistic forecasts are essential.** Distributional neural networks achieve 7-8% profit improvement over point forecasts (Marcjasz et al.). Position sizing should be proportional to confidence.

3. **Three-regime structure for DART spreads.** Normal (Gaussian) + positive spike (GPD) + negative spike (GPD). Covariates in the severity component matter, not just frequency (Forgetta et al.).

4. **DECs are more predictable than INCs.** ~76% of DEC trades coincide with negative spikes, ~80% correctly predict spread sign (Hubert et al.). Asymmetric accuracy is directly actionable.

5. **Node selection is as important as direction.** Congestion-driven spreads are more predictable than energy-driven (Zhao et al.). Focus on nodes near congestion interfaces (Sarfati et al.).

6. **Temperature-conditioned forced outage rates are forecastable.** Scarcity pricing is most likely during extreme heat/cold because generator outage rates are temperature-dependent (Lavin et al.).

7. **Wind forecast errors > solar forecast errors > load forecast errors** in terms of RT price impact (Kiesel & Paraschiv). The relationship is asymmetric: under-forecasts cause larger impacts.

8. **Alpha erodes.** As markets mature and competition increases, DA-RT spreads narrow (Jha & Wolak). Continuous model adaptation and seeking frontier alpha sources is required.

9. **BOR charges erode profits.** Factor uplift allocation into P&L. Large deviations from DA attract disproportionate uplift (Parsons et al., Hadsell & Shawky).

10. **Co-optimize across DA and RT.** Two-stage stochastic models yield 15-25% higher profits than sequential approaches (Risk-Aware ESS paper).

---

## 7. Implementation Priorities

### Immediate Wins (Low Effort, High Impact)

1. **Build a LEAR baseline for PJM RT prices.** Use epftoolbox as starting point. LASSO with 200+ candidate features, 730-day rolling calibration window. This is the benchmark to beat.

2. **Monitor reserve levels in real-time.** The ORDC step functions ($850, $300) create deterministic price jumps. Track synchronized reserve MW against ORDC thresholds.

3. **Track renewable forecast errors.** Compute real-time wind and solar forecast vs actual deviations. These are the leading signal for RT price surprises.

4. **Build DA-RT spread history by node.** Characterize which PJM nodes have persistent, exploitable spreads and which are noise. Focus on congested interfaces first.

### Medium-Term Improvements

5. **DART spike classifier.** Three-regime mixture model (normal, positive spike, negative spike) with weather, load, and gas covariates. Start with Western Hub, expand to key nodes.

6. **XGBoost/LightGBM ensemble for 1-6 hour ahead RT prices.** Features: lagged RT prices, load forecast errors, renewable forecast errors, reserve levels, temperature, gas prices. Use MLForecast for automated feature engineering.

7. **Virtual bidding backtester.** Simulate INC/DEC positions with realistic BOR charge allocation. Account for price impact at scale.

8. **Regime-switching overlay.** Markov switching GARCH to detect normal vs. scarcity regimes. Widen confidence intervals and adjust positions when regime probability shifts.

### Longer-Term Enhancements

9. **Transformer or Seq2Seq for multi-node spread forecasting.** Jointly forecast DA-RT spreads at 10-20 key nodes. Exploit cross-zonal correlations (Zheng & Xu).

10. **DRL-based virtual bidding agent.** CVaR-constrained MDP formulation (Tang et al.). Train on historical PJM data with realistic transaction costs and uplift.

11. **Battery co-optimization engine.** Two-stage stochastic program for DA commitment + RT dispatch with degradation-aware constraints.

12. **Conformal prediction for calibrated intervals.** Layer on top of any point forecast for formal coverage guarantees on RT price ranges.

---

## 8. References Summary

### Papers Cited

1. Weron, R. (2014). Int. J. Forecasting, 30(4). [Link](https://www.sciencedirect.com/science/article/pii/S0169207014001083)
2. Nowotarski, J. & Weron, R. (2018). Renew. Sustain. Energy Rev., 81(1). [Link](https://www.sciencedirect.com/science/article/abs/pii/S1364032117308808)
3. Lago, J. et al. (2021). Applied Energy, 293. [Link](https://www.sciencedirect.com/science/article/pii/S0306261921004529)
4. Yu, R. et al. (2026). arXiv:2602.10071. [Link](https://arxiv.org/abs/2602.10071)
5. Polson, M. & Sokolov, V. (2019). arXiv:1808.05527. [Link](https://arxiv.org/abs/1808.05527)
6. Zheng, H. & Xu, Y. (2020). Sustain. Energy Grids Netw., 24. [Link](https://www.sciencedirect.com/science/article/abs/pii/S2352467720303374)
7. Jami, N. et al. (2023). arXiv:2306.10080. [Link](https://arxiv.org/abs/2306.10080)
8. Marcjasz, G. et al. (2023). Energy Economics, 125. [Link](https://www.sciencedirect.com/science/article/abs/pii/S0140988323003419)
9. Llorente, O. & Portela, J. (2024). arXiv:2403.16108. [Link](https://arxiv.org/abs/2403.16108)
10. Ziel, F. & Weron, R. (2018). Energy Economics, 70. [Link](https://www.sciencedirect.com/science/article/abs/pii/S0140988317303651)
11. Kapoor, N. (2023). J. Forecasting, 42(8). [Link](https://onlinelibrary.wiley.com/doi/full/10.1002/for.3004)
12. Sahraei-Ardakani, M. et al. (2021). IEEE Access, 10. [Link](https://ieeexplore.ieee.org/document/9641818/)
13. Wang, X. et al. (2024). arXiv:2412.00062. [Link](https://arxiv.org/abs/2412.00062)
14. Baltaoglu, S., Tong, L. & Zhao, Q. (2018). arXiv:1802.03010. [Link](https://arxiv.org/abs/1802.03010)
15. Li, Y., Yu, N. & Wang, W. (2021). arXiv:2104.02754. [Link](https://arxiv.org/abs/2104.02754)
16. Samani, E. et al. (2021). arXiv:2109.09238. [Link](https://arxiv.org/abs/2109.09238)
17. Galarneau-Vincent, R. et al. (2023). Energy Economics, 119. [Link](https://www.sciencedirect.com/science/article/abs/pii/S0140988323000191)
18. Forgetta, A. et al. (2025). Energy Economics, 144. [Link](https://www.sciencedirect.com/science/article/abs/pii/S0140988325001562)
19. Hubert, E. et al. (2026). arXiv:2601.05085. [Link](https://arxiv.org/abs/2601.05085)
20. Tang, Q. et al. (2022). Computers & Chem. Eng. [Link](https://www.sciencedirect.com/science/article/abs/pii/S014206152200494X)
21. Sarfati, M. et al. (2021). IEEE Trans. Power Systems. [Link](https://par.nsf.gov/servlets/purl/10283180)
22. Hadsell, L. & Shawky, H. (2016). Electricity Journal. [Link](https://www.sciencedirect.com/science/article/abs/pii/S104061901630063X)
23. Jha, A. & Wolak, F. (2015). J. Regulatory Economics, 48(3). [Link](https://link.springer.com/article/10.1007/s11149-015-9281-3)
24. Parsons, J. et al. (2015). MIT CEEPR WP 2015-002. [Link](https://www.mit.edu/~jparsons/publications/20150300_Financial_Arbitrage_and_Efficient_Dispatch.pdf)
25. Giraldo, J. (2025). Purdue Dissertation. [Link](https://www.purdue.edu/discoverypark/sufg/wp-content/uploads/2025/04/Juan-Giraldo-Dissertation.pdf)
26. Sioshansi, R. et al. (2009). Energy Economics, 31(2). [Link](https://www.sciencedirect.com/science/article/abs/pii/S0140988308001631)
27. Kim, B. & Powell, W. (2018). arXiv:1711.03127. [Link](https://arxiv.org/abs/1711.03127)
28. Krishnamurthy, D. et al. (2018). IEEE Trans. Power Systems. [Link](https://www.osti.gov/pages/servlets/purl/1358239)
29. Cao, J. et al. (2020). IEEE Trans. Smart Grid, 11(5). [Link](https://ieeexplore.ieee.org/document/9061038/)
30. Risk-Aware ESS Participation (2024). Elec. Power Syst. Res. [Link](https://www.sciencedirect.com/science/article/abs/pii/S0378779624006278)
31. Zhao, F. et al. (2021). IEEE Trans. Power Systems. [Link](https://scholarsmine.mst.edu/cgi/viewcontent.cgi?article=5574&context=ele_comeng_facwork)
32. Zhou, Z. et al. (2011). IEEE Trans. Power Systems, 26(4). [Link](https://faculty.sites.iastate.edu/tesfatsi/archive/tesfatsi/CongestionForecasting.ZTL.pdf)
33. ILO for Congestion (2024). arXiv:2412.18003. [Link](https://arxiv.org/abs/2412.18003)
34. PJM FTR Whitepaper (2020). [Link](https://www.pjm.com/-/media/DotCom/library/reports-notices/special-reports/2020/ftr-market-review-whitepaper.pdf)
35. Deng, S. & Oren, S. CEIC WP 06-03. [Link](https://www.cmu.edu/ceic/assets/docs/publications/working-papers/ceic-06-03.pdf)
36. Kiesel, R. & Paraschiv, F. (2019). Energy Policy, 134. [Link](https://www.sciencedirect.com/science/article/abs/pii/S0301421519304057)
37. Ketterer, J. (2014). Energy Economics, 44. [Link](https://www.sciencedirect.com/science/article/abs/pii/S0140988314000875)
38. Millstein, D. et al. (2021). Solar Energy, LBNL. [Link](https://www.sciencedirect.com/science/article/pii/S0038092X21010616)
39. Jonsson, T. et al. (2010). Energy Economics, 32(2). [Link](https://econpapers.repec.org/article/eeeeneeco/v_3a32_3ay_3a2010_3ai_3a2_3ap_3a313-320.htm)
40. Manner, H. et al. (2018). Energy Economics, 73. [Link](https://www.sciencedirect.com/science/article/abs/pii/S0140988318302512)
41. Hogan, W. (2013). Econ. Energy & Envtl. Policy, 2(2). [Link](https://whogan.scholars.harvard.edu/sites/g/files/omnuum4216/files/whogan/files/hogan_ordc_110112r.pdf)
42. Lavin, L. et al. (2020). Energy Policy, 147. [Link](https://www.sciencedirect.com/science/article/pii/S0301421520305747)
43. PJM Reserve Shortage Pricing (2023). [Link](https://www.pjm.com/-/media/DotCom/markets-ops/energy/real-time/reserve-shortage-pricing-paper.pdf)
44. Bajo-Buenestado, R. (2021). Energy Policy, 149. [Link](https://www.sciencedirect.com/science/article/pii/S0301421520307680)
45. Zugno, M. et al. (2013). IEEE Trans. Power Systems. [Link](https://www.researchgate.net/publication/258629006)
46. Wang, X. et al. (2024). arXiv:2403.05743. [Link](https://arxiv.org/abs/2403.05743)
47. Uniejewski, B. et al. (2019). Int. J. Forecasting, 35(4). [Link](https://onlinelibrary.wiley.com/doi/abs/10.1002/for.3004)
48. Energies Review (2025). Energies, 18(12). [Link](https://www.mdpi.com/1996-1073/18/12/3097)

### GitHub Repositories

1. [gridstatus](https://github.com/gridstatus/gridstatus) -- Unified ISO data API (PJM RT/DA/load)
2. [epftoolbox](https://github.com/jeslago/epftoolbox) -- EPF benchmark with PJM data
3. [epf-transformers](https://github.com/osllogon/epf-transformers) -- Transformer EPF
4. [WholesaleElectrPricePredict](https://github.com/rpglab/WholesaleElectrPricePredict) -- Hour-ahead price prediction
5. [ReModels](https://github.com/zakrzewow/remodels) -- QRA for probabilistic EPF
6. [Time-Series-Library](https://github.com/thuml/Time-Series-Library) -- SOTA deep TS models
7. [Darts](https://github.com/unit8co/darts) -- General-purpose TS forecasting
8. [StatsForecast](https://github.com/Nixtla/statsforecast) -- Fast statistical baselines
9. [NeuralForecast](https://github.com/Nixtla/neuralforecast) -- Neural forecasting with HPO
10. [Nixtla/nixtla (TimeGPT)](https://github.com/Nixtla/nixtla) -- Foundation model for TS
11. [PatchTST](https://github.com/yuqinie98/PatchTST) -- ICLR 2023 transformer
12. [MLForecast](https://github.com/Nixtla/mlforecast) -- XGBoost/LightGBM with auto features
13. [EPEX-machine-learning](https://github.com/ekapope/EPEX-machine-learning) -- DA-intraday spread trading
14. [MA-VPP-RL-bidding](https://github.com/jlp237/MA-VPP-RL-bidding) -- RL balancing market bidding
15. [energy-py](https://github.com/ADGEfficiency/energy-py) -- RL battery arbitrage
16. [bess-optimizer](https://github.com/FlexPwr/bess-optimizer) -- Multi-market BESS optimization
17. [battery_model](https://github.com/gschivley/battery_model) -- BESS arbitrage analysis
18. [battery-optimisation-with-drl](https://github.com/RichardFindlay/battery-optimisation-with-drl) -- DQN battery dispatch
19. [PyPSA](https://github.com/PyPSA/PyPSA) -- Power system analysis
20. [Egret](https://github.com/grid-parity-exchange/Egret) -- SCUC/SCED simulation
21. [pomato](https://github.com/richard-weinhold/pomato) -- Nodal market modelling
22. [day-ahead-probabilistic-QR](https://github.com/RichardFindlay/day-ahead-probablistic-forecasting-with-quantile-regression) -- Quantile regression forecasting
23. [Energy-Schaake](https://github.com/FabianKaechele/Energy-Schaake) -- Schaake shuffle scenarios
24. [deepTCN](https://github.com/oneday88/deepTCN) -- Probabilistic TCN
25. [OpenSTEF](https://github.com/OpenSTEF/openstef) -- Automated ML energy forecasting
26. [pjm_dataminer](https://github.com/rzwink/pjm_dataminer) -- PJM DataMiner API wrapper
27. [pyiso](https://github.com/WattTime/pyiso) -- ISO data client libraries
