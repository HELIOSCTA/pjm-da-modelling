# Like-Day Strip Forecast — Issues and Recommended Direction

## Why the Like-Day Model Needs a Reference Date

The like-day model forecasts by finding historical days that look like today and using what actually happened the next day. The reference date defines "today's conditions" (what we're matching against) and determines which historical next-day prices to pull.

## The Rolling-Reference Strip Issue

For a multi-day strip, we fake a future reference day by rewrapping today's realized prices, gas, and load with a future calendar. Every day in the strip is anchored to the same starting regime and can't drift far from today's market, even five days out.

## Why Forecasting the Reference Day Isn't the Fix

Tempting solution: plug forecasts (load, outages, weather) into the reference day for D+2, D+3, etc. This doesn't work:

**1. Like-day's whole premise is matching to reality, not predictions.** The model's edge is saying "this day actually happened, here's what followed." If we build a reference out of forecasts, we're matching against a day that never existed — no historical analog will truly resemble it.

**2. Features are correlated, so partial updates lie.** Load, gas, congestion, renewable variability, and fuel mix all move together. If we plug a higher load forecast in but leave gas, congestion, and LMP at today's values, we've built a Frankenstein day — high-load conditions sitting on top of a low-gas, calm-congestion regime that's internally inconsistent and matches no historical pattern.

**3. Forecasts carry their own error.** The load forecast has a ~2–3% MAPE. Using it as the reference stacks that error onto the analog-selection error and the D+1 projection error — we're compounding three sources of uncertainty instead of one.

**4. It breaks backtesting.** Honest backtests require replaying the exact forecast we would have had on that day. If the reference is a blend of realized and forecast, we'd need historical load-forecast vintages for every date — which we don't store and can't reconstruct faithfully.

**5. It hides the real fix.** If the strip isn't working past D+2, the answer isn't patching more forecasts into the reference — it's admitting the like-day model is a D+1 tool by construction and using a different model for D+2 onward.

## Why LightGBM / LASSO QR Work for D+2+

**The structural problem with like-day past D+1:**
Like-day outputs a real historical day's next-day prices. To forecast D+2, you'd need to match on "end-of-D+1 state" — which hasn't happened. There's no honest way to do it, which is why the strip has to fake a reference day.

**LightGBM and LASSO QR — feature-to-price, no analog needed:**
These don't need a historical day to copy from. They learn the relationship between conditions (load, gas, outages, weather) and price directly from years of data. For D+2 you plug in D+2's load forecast, D+2's gas, D+2's outages — the model predicts the price. No pretending about reference days, no Frankenstein rows. If the market says tomorrow is 95 GW even though we've never seen a shoulder-season day that hot, the model can still answer because it learned how price responds to load in general, not which specific day looked like today.

## The Split in Plain Terms

- **D+1:** like-day is great — we know today's state, we match it, we take next-day.
- **D+2+:** like-day runs out of road. Switch to models that map features to price (LightGBM, LASSO QR).

## Literature Support

The arguments above are consistent with the published electricity-price-forecasting (EPF) literature. The foundational similar-day / analog-ensemble papers explicitly frame the method as a **next-day** tool, and the multi-day literature has moved to generative or feature-based approaches precisely because the analog premise breaks down past D+1.

### Similar-day / analog methods are defined for D+1

- **Che & Chen (2007), "A Novel Approach to Forecast Electricity Price for PJM Using Neural Network and Similar Days Method,"** *IEEE Trans. Power Systems.* — the foundational PJM similar-day paper. The five-step similar-day procedure (reference-day definition → interval → similarity → sort → selection) is specified for **next-day** LMP forecasting. There is no multi-day variant in the methodology. [Link](https://ieeexplore.ieee.org/abstract/document/4349103/)

- **Lora et al. (2020), "Short-Term Electricity Price Forecasting Based on Similar Day-Based Neural Network,"** *Energies* 13(17), 4408. — SDR and SDANN are evaluated only for **day-ahead** forecasts. Window sizes (d = 15, 30, 45, 60) refer to the *lookback* for analog selection, not the *horizon*. [Link](https://www.mdpi.com/1996-1073/13/17/4408)

- **Delle Monache et al. (2013), "Probabilistic Weather Prediction with an Analog Ensemble,"** *Monthly Weather Review* 141(10). — the canonical Analog Ensemble (AnEn) methodology. Each analog's *verifying observation* — i.e., the actual outcome one step after the analog — becomes an ensemble member. The method is one-step-ahead by construction; multi-step extensions require either chaining (which compounds error) or switching to a generative model. [Link](https://journals.ametsoc.org/view/journals/mwre/141/10/mwr-d-12-00281.1.xml)

**Implication for our strip:** we are using an analog framework outside the horizon it was designed for. The rolling-reference workaround is not in any of these papers — it is a local patch.

### Beyond D+1, the literature uses feature-based or generative models

- **Lago, Marcjasz, De Schutter, Weron (2021), "Forecasting day-ahead electricity prices: A review of state-of-the-art algorithms, best practices and an open-access benchmark,"** *Applied Energy* 293, 116983. — the current benchmark paper (including PJM). Their reference models **LEAR** (LASSO-estimated autoregressive, the lineage of our LASSO QR) and **DNN** are feature-to-price mappings, not analog methods. [Link](https://www.sciencedirect.com/science/article/pii/S0306261921004529)

- **Weron (2014), "Electricity Price Forecasting: A review of the state-of-the-art with a look into the future,"** *Int. J. Forecasting* 30, 1030–1081. — explicitly notes that "KNN/similar-day approaches often underperform advanced ensemble models (RF, XGBoost) due to sensitivity to neighbor selection and distance metrics." The review also stresses that load forecasts and lagged prices dominate explanatory power — exactly the features LightGBM and LASSO QR use directly. [Link](https://www.sciencedirect.com/science/article/pii/S0169207014001083)

- **"Multivariate scenario generation of day-ahead electricity prices using normalizing flows,"** *Applied Energy* (2024). — the paper on multi-day scenario generation explicitly argues that analog/KNN sampling is outperformed by conditional generative models on energy score and variogram score. The conditioning vector (load, wind, solar, prior-day prices) is plugged in directly — no analog required. [Link](https://www.sciencedirect.com/science/article/pii/S030626192400624X)

**Implication for our strip:** the field's answer to "how do you forecast D+2, D+3, D+5" is not "patch the analog model" — it's "use a feature-based or generative model that accepts forward conditioning inputs cleanly."

### Synthetic/imputed reference features break the analog premise

- **Weron (2014), op. cit.** — the review emphasizes that normalization and feature quality matter precisely because analog distance is sensitive to the coherence of the feature vector. Plugging in forecasts for some reference features while leaving others realized breaks that coherence.

- **Tajmouati et al. (2024), "Applying k-nearest neighbors to time series forecasting,"** *J. Forecasting* 43(5). — documents that KNN performance is highly sensitive to feature selection and that small perturbations in input structure materially degrade forecast quality. A Frankenstein reference row is exactly the kind of perturbation the method is not robust to. [Link](https://onlinelibrary.wiley.com/doi/10.1002/for.3093)

- **Delle Monache et al. (2013), op. cit.** — AnEn requires 12–15 months of **forecasts and observations** during training for the analog-to-observation mapping to be calibrated. Our rolling-reference approach has no such calibration step; we are using mismatched-vintage inputs (today's realized + target-day forecasts) at inference without any training-time equivalent.

### Multi-model combination is the validated path

- **Nowotarski & Weron (2015), "Computing electricity spot price prediction intervals using quantile regression and forecast averaging,"** *Computational Statistics* 30(3). — introduces **Quantile Regression Averaging (QRA)**, which won GEFCom2014 and is the dominant probabilistic EPF paradigm. The recommendation is to combine diverse point forecasts (like-day, LASSO, LightGBM) rather than force one model outside its horizon. [Link](https://link.springer.com/article/10.1007/s00180-014-0523-0)

- **Nowotarski & Weron (2018), "Recent advances in electricity price forecasting: A review of probabilistic forecasting,"** *Renew. Sustain. Energy Rev.* 81(1), 1548–1568. — positions like-day/analog approaches as **one input** to a larger ensemble, not as a standalone multi-day forecaster. [Link](https://www.sciencedirect.com/science/article/abs/pii/S1364032117308808)

### Summary

The literature is consistent: analog/similar-day methods (Che & Chen; Lora; Delle Monache) are **D+1 tools**. Multi-horizon probabilistic EPF is done with feature-based parametric models (Lago et al.; Weron) or conditional generative models (normalizing-flow paper), and best results come from **combining** them via QRA (Nowotarski & Weron). Our rolling-reference strip is outside this playbook — the correct move is to hand D+2+ to LightGBM / LASSO QR and, where useful, blend via QRA rather than stretch like-day past its design point.
