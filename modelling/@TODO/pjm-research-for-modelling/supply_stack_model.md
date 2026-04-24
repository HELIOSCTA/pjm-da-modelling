# Supply Stack Model: Implementation Summary

## Objective
Build a forward-looking, physically interpretable DA LMP forecast model for PJM that:
- Takes hourly load, renewable forecasts, outages, and fuel prices.
- Builds an hourly merit-order supply stack.
- Finds the marginal unit at each hour.
- Sets forecast price from marginal variable cost (plus configurable adder).

Core intuition:
price is driven by the marginal generator needed to serve net load.

## External GitHub Benchmarks: What to Copy

These recommendations are based on patterns from mature open-source power/data projects:

1. PowNet (`Critical-Infrastructure-Systems-Lab/PowNet`)
- Strong pattern: deterministic simulation pipeline from tabular inputs to reproducible outputs.
- Copy for this model:
  - Keep dispatch deterministic first, then layer stochastic uncertainty.
  - Make every forecast run reproducible from one config object + one snapshot of inputs.

2. nempy (`UNSW-CEEM/nempy`)
- Strong pattern: market-dispatch logic with transparent assumptions and worked examples.
- Copy for this model:
  - Keep dispatch assumptions explicit in output payload (reserve margin, derates, outage method).
  - Add side-by-side actual-vs-modeled validation for price and marginal fuel behavior.

3. PyPSA / PyPSA-Eur / PyPSA-Earth
- Strong pattern: clean model architecture + scenario workflows + diagnostics.
- Copy for this model:
  - Separate modules cleanly: data assembly, stack construction, dispatch, uncertainty, reporting.
  - Treat diagnostics and validation outputs as first-class deliverables, not ad hoc notebooks.

4. gridstatus (`gridstatus/gridstatus`)
- Strong pattern: stable data interfaces across heterogeneous ISO sources and rigorous tests.
- Copy for this model:
  - Enforce strict source contracts for columns, dtypes, row completeness, and timezone/hour semantics.
  - Add source-level smoke tests and regression fixtures for known edge dates (HE24, DST boundary behavior).

5. Google Data Validation Tool (`GoogleCloudPlatform/professional-services-data-validator`)
- Strong pattern: config-driven data quality checks and repeatable validation reports.
- Copy for this model:
  - Add automated row-count, aggregate, and range checks for all upstream tables used by stack forecast.
  - Persist validation results alongside forecast runs for auditability.

## Data Sources: What You Have and What Is Missing

### Available Now: Ready to Use

1. Hourly Load Forecast (`data/pjm_load_forecast_hourly.py`)
- `pull()`: latest PJM RTO load forecast, today onward.
- `pull_da_history()`: full historical DA load forecasts.
- `pull_da_cutoff_vintages()`: 4 vintages (DA cutoff, -12h, -24h, -48h).
- Key columns: `forecast_date`, `hour_ending`, `forecast_load_mw`.
- Stack usage: demand side backbone.
  - `net_load = load_forecast - solar - wind`.

2. Hourly Solar Forecast (`data/solar_forecast_hourly.py`)
- `pull()`: D+1 solar forecast from gridstatus (published day-before).
- Key columns: `date`, `hour_ending`, `solar_forecast`.
- Stack usage: subtract from load to get net load (zero-marginal-cost resource).

3. Hourly Wind Forecast (`data/wind_forecast_hourly.py`)
- `pull()`: D+1 wind forecast from gridstatus (published day-before).
- Key columns: `date`, `hour_ending`, `wind_forecast`.
- Stack usage: subtract from load before dispatching thermal stack.

4. Hourly Gas Prices (10 hubs) (`data/gas_prices_hourly.py`)
- `pull()`: next-day gas cash prices for 10 PJM-relevant hubs, hourly since 2020.
- Key hubs + gas-linked capacity context:
  - `gas_m3` (Tetco M3): 17,767 MW (Western PJM marginal hub)
  - `gas_tco` (Columbia TCO): 17,677 MW
  - `gas_tz6` (Transco Z6): 13,447 MW
  - `gas_dom_south` (Dominion South): 12,035 MW
  - `gas_ventura` (NNG Ventura): 7,771 MW
  - plus 5 additional hubs in the source table
- Stack usage: primary cost driver.
  - gas block variable cost = `heat_rate * gas_price + VOM`.
  - The hub-to-capacity context in the source header is directly useful for stack mapping.

5. Daily Outages (`data/outages_actual_daily.py`, `data/outages_forecast_daily.py`)
- Actuals: `date`, `region`, `total_outages_mw`, `planned_outages_mw`, `maintenance_outages_mw`, `forced_outages_mw`.
- Forecast: same outage fields plus `forecast_execution_date`, `forecast_rank`, `forecast_day_number`.
- Stack usage: subtract from available capacity before dispatch.
- Current limitation: aggregate MW only (no fuel-type outage split), so v1 must allocate outages by heuristic (for example pro-rata).

6. Historical Fuel Mix (`data/fuel_mix_hourly.py`)
- `pull()`: hourly actual generation by fuel type from gridstatus.
- Key columns include:
  - `date`, `hour_ending`, `solar`, `wind`, `gas`, `coal`, `nuclear`, `hydro`, `oil`, `storage`, `other`, `other_renewables`, `multiple_fuels`.
- Stack usage:
  - Derive fleet capacity in a fast-start approach (Option B).
  - Backtest validation: compare modeled dispatch vs observed fuel mix behavior.

7. Hub-Gas Mapping + Heat-Rate Sanity Checks (`views/regional_spark_spreads.py`)
- Existing mapping:
  - Western Hub -> Tetco M3
  - AEP -> Tetco M2
  - Dominion -> Transco Z5
  - Eastern -> Transco Z6
- Reference CC heat rate: `7.0 MMBtu/MWh`.
- Existing implied heat-rate calculation: `lmp / gas_price`.
- Stack usage: fast reasonability check.
  - If CC is marginal, modeled clearing price should be directionally close to `7.0 * gas_hub_price` (before adders).

### Not Available Yet: Needs to Be Added

8. Fleet Database (EIA-860): main gap
- Needed output: PJM generator/fleet blocks in cost-order form.
- Required fields per block:
  - `fuel_type`, `capacity_mw`, `heat_rate_mmbtu_mwh`, `vom_usd_mwh`, `must_run`.
- Build options:
  - Option A (recommended accuracy): static EIA-860 CSV aggregated for PJM, optionally enriched with EIA-923 heat rates.
  - Option B (fast start): derive approximate capacities from historical fuel mix peaks and assign technology-average heat rates.

9. Coal Price: minor gap
- No live coal feed in current DB.
- v1 approach: config constant (quarterly/manual update cadence).
- Limited impact because coal is off-margin in most PJM hours.

10. Oil Price: minor gap
- No live distillate/residual feed in current DB.
- v1 approach: config constant (for scarcity-tail stack blocks).
- Usually low impact except scarcity conditions.

### Summary Table

| Source | Granularity | Key for Stack | Status |
|---|---|---|---|
| Load forecast | Hourly | Demand side | Ready |
| Solar forecast | Hourly | Net load reduction | Ready |
| Wind forecast | Hourly | Net load reduction | Ready |
| Gas prices (10 hubs) | Hourly, 10 hubs | Gas block costs | Ready |
| Outages | Daily, aggregate MW | Capacity derate | Ready (no fuel breakdown) |
| Fuel mix actuals | Hourly, by fuel | Fleet derivation + backtest | Ready |
| Spark spread mapping | Static | Sanity check | Ready |
| Fleet database | Static | Supply curve definition | Needs building |
| Coal price | Slow/quarterly | Coal block cost | Config constant |
| Oil price | Slow/quarterly | Peaker block cost | Config constant |

Bottom line:
- The fleet database is the only substantive missing data build.
- Everything else is already available or can be handled with config constants in v1.

## Proposed 4-Phase Build

### Phase 1: Fleet Database (2-3 days)
Create an initial PJM fleet dataset, preferably from EIA-860 static CSV:
- Fields: fuel type, capacity, heat rate, VOM, must-run flag.
- Bucket by technology (nuclear, coal, CC, CT, oil, hydro/other, renewables/storage).
- Store under `backend/src/supply_stack_model/data/pjm_fleet.csv`.
- Add loader utility to return fleet blocks with cost-ready schema.

Alternative for faster start:
- Technology-level aggregate stack from observed fuel mix + published average heat rates.

### Phase 2: Core Stack/Dispatch Logic (about 2 days)
Create module structure under `backend/src/supply_stack_model/`:
- `configs.py`
- `data/fleet.py`
- `stack/merit_order.py`
- `stack/dispatch.py`
- `pipelines/forecast.py`
- `pipelines/strip_forecast.py`
- `uncertainty/monte_carlo.py` (Phase 3 usage)

Core hourly flow:
1. Compute net load = load - solar - wind.
2. Compute variable cost per block:
   - gas/coal/oil: `heat_rate * fuel_price + VOM`
   - nuclear/renewables: near-zero marginal cost
3. Apply outage derate to available capacity.
4. Sort by cost, compute cumulative capacity.
5. Find first block where cumulative capacity >= net load.
6. Set clearing price from marginal block (+ configurable adder).

### Phase 3: Uncertainty Bands (about 1 day)
Add quantile generation (P10/P25/P50/P75/P90, or wider standard set) via Monte Carlo:
- Forced outage perturbations by technology FOR assumptions.
- Load forecast perturbation.
- Optional gas perturbation.
- Re-dispatch each draw and compute quantiles.

Simpler fallback:
- Calibrate a parametric error distribution around deterministic forecast from historical residuals.

### Phase 4: API/View Integration (about 1 day)
Follow existing forecast model patterns:
- Add `SupplyStackConfig` dataclass with defaults.
- Implement pipeline `run()` returning standard output contract.
- Add view model builder:
  - `views/supply_stack_forecast_results.py`
  - Include marginal fuel/heat-rate metadata in hourly payload.
- Register API route:
  - `/views/supply_stack_forecast_results`
- Add markdown formatter in `views/markdown_formatters.py` for `format=md`.

### Phase 5: Visualization + Interaction + Validation Productization (about 2 days)
Add a backend-first interactive reporting layer and validation endpoints.

#### 5a. Visualization payloads (returned by pipeline + view model)
- `hourly_summary` (24 HE rows):
  - `load_mw`, `solar_mw`, `wind_mw`, `net_load_mw`
  - `clearing_price_usd_mwh`, `marginal_fuel`, `marginal_heat_rate`
  - `reserve_headroom_mw`, `stack_position_pct`
- `hourly_quantiles`:
  - P01..P99 hourly table, plus P10/P25/P50/P75/P90 convenience columns
- `merit_order_by_hour`:
  - per-block cost, derated capacity, cumulative capacity, marginal flag

#### 5b. Charts for the HTML report

All charts use `plotly.graph_objects` / `plotly.express`. Emit standalone HTML
via `fig.to_html(include_plotlyjs='cdn')` (~50 KB per chart vs 3 MB bundled).
Compose multiple figures into one HTML file with a lightweight JS hour selector.

| Chart | Plotly pattern | Precedent repo |
|---|---|---|
| Merit Order Curve (per hour) | `go.Bar` with `base` param for horizontal stacked blocks. x = cumulative capacity (GW), y = marginal cost ($/MWh), color = fuel type. Vertical `go.Scatter` line at net load. Marginal block highlighted with contrasting border. Hover: block name, capacity, heat rate, cost. | power-merit-Eur (Altair `mark_rect` adapted to Plotly) |
| Price vs Net Load | `px.scatter` with `color='marginal_fuel'`. Each point is one HE. Overlay actual DA LMP as second trace if available. | nempy price scatter |
| Marginal Fuel Timeline | `px.bar` with categorical y-axis (fuel type), x = HE1-24, color = fuel type. One bar per hour. | Custom (categorical strip) |
| Quantile Fan Chart | `go.Scatter` with `fill='tonexty'` for nested P10/P25/P50/P75/P90 bands. Actual DA LMP overlaid as markers. | Merit-Order-Effect bootstrap CI bands |
| Reserve Headroom | `go.Bar` showing reserve MW by hour, color-coded red/amber/green by headroom %. | Custom (risk heatmap) |
| Dispatch vs Actual Fuel Mix (backtest) | `go.Scatter` with `stackgroup` for modeled generation by fuel type. Overlay actual from `fuel_mix_hourly` as dashed lines. | PyPSA stacked-area dispatch charts |
| Price Duration Curve (backtest) | Dual-panel `plotly.subplots`: log-scale top for spikes, linear bottom for bulk. Two traces: simulated vs actual, sorted descending. | nempy `benchmarking/pdc_plot.py` |
| DM Test Heatmap (model comparison) | `px.imshow` for pairwise p-value matrix across models (stack, LASSO QR, LightGBM, like-day). Green = significantly better, red = worse. | epftoolbox `plot_multivariate_DM_test` |

#### 5c. Interaction controls

**API-driven scenario overrides** (no separate web framework needed):

FastAPI query params on `/views/supply_stack_forecast_results`:
- `forecast_date`, `region_preset`, `gas_hub` (existing)
- `gas_override_usd`: override gas price for all hours
- `outage_override_mw`: override outage MW
- `load_scale_pct`: scale load forecast (e.g., 110 = +10%)
- `congestion_adder_usd`: override adder
- `outage_method`: `pro_rata` | `forced_weighted` (allocation toggle)

**HTML report hour selector** (no server needed):
- Pre-compute merit order for all 24 hours, embed as JSON in the HTML.
- `<select>` dropdown toggles which hour's stack chart is visible via vanilla JS.
- No Dash/Streamlit/Panel dependency.

**Notebook widgets** (for trader exploration):
- `ipywidgets` sliders bound to `dispatch()` for gas price, load, outages.
- Drag slider → stack curve + clearing price update live in cell output.
- Precedent: power-merit-Eur uses Panel `param.Parameterized` for this pattern;
  `ipywidgets` is lighter and already in the conda env.

**Scenario comparison mode**:
- Base vs stress: supply `gas_shock_pct`, `load_shock_pct`, `outage_shock_pct`.
- Pipeline returns both base and stressed results.
- Output: delta table by hour and period (on-peak / off-peak / flat).

#### 5d. Validation views/endpoints

Add `supply_stack_validation_results` view endpoint with:
- Input quality checks (source completeness, range plausibility).
- Stack mechanics invariants (monotonicity, capacity bounds).
- Hindcast performance metrics (see Validation Framework below).
- Scenario sensitivity summary (price delta per +1% gas, +1 GW outages, etc.).
- Both `format=json` and `format=md` for compact agent/frontend use.

## Key Design Decisions
- Hourly stack (not daily): required because load and gas are hourly.
- Outages: start pro-rata, upgrade to fuel-type allocation when breakdown is available.
- Coal/oil prices: config constants initially; integrate live feeds later.
- Fleet updates: annual refresh from EIA-860.
- No training pipeline required for deterministic core model.

## Practical Implementation Notes
- Start with a minimum viable version:
  - aggregated fleet blocks + single-hour dispatch + sanity check against DA LMP.
- Then expand to 24-hour day + quantile bands + endpoint/view integration.
- Use regional spark spread mapping as early validation anchor:
  - when CC is marginal, clearing price should be directionally close to `7.0 * gas_hub_price` (before adders).

## Validation Framework (Recommended Test Pyramid)

### Layer 1: Data contract tests (source layer)
- Required columns and dtypes for each source pull.
- 24 hourly completeness checks after normalization.
- Gas HE alignment checks (`HE1-9` rollover from gas day).
- Outage row selection determinism (`latest execution`, `highest rank`).

### Layer 2: Stack mechanics tests (model core)
- Merit order strictly non-decreasing by variable cost.
- Cumulative capacity monotonic increasing.
- Derated capacity never negative and never exceeds nameplate.
- Quantiles monotonic by probability for each hour.

### Layer 3: Dispatch behavior tests
- Marginal block selection is stable at boundary conditions.
- Clearing price equals marginal block variable cost plus configured adder.
- Scarcity condition behavior when net load exceeds derated stack.

### Layer 4: Forecast/pipeline contract tests
- Always returns 24 HE rows.
- Includes required keys for API/view/report (`hourly`, `quantiles`, `metrics`, `assumptions`).
- Deterministic reproducibility for fixed seed and fixed input fixtures.

### Layer 5: Backtest validation (rolling historical replay)

**Approach** (adapted from nempy historical dispatch replay):
```
for each historical date in backtest window:
    pull actual inputs (load, solar, wind, gas, outages) for that date
    run stack dispatch → 24 hourly clearing prices
    compare to actual DA LMP → per-hour error
    compare dispatched fuel mix to actual fuel_mix_hourly → generation delta by fuel type
    record marginal fuel per hour for fuel-match analysis
```

**Price accuracy metrics** (from epftoolbox):

| Metric | Formula / Description | What it tells you |
|---|---|---|
| MAE | mean(\|forecast - actual\|) | Average absolute error in $/MWh |
| RMSE | sqrt(mean((forecast - actual)^2)) | Penalizes large misses (spikes) |
| sMAPE | mean(2\|f-a\| / (\|f\|+\|a\|)) | Scale-independent percentage error |
| rMAE | MAE_model / MAE_naive | <1.0 means you beat naive (yesterday's price). Benchmark: 0.7-0.85 is competitive for PJM DA |
| MASE | MAE / MAE_seasonal_naive | Normalized by seasonal naive; handles zeros better than MAPE |

Compute all metrics on `(n_days, 24)` shaped arrays, reporting both per-hour and period aggregates (on-peak, off-peak, flat).

**Quantile calibration metrics**:

| Metric | Description |
|---|---|
| Coverage | % of actuals within [P_low, P_high] band. Target: 80% for P10-P90. |
| Sharpness | Average band width (P90 - P10). Narrower is better at equal coverage. |
| Pinball loss | Asymmetric quantile loss per quantile level. The proper scoring rule for probabilistic forecasts. |

**Statistical significance** (from epftoolbox):
- **Diebold-Mariano (DM) test**: is the stack model *significantly* better/worse than LASSO QR / LightGBM / like-day? Operates on per-hour loss differentials. Use both univariate (per-hour) and multivariate (joint 24-hour) variants.
- **Giacomini-White (GW) test**: conditional predictive ability — does the stack model add value *given* that the other model's forecast is known? If yes, it's useful as an ensemble component even if standalone MAE is worse.

Pairwise results rendered as a DM heatmap (`px.imshow`) for the model comparison dashboard.

**Fuel mix validation**:
- For each backtest date × hour: compare dispatched MW per fuel type to actual `fuel_mix_hourly` columns (`gas`, `coal`, `nuclear`, `hydro`, `oil`).
- Report correlation and MAE per fuel type.
- Visualize as stacked area (modeled) vs dashed lines (actual) per fuel type — adapted from PyPSA-Eur dispatch validation.

**Marginal fuel identification check**:
- For each hour, record which fuel type the model says is marginal.
- Cross-reference against implied heat rate from `regional_spark_spreads.py`:
  - actual `LMP / gas_price` ≈ 7.0 → marginal unit is CC gas
  - actual `LMP / gas_price` ≈ 10.0-11.0 → marginal unit is CT gas
  - actual `LMP / gas_price` ≈ 4.0-5.0 → marginal unit is coal (at coal/gas price ratio)
- Report match rate (% of hours where modeled marginal fuel agrees with implied regime).

**Price Duration Curve (PDC) validation** (adapted from nempy):
- Sort all backtest hours by price descending.
- Dual-panel Plotly figure: log-scale top panel for spikes (>$100), linear bottom for bulk ($0-$100).
- Overlay simulated vs actual as two traces.
- Immediately reveals systematic bias: if the simulated curve is consistently below actual in the top 5%, the stack underestimates scarcity rents. If above in the bulk, VOM or heat rates are too high.

**Non-regression CI gate**:
- Capture baseline MAE / RMSE / coverage after initial calibration.
- Assert in CI that updated fleet data or config changes don't degrade metrics beyond a tolerance band (e.g., MAE < baseline + 15%).

## Why This Model Adds Value
This model is explicitly forward-looking and structural:
- Uses tomorrow fundamentals directly (load, renewables, outages, gas).
- Avoids direct dependence on lagged LMP anchoring.
- Useful as an ensemble complement during regime shifts when backward-looking models underreact.

## Fleet Data: PUDL as a Source for EIA-860/923

PUDL (Public Utility Data Liberation) by Catalyst Cooperative provides cleaned,
denormalized EIA generator data as free Parquet files on S3. No install required.

**Quickstart** — read PJM generators directly from S3:
```python
import pandas as pd
df = pd.read_parquet("s3://pudl.catalyst.coop/stable/out_eia__monthly_generators.parquet")
# Filter to PJM balancing authority, select relevant columns:
# fuel_type_code_pudl, capacity_mw, heat_rate_mmbtu_mwh, fuel_cost_per_mwh, ...
```

**What PUDL provides for the stack model:**

| Need | PUDL table | Column(s) | Notes |
|---|---|---|---|
| Fuel type | `out_eia__monthly_generators` | `fuel_type_code_pudl` | nuclear, gas, coal, oil, hydro, solar, wind, etc. |
| Capacity | same | `capacity_mw` (nameplate), `net_summer_capacity_mw` | Use summer capacity for PJM stack |
| Heat rate | same | `heat_rate_mmbtu_mwh` | Calculated from EIA-923 fuel consumption / generation |
| Fuel cost | same | `fuel_cost_per_mwh`, `fuel_cost_per_mmbtu` | Historical average, not spot — use for coal/oil baseline only |
| Plant location | same | `state`, `balancing_authority_code_eia` | Filter to PJM BA codes |

**What PUDL does NOT provide:**
- VOM (variable O&M) — supplement from NREL ATB or use technology defaults.
- Must-run flag — derive from fuel type (nuclear = must-run) or min generation data.
- Real-time gas prices — already have this from `gas_prices_hourly.py`.

**Recommended fleet build workflow:**
1. Pull `out_eia__monthly_generators` from S3 Parquet (no pip install).
2. Filter to PJM BA codes + latest reporting year.
3. Aggregate by technology class (nuclear, coal, CC gas, CT gas, oil, hydro).
4. Assign VOM from NREL ATB defaults.
5. Export as `pjm_fleet.csv` for the static fleet loader.
6. Refresh annually when EIA publishes new data.

Also available on Kaggle and at <https://data.catalyst.coop> for browsing.

## External References

### Dispatch / Power System Models
- PowNet: <https://github.com/Critical-Infrastructure-Systems-Lab/PowNet>
- nempy (AUS NEM dispatch): <https://github.com/UNSW-CEEM/nempy>
- NEMOSIS (AUS NEM data): <https://github.com/UNSW-CEEM/NEMOSIS>
- PyPSA: <https://github.com/PyPSA/PyPSA>
- PyPSA-Eur: <https://github.com/PyPSA/pypsa-eur>
- PyPSA-Earth: <https://github.com/pypsa-meets-earth/pypsa-earth>
- gridstatus: <https://github.com/gridstatus/gridstatus>

### Data / Fleet
- PUDL (EIA-860/923 cleaned data): <https://github.com/catalyst-cooperative/pudl>
- PUDL S3 data (no install): `s3://pudl.catalyst.coop/stable/`
- PUDL data browser: <https://data.catalyst.coop>
- PowerGenome (fleet assembly for GenX): <https://github.com/PowerGenome/PowerGenome>
- NREL ATB (VOM / cost defaults): <https://atb.nrel.gov>

### Validation / Forecasting
- epftoolbox (MAE, DM test, GW test): <https://github.com/jeslago/epftoolbox>
- Merit-Order-Effect (LOWESS supply curves): <https://github.com/AyrtonB/Merit-Order-Effect>
- Google Data Validation Tool: <https://github.com/GoogleCloudPlatform/professional-services-data-validator>

### Visualization / Dashboards
- power-merit-Eur (interactive stack dashboard): <https://github.com/kavvkon/power-merit-Eur>
- PyPSA interactive plots: `pypsa/plot/statistics/charts.py` in PyPSA repo
- gridemissions (heatmap viz): <https://github.com/jdechalendar/gridemissions>
- Data Science for ESM (Streamlit tutorial): <https://fneum.github.io/data-science-for-esm/dsesm/workshop-interactive-visualisation/>
