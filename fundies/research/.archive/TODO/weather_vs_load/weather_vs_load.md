---
timestamp_local: "2026-03-11 11:05 ET"
timestamp_utc: "2026-03-11T15:05:00Z"
market: "power"
source: ""
tags: [weather, storms, load, seasonality, solar, irradiance, clouds]
summary: "Storm/cloud impact on load varies by season — winter/shoulder bullish, summer bearish. Irradiance drives BTM solar offset and sets the midday load shape."
signal_relevance: "Seasonal directional signal for storm/cloud events; irradiance forecast vs actual is the diagnostic for load-beats-forecast days"
confidence: 3
status: "todo"
---

Yeah, you’re right there’s definitely seasonality with it
 
Winter storms = bullish
 
Shoulder storms = generally bullish (lack of solar, high outages)
 
But summer is the flip where’s it’s bearish

Cooling demand kicks in around **75°F** — in shoulder season (Mar-Apr), Southern Virginia can hit this on warm afternoons while Northern PJM is still in heating mode. This creates simultaneous heating + cooling load (the MISO "dual load" pattern). Residual daytime warmth from a warm afternoon dampens the next morning’s heating ramp — so a cooler day today → sharper heating response tomorrow AM. Clear skies overnight amplify this via radiative cooling.

---

## Dew Point & Cooling Load (2026-04-13)

### Why Dew Point Matters More Than Temperature for Cooling Load

Dew point measures absolute moisture content in the air. Unlike relative humidity (which changes with temperature), dew point is fixed — 60F dew point means the same moisture whether it’s 75F or 95F outside. For cooling load, dew point is the better predictor because AC systems don’t just cool air, they **dehumidify** it. Humid air is harder and more energy-intensive to condition.

In summer, **feels-like temperature** has a much stronger impact on load than in winter because humidity directly amplifies cooling demand. Feels-like = f(temperature, dew point). Low dew points mean feels-like ≈ actual air temp; high dew points push feels-like well above.

### Dew Point Load Impact Scale

| Dew Point | Feel | Load Impact |
|-----------|------|-------------|
| <55F | Comfortable/dry | Minimal cooling penalty |
| 55-65F | Noticeable but tolerable | Moderate — AC runs but not hard |
| 65-70F | Muggy | Significant — AC works harder to dehumidify |
| 70F+ | Oppressive | Peak cooling load — drives summer peaks |

### Case Study: Apr 15 (Wed) South Load Forecast

PJM forecast has Southern PJM peaking **<20 GW** despite 90+F highs for multiple consecutive days.

**Dew point context (Apr 15 forecast)**:
- PJM aggregate: 60F — moderate
- Southern zones (Baltimore 59F, Dover 60F, Atlantic City 59F): low 60s
- Western/Ohio zones (Columbus 60F, Cincinnati 62F, Akron 63F): slightly higher than South

At 59-63F dew points, AC will run but you don’t get the humidity multiplier that turns a 90F day into a 95F+ feels-like day. **The bull case for South load overperformance rests on multi-day heat accumulation and warm overnight lows (70s), not humidity.**

### Multi-Day Heat Accumulation Effect

Multiple consecutive days of 90+F with overnight lows only in the 70s reduces natural structural cooling that occurs after sunset. Buildings retain heat, cooling systems run longer, and each successive warm day compounds the load effect. This is a known source of systematic forecast bias — PJM’s models may underweight the cumulative effect.

### Regional Divergence Pattern

When Southern PJM runs hot + dry while Western PJM is milder:
- South: load overperforms forecast (under-forecast bias)
- West: load may underperform
- Creates **regional DART divergence** — positive DARTs at DOM/BGE/PEPCO, negative at western hubs
- Consistent with operator-effect-on-congestion and pjm-overstated-forecast research

---

## Clouds / Irradiance Seasonality (2026-04-22)

<!-- TODO: verify the "cloudy shoulder day beats forecast" pattern with PJM solar forecast vs actual + DA/RT clears across N cloudy shoulder days -->
<!-- NOTE: NEPOOL (ISO-NE) publishes solar forecast + actual cleanly; PJM equivalent is on Data Miner / Markets Gateway -->

### Core Rule

Clouds and solar irradiance are **asymmetric price drivers by season**:

- **Shoulder / winter: clouds = bullish.** BTM + utility solar collapses (2-5 GW in PJM). Load is driven by temperature not irradiance, so at mild temps (55-60F) demand is unchanged. Supply curve shifts left, demand flat → price up.
- **Summer: clouds = bearish.** Solar still collapses, but clouds suppress peak temps 3-8F. AC load is nonlinear above 80F — small temp drops collapse a lot of demand. Demand drops faster than supply → price down.

### The "Load Hangs Flat" Mechanism

On a sunny shoulder day, midday load shows a **solar belly** — net load dips HE11-15 as BTM solar offsets demand. On a cloudy shoulder day that belly disappears. Load prints flat-to-rising straight through midday. Two things happen simultaneously:
1. More energy to serve (no BTM offset)
2. No cheap solar to serve it

That's the bullish setup. Today (2026-04-22) is a live example — Southern Region load running ~500-1000 MW above P-Forecast through the morning ramp, heavy stratus across mid-Atlantic, 55-60F temps. Textbook cloudy-shoulder regime.

### Directional Matrix

| Season | Day type | Solar MW | Cooling load | Net price |
|--------|----------|----------|--------------|-----------|
| Shoulder / winter | Cloudy | ↓↓ | flat | **bullish** |
| Shoulder / winter | Sunny | ↑↑ | flat | bearish (solar belly) |
| Summer | Cloudy | ↓↓ | ↓↓↓ | **bearish** |
| Summer | Sunny | ↑↑ | ↑↑↑ | bullish |

### Irradiance Primer

- **GHI (Global Horizontal Irradiance)** — standard input for solar forecasts. Clear-sky noon mid-latitudes peaks ~900-1000 W/m². Heavy overcast cuts to 100-300 W/m².
- **DNI (Direct Normal Irradiance)** — direct beam only; collapses to ~0 under thick overcast.
- **DHI (Diffuse Horizontal Irradiance)** — scattered sky light; rises modestly under thin cloud, falls under thick.
- **GHI = DNI × cos(θ) + DHI**, where θ = solar zenith angle.

### How Irradiance Flows Into PJM Prices

1. NWP model (HRRR, GFS, ECMWF) forecasts cloud cover → cloud optical depth → irradiance forecast.
2. Solar fleet model maps irradiance → MW output. PJM ~10+ GW utility-scale + ~3-5 GW BTM (heaviest in NJ/MD/DE, growing).
3. Load forecast **subtracts** BTM solar. PJM "load" is net of BTM.
4. Miss mechanics: if forecast assumes 700 W/m² GHI at HE11 and actual is 250 W/m², BTM solar produces ~35% of what was baked in. On 4 GW BTM that's ~2.5 GW of "phantom load" appearing on the grid. Same effect on utility-scale shows up as a generation shortfall, but stack impact is identical.

### Diagnostic Workflow (Don't Guess)

When load beats forecast on a cloudy day, the diagnostic isn't "probably BTM solar" — it's:

1. **PJM solar forecast vs actual MW by hour.** If solar actual < solar forecast → irradiance miss confirmed.
2. **Temp forecast vs actual in DOM/PEPCO.** If actual cooler than forecast, residual heating load.
3. **If solar is tracking forecast and load is still over** → forecaster underestimated underlying demand (data center ramp, industrial). Different trade.

NWP cloud forecasts are notoriously low-skill in shoulder season — under-clouding is common, which is why DA under-commits on cloudy shoulder days and RT has to scramble. That's the **RT > DA** setup.

### Trade Record Cross-Reference

- **3/26 DA shorts (-$8,464)**: low solar + outages in PEPCO. PJM committed more expensive units than forecast assumed. Shoulder + low renewables = shorting DA is structurally wrong side.
- **3/24 BalDay RT long**: congestion + outages during shoulder, RT $51 vs DA $41 — thesis was regime-consistent (cloudy/shoulder bullish) even though execution got shaken out.
- **3/19 DA short Sat**: warm + clear + weekend soft → bearish shoulder setup, correct side (would-win +$3.60).

### Action Items

- [ ] Pull 10-20 cloudy shoulder days from past year: compare PJM solar forecast vs actual vs DA clear vs RT clear.
- [ ] Build a simple daily check: cloud forecast (HRRR GHI) vs DA clear — flag days where DA looks under-priced relative to forecast cloud cover.
- [ ] Validate the 3-8F summer temp suppression number against historical cloudy-vs-sunny summer days (DOM, PEPCO).
- [ ] Identify the exact PJM Data Miner endpoint for solar forecast + solar actual by hour.

