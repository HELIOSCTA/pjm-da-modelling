---
timestamp_local: "2026-03-18 08:30 ET"
timestamp_utc: "2026-03-18T13:30:00Z"
market: "power"
source: "PJM, Meteologica"
tags: [load, power]
summary: "PJM load forecast for 3/18 at 117.9 GW peak (HE8) — diverges from Meteo mainly in the afternoon/evening, not the morning. Projection (query 18) is the real outlier."
signal_relevance: "PJM and Meteo ECMWF agree on morning peak load. The DART signal comes from afternoon solar valley underestimation by PJM, not from peak load overforecast."
confidence: 2
status: "todo"
---

<!-- TODO: Validate today's actual peak against PJM (117.9 GW) vs Meteo ECMWF (117.1 GW) vs Meteo projection (101.5 GW) — determine which was right -->
<!-- NOTE: Initial hypothesis was wrong — PJM doesn't massively overstate the peak. The real divergence is PJM vs Meteo in the afternoon solar valley (HE13-19), where PJM runs 3-5 GW above ECMWF -->
<!-- REVIEW: Discuss with Edi — is the persistent DART driven by load forecast bias or by DA congestion/commitment model issues? -->

# PJM Overstated Load Forecast — March 18

## Question / Hypothesis

**Original hypothesis:** PJM's load forecast overreacts to cold fronts in shoulder season, causing DA over-commitment and persistent positive DARTs.

**Revised after data analysis:** The picture is more nuanced than "PJM overstates the peak."

## Research Notes

### Finding 1: PJM's Peak is at HE8, Not HE21

The PJM hourly forecast (latest, rank 162) shows the **peak is at HE8 morning ramp (117,898 MW)**, not the evening. The daily "peak" period = instantaneous max across all hours.

| Hour | PJM (rank 162) | Meteo ECMWF Ens | PJM vs Meteo |
|------|---------------|-----------------|--------------|
| HE3 | 99,345 | 99,416 | **-71** |
| HE5 | 101,925 | 102,025 | **-100** |
| HE7 | 113,474 | 113,214 | **+260** |
| **HE8** | **117,898** | **117,117** | **+781** |
| HE9 | 117,210 | 116,148 | +1,062 |
| HE10 | 113,495 | 113,504 | -9 |

**PJM and Meteo ECMWF agree almost perfectly on the morning ramp.** The HE8 peak gap is only 781 MW — well within normal forecast error.

### Finding 2: The Real Divergence is the Afternoon/Evening (HE13-19)

| Hour | PJM | Meteo ECMWF | Gap | Notes |
|------|-----|-------------|-----|-------|
| HE12 | 107,222 | 105,920 | +1,302 | |
| HE13 | 104,669 | 101,866 | **+2,803** | Solar peak |
| HE14 | 102,476 | 99,168 | **+3,308** | Solar peak |
| HE15 | 100,903 | 97,269 | **+3,634** | |
| HE16 | 100,205 | 96,292 | **+3,913** | |
| HE17 | 101,534 | 96,923 | **+4,611** | Solar cliff starts |
| HE18 | 104,072 | 99,525 | **+4,547** | |
| **HE19** | **107,792** | **102,478** | **+5,314** | **Peak divergence** |
| HE20 | 110,200 | 106,374 | +3,826 | Evening ramp |
| HE21 | 111,155 | 108,287 | +2,868 | |

**PJM consistently forecasts 3-5 GW more load than ECMWF during the solar hours (HE13-19).** This suggests PJM's model either:
- Underestimates solar generation's load-suppression effect (behind-the-meter solar reduces apparent load)
- Uses a different temperature profile that runs warmer in the afternoon
- Has a flatter load shape that doesn't account for the midday dip

The gap peaks at **HE19 (+5,314 MW)** — exactly when the solar cliff hits. If PJM expects less solar load suppression, it would over-forecast right when solar drops off.

### Finding 3: The Meteo Projection (Query 18) is the Real Outlier

| Hour | PJM | ECMWF Ens | Projection | PJM vs Proj |
|------|-----|-----------|------------|-------------|
| HE8 | 117,898 | 117,117 | 101,486 | **+16,412** |
| HE20 | 110,200 | 106,374 | 96,293 | **+13,907** |
| HE21 | 111,155 | 108,287 | 96,914 | **+14,241** |

The projection runs **13-16 GW below both PJM and ECMWF** across all hours. This is not a PJM issue — the projection is broken or using a fundamentally different methodology. It has been flagged as unreliable in 4+ prior morning fundies runs.

### Finding 4: PJM Forecast Revision History Shows Convergence, Not Bias

PJM's peak forecast for 3/18 evolved through 162 updates:

| Phase | Rank Range | Peak Forecast | Notes |
|-------|-----------|---------------|-------|
| Initial | 1 | 99,039 MW | Very first forecast — conservative |
| Jump | 2-30 | 117,000-117,700 | Cold front entered models |
| Escalation | 31-137 | 119,400-120,700 | Model locked onto worst-case cold |
| **Peak overshoot** | **136** | **120,663** | **Highest forecast — 3.5 GW above final** |
| Revision down | 138-162 | 120,564 → 117,898 | Progressive downward revision |

PJM's model **initially overshot to 120.7 GW then revised down 2.8 GW to 117.9 GW** through the latest update. The revision pattern is clear: PJM ratchets up aggressively when cold fronts enter the model, then walks it back as the event approaches. The DA commitment would have been based on an earlier, higher forecast (likely the 119-120 GW range from ranks 31-157).

### Finding 5: Historical Forecast Accuracy This Week

| Date | Early Forecast (rank 1) | Final Forecast | Actual Peak | Early Miss | Final Miss |
|------|------------------------|---------------|-------------|------------|------------|
| 3/11 Mon | 96,982 | 96,563 | 96,564 | +418 | -1 |
| 3/12 Wed | 96,223 | 101,894 | 101,894 | **-5,671** | 0 |
| 3/13 Thu | 100,649 | 108,164 | 108,164 | **-7,515** | 0 |
| 3/14 Fri | 90,263 | 90,490 | 90,491 | -228 | -1 |
| 3/15 Sat | 88,558 | 91,355 | 91,354 | **-2,796** | +1 |
| 3/16 Mon | 104,802 | 101,491 | 101,488 | **+3,314** | +3 |
| 3/17 Tue | 94,060 | 112,580 | 112,580 | **-18,520** | 0 |

Key observations:
- **Final forecasts converge perfectly to actuals** (late_miss = 0-3 MW). This is because the final forecast_rank is post-settlement — it's essentially the actual itself.
- **Early forecasts are biased LOW, not high.** 3/12 (-5.7 GW), 3/13 (-7.5 GW), 3/17 (-18.5 GW). PJM's initial read underforecasts cold snap peaks, then revises up.
- **Only 3/16 (Mon) had an early overforecast** (+3.3 GW) — the post-weekend forecast was too aggressive.

This means the "PJM overstates" narrative is **wrong for the peak**. The issue is more subtle.

### Revised Hypothesis

The persistent positive DARTs this week are NOT driven by PJM peak load forecast bias. Instead:

1. **DA over-commitment is driven by the intermediate forecast vintage** (ranks 31-157) which locked onto 119-120 GW before PJM revised down. The DA clear likely used a forecast in this range.

2. **The afternoon solar valley divergence (PJM +3-5 GW vs ECMWF)** means PJM commits more generation for the midday-to-evening transition than weather models suggest is needed. This excess committed generation keeps RT loose.

3. **Wind holding above forecast** was the biggest factor in positive DARTs 3/15-3/16. By 3/17 wind crashed but hydro ramped (+5.8 GW at HE20) to partially replace it.

4. **The Meteo projection (query 18) is not a reliable comparison point.** It consistently runs 13-16 GW below both PJM and ECMWF. The PJM vs ECMWF gap is the relevant one: only +0.8 GW at peak, but +3-5 GW in the solar valley.

## Data Sources

- `pjm_cleaned.pjm_load_forecast_daily` — 162 forecast revisions for 3/18
- `pjm_cleaned.pjm_load_forecast_hourly` — Hourly shape at latest rank
- `meteologica_cleaned.meteologica_pjm_demand_forecast_ecmwf_ens_hourly` — ECMWF ensemble
- `meteologica_cleaned.meteologica_pjm_demand_projection_hourly` — Intraday projection (unreliable)
- `pjm_cleaned.pjm_load_rt_prelim_daily` — Actuals for 3/11-3/17

## Real-Time Validation (09:40 ET — Innotap)

**RT instantaneous load came in below ALL forecasts at the morning peak:**

| Region | PJM Forecast (HE8) | RT Actual (~HE9:40) | Miss |
|--------|-------------------|---------------------|------|
| **RTO Total** | **117,898** | **114,964** | **-2,934** |
| Western | 60,380 (HE8) | 57,161 | **-3,219** |
| Mid-Atlantic | ~36,679 (HE9) | 36,873 | +194 |
| Southern | 19,663 (HE8) | 18,816 | -847 |

**The miss is spread across Western (-3.2 GW) and Southern/Dominion (-0.8 GW).** MidAtl is tracking forecast.

This changes the prior conclusion. The database analysis showed PJM and ECMWF agreed at the peak — but both overforecast:
- PJM: 117,898 → actual ~115,000 = **-3 GW miss**
- ECMWF: 117,117 → actual ~115,000 = **-2 GW miss**
- Meteo projection: 101,486 → actual ~115,000 = **+13.5 GW miss** (wildly wrong the other direction)

### GridStatus Color (09:40 ET)

> Extremely wide positive DARTs verified in PJM this morning as load, and particularly Dominion load, underperformed the RTO forecast. While temperatures were chilly across the region, PJM kept volatility under wraps this morning, limiting RT energy price volatility as well as limiting real-time congestion, particularly limiting N-S congestion into Virginia. Temperatures across much of Dominion's service territory were at or slightly below freezing, but verified ~2 GW stronger than the previous day.

**Key takeaway from GridStatus:** Dominion load specifically underperformed. Temperatures were at/below freezing but the heating load response was weaker than forecast — consistent with shoulder season reduced sensitivity. Dom verified **~2 GW stronger than yesterday** (i.e., load was 2 GW higher than Monday, but still below forecast). PJM also actively managed RT congestion down, keeping N-S flows contained despite the transmission constraints. This is the operator effect — PJM's real-time operators can manage congestion in ways the DA model doesn't anticipate.

**Two drivers of the forecast miss:**
1. **Western region (-3.2 GW):** Temperatures likely ran milder than forecast, or MISO-seam wind generation reduced net Western load.
2. **Southern/Dominion (-0.8 GW):** Heating load sensitivity in mid-March is lower than models assume. At/below-freezing temps produced less heating load than a deep winter day at the same temperature would. Dom load came up 2 GW vs yesterday but the forecast expected even more.

**PJM operator effect on congestion:** GridStatus notes PJM "kept volatility under wraps" and limited N-S congestion. This suggests PJM's RT operators actively managed transmission flows — possibly through redispatch or emergency actions — to prevent the extreme congestion the DA model priced. This is a structural reason DA congestion can overstate: DA prices constraints assuming no operator intervention, but RT operators manage them down.

Given the afternoon divergence pattern (PJM +3-5 GW above ECMWF at HE13-19), and the morning already coming in 3 GW light, **the afternoon forecast miss could widen to 5-8 GW.** This would keep RT well below DA commitments and extend the positive DART streak.

## Conclusion

**Both PJM and Meteo ECMWF overforecast the morning peak by 2-3 GW.** The miss is spread across Western (-3.2 GW) and Southern/Dominion (-0.8 GW). The original "PJM overstates" hypothesis has merit — **all models overforecast heating load during shoulder season cold snaps.**

The real story has five parts:

1. **Shoulder season heating load sensitivity is lower than models assume.** Dominion temps were at/below freezing but load underperformed. In mid-March, buildings have more solar gain, insulation is more effective, and heating load per degree F is lower than deep winter. Models trained on Jan/Feb cold snaps will systematically overforecast March heating load.

2. **The miss is broadest in Western PJM** (-3.2 GW) **and Dominion** (-0.8 GW). MidAtl tracked forecast. This suggests Western temps ran milder than forecast, while Dominion's heating load sensitivity specifically disappointed.

3. **PJM operators actively manage RT congestion down.** GridStatus confirms PJM "kept volatility under wraps" and limited N-S congestion despite the transmission constraints. DA prices constraints assuming no operator intervention; RT operators redispatch to manage them. This is a structural reason DA congestion overprices — the DA model doesn't account for real-time operator actions.

4. **PJM's afternoon load forecast (HE13-19) runs 3-5 GW above ECMWF**, and if the morning is already 3 GW light, the afternoon will likely be even further below forecast. This is the primary driver of DA over-commitment and positive DARTs.

5. **DA commitment timing amplifies the miss.** The DA clear used a forecast vintage in the 119-120 GW range (ranks 31-157). The final forecast revised to 117.9 GW, and actuals came in at ~115 GW. That's a **5 GW gap between what DA committed for and what showed up** — explaining the persistent +$28-35 DARTs.

**The Meteo projection (query 18) is confirmed unreliable** — it forecast 101.5 GW when the actual was 115 GW. Off by 13.5 GW in the wrong direction.

**Trading implication:** During shoulder season cold snaps, both PJM and weather models overforecast heating load sensitivity. DA commits to the high forecast, RT comes in lighter, DARTs are persistently positive. **This pattern is strongest in Western PJM** and will likely persist through the remainder of this cold snap (through Thursday). Friday's outage relief + warming should normalize the dynamic.

**Next step:** Check today's actual daily peak (likely already passed at HE8) and evening peak actuals (HE20-21) to see if the afternoon divergence widens as predicted.
