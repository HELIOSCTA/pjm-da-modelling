---
timestamp_local: "2026-03-13 11:00 ET"
timestamp_utc: "2026-03-13T16:00:00Z"
market: "power"
source: "PJM, GridStatus"
tags: [power, load, outage, solar]
summary: "HE20 3/12 RT printed $889 West / $882 Dom — a system energy event driven by +4.8 GW load overperformance, 6.9 GW solar cliff, 41 GW outages, and MISO loop flows draining 2 GW"
signal_relevance: "DST evening ramp mispricing is persistent — DA underpriced HE20 by $786. Pattern repeated on 3/13 morning (HE8 $1,363). Key setup: solar cliff + cold-driven load + outage season."
confidence: 4
status: "todo"
---

<!-- TODO: Determine if this DST evening ramp mispricing is a tradeable pattern through outage season or a transient 2-3 day event -->
<!-- NOTE: HE20 3/12 was the largest single-hour DART of the week at -$786. Morning ramp 3/13 HE8 hit $1,363 — same mechanism (under-commitment). -->
<!-- REVIEW: Discuss with Edi whether the evening ramp DART is widening or if DA will catch up -->

# RT Price Spike Analysis: 3/12 HE20 ($889) and 3/13 HE8 ($1,363)

## Question / Hypothesis

Why did HE20 on 3/12 print $889 West / $882 Dom when DA cleared at $103/$107? Is this a repeatable pattern through outage season, or will DA adjust?

## Research Notes

### The HE20 Blowout: It Was a System Energy Event

RT system energy price at HE20: **$881.24/MWh**. Congestion was minimal (West -$3, Dom +$2). This wasn't a transmission constraint — PJM simply ran out of cheap generation.

| Hub | DA | RT | DART | RT Congestion |
|-----|----|----|------|---------------|
| Western | $102.60 | $889.13 | -$786.52 | -$3.09 |
| Dominion | $106.73 | $882.18 | -$775.45 | +$1.88 |
| Eastern | $51.08 | $907.18 | -$856.10 | -$7.26 |
| AEP Gen | $94.29 | $854.66 | -$760.37 | +$8.51 |

Note: Eastern Hub had the **largest** DART at -$856 despite having -$40 DA congestion discount. In RT, the system energy price overwhelmed congestion.

### Contrast with HE23: Congestion + Energy

HE23 was a different beast — both energy AND congestion:
- RT energy: $183.78 (high but not HE20-level)
- RT congestion: Dom **+$56.98**, AEP +$43.77, West +$37.75, Eastern **-$128.58**
- Dom printed $241.94 while Eastern only $62.04 — a $180 spread
- This suggests transmission constraints were binding into Dom/AEP by late evening

### Root Cause #1: Load Overperformance (+4,804 MW at HE20)

DA forecast missed badly and the error grew through the evening:

| HE | DA Forecast | RT Actual | Miss |
|----|-------------|-----------|------|
| 17 | 89,806 | 94,446 | **+4,640** |
| 18 | 92,608 | 95,259 | +2,651 |
| 19 | 94,890 | 96,928 | +2,038 |
| 20 | **95,441** | **100,245** | **+4,804** |
| 21 | 95,142 | 101,894 | **+6,752** |
| 22 | 92,860 | 100,122 | +7,262 |
| 23 | 89,040 | 96,654 | **+7,614** |
| 24 | 85,498 | 93,151 | +7,653 |

DA forecast peaked at 95.4 GW for HE20. Actual peaked at **101.9 GW at HE21**. The DA model missed by nearly 7 GW at peak. Cold temperatures + electric heating in Virginia (per GridStatus) drove sustained overperformance that worsened into the evening as heating load built.

Regional load ramp (HE15 → HE20):
- RTO: 94,818 → 100,245 (+5,427 MW)
- WEST: 48,622 → 50,411 (+1,789 MW)
- MIDATL: 30,864 → 33,280 (+2,416 MW) — largest absolute ramp
- SOUTH: 15,333 → 16,554 (+1,221 MW)
- DOM data not available in this query but MIDATL includes DOM

### Root Cause #2: Solar Cliff (-6,855 MW in 2 Hours)

Solar generation collapsed right as load was ramping up:

| HE | Solar | Change | Gas | Gas Change |
|----|-------|--------|-----|------------|
| 17 | 7,441 | +825 | 39,041 | -4 |
| 18 | 7,164 | -277 | 37,989 | **-1,052** |
| 19 | 4,021 | **-3,143** | 40,228 | +2,239 |
| 20 | **309** | **-3,712** | **44,568** | **+4,340** |
| 21 | 6 | -303 | 46,642 | +2,074 |

Critical sequence: Gas was actually **declining** at HE18 (-1,052 MW) right as solar started its cliff. By HE19, gas had to reverse course and ramp +2,239 MW while solar was dropping -3,143 MW. At HE20, gas added +4,340 MW but solar lost -3,712 MW — gas was essentially running to replace solar AND serve new load. Net gain from gas-solar swap: only +628 MW at HE20 vs HE19.

Coal also contributed: +1,324 MW (HE19→20). Oil came online: 461→764 MW. Even with everything ramping, the system was barely keeping up.

### Root Cause #3: Generation-Load Balance Was Razor Thin

| HE | Generation | Load | Margin | Note |
|----|-----------|------|--------|------|
| 17 | 98,172 | 94,446 | +3,726 | Comfortable |
| 18 | 96,703 | 95,259 | **+1,444** | Tightening — gen DROPPED |
| 19 | 98,101 | 96,928 | **+1,173** | Tightest pre-spike |
| 20 | 101,428 | 100,245 | **+1,183** | Tight + 2 GW net exports |
| 21 | 103,798 | 101,894 | +1,904 | Still tight |
| 22 | 102,711 | 100,122 | +2,589 | Easing |

HE18 was the setup: generation actually **fell** 1,469 MW (98,172→96,703) while load rose 813 MW. The margin went from 3,726 to 1,444 MW. Add ~2 GW of net exports via tie lines and PJM was effectively at **negative** reserve margin going into HE19.

### Root Cause #4: 41 GW Outages (Forced Outages Concentrated in WEST)

3/12 outages:
- RTO: 41,063 MW total (22,356 planned, 7,443 maint, **11,264 forced**)
- WEST: 19,822 MW total (10,023 planned, 2,933 maint, **6,866 forced**)
- MIDATL_DOM: 21,241 MW total (12,333 planned, 4,510 maint, 4,398 forced)

**61% of forced outages (6,866 of 11,264 MW) were in the WEST region.** This limited the ramp capacity exactly where it was needed most. WEST also had the largest total outages (19,822 MW / 48% of RTO).

### Root Cause #5: MISO Unscheduled Flows Draining Supply

Tie flows during the evening ramp (all positive = into PJM):

| HE | RTO Net | MISO | NIPS (unsched) | NYISO | TVA |
|----|---------|------|----------------|-------|-----|
| 18 | -1,703 | -1,764 | -1,478 (all) | -2,102 | +1,486 |
| 19 | -1,630 | -1,662 | -1,885 (all) | -2,130 | +1,693 |
| 20 | **-1,968** | -1,612 | **-2,031 (all)** | -1,959 | +1,445 |
| 21 | -2,031 | -1,837 | -2,258 (all) | -2,101 | +1,477 |

PJM was net exporting ~2 GW throughout the spike. NIPS (Northern Indiana) was sending **100% unscheduled** flows to MISO — over 2 GW at HE20. The MISO NYISO exports were also significant (-2 GW). Southeast imports (TVA, DUKE, CPLE) were helping (+2.7 GW combined) but couldn't offset the MISO/NYISO drain.

With 100 GW load + 2 GW net exports, PJM needed ~102 GW of generation from a fleet with 41 GW offline.

### Why DA Got It So Wrong

The DA model failed on multiple inputs simultaneously:
1. **Load forecast: -4.8 GW miss at HE20, growing to -7.6 GW by HE23.** Cold-driven heating load + DST peak shift not captured.
2. **Solar decline rate:** DA likely modeled a smoother solar sunset. Actual was a cliff: -6,855 MW in 2 hours.
3. **Gas unit commitment:** DA cleared assuming ~$87/MWh system energy at HE20. RT needed $881/MWh — the DA didn't commit enough fast-start capacity.
4. **Timing of peak:** DA peaked load forecast at HE20 (95.4 GW). Actual peak was HE21 (101.9 GW). DST shifted the evening peak later than the model expected.

### 3/13 Morning: Same Mechanism, Different Hour

HE8 on 3/13 printed $1,363 Dom / $1,339 West — even higher than HE20 the night before. Same root cause: cold morning temps drove heating load above DA commitment levels. With 45 GW outages (up 4 GW from 3/12), the morning ramp response was even more constrained.

## Data Sources

- `pjm_cleaned.pjm_lmps_hourly` — DA/RT/DART by hub by hour
- `pjm_cleaned.pjm_load_rt_prelim_hourly` — RT load by region by hour
- `pjm_cleaned.pjm_load_forecast_hourly` — DA load forecast by hour
- `pjm_cleaned.pjm_fuel_mix_hourly` — Generation by fuel type by hour
- `pjm_cleaned.pjm_tie_flows_hourly` — Tie line flows by hour
- `pjm_cleaned.pjm_outages_actual_daily` — Outages by region by day
- GridStatus blurb (3/13 morning)

## Conclusion

HE20's $889 print was **not** a congestion event or a single-unit trip. It was a systemic failure of the DA model to price the evening ramp under these conditions:

1. **Solar cliff + load ramp = double squeeze.** 6.9 GW of solar disappeared in 2 hours while load was adding 5.4 GW. Gas couldn't ramp fast enough to replace both.
2. **41 GW outages with forced outages concentrated in WEST** (61% of forced) limited ramp capacity.
3. **2 GW net exports via MISO loop flows** meant PJM's effective load was ~102 GW against a constrained fleet.
4. **DA missed load by 4.8-7.6 GW** because cold temps + DST shift weren't in the model.

**Is this tradeable?** The pattern has now repeated across two events (evening 3/12, morning 3/13). The setup persists as long as:
- Outages stay >40 GW (outage season is accelerating)
- Cold snaps drive heating load above forecast
- Solar cliff coincides with evening ramp (structural until DST settles in)

The DA will eventually adjust, but PJM's load forecasting model appears to be systematically behind on cold-driven shoulder season load. The DART opportunity concentrates in **HE7-8 (morning ramp)** and **HE19-21 (evening ramp)** when solar transitions collide with thermal demand.
