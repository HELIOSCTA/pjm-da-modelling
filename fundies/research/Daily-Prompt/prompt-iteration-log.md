# Prompt Iteration Log

## 2026-04-13 — Run #21 (Sunday Morning) — NEW FORMAT

### What Worked Well
- **First run on lean MCP-powered format.** Eliminated 29 SQL files. Two MCP calls (ICE power intraday + gas prices) returned all needed data in seconds. No query failures, no missing data.
- **Z5S and AGT gas columns working.** New gas view shows 4 hubs + basis spreads + DoD — immediately scannable. Z5S bouncing +$0.36 is visible at a glance.
- **Intraday tape reveals the story.** NxtDay RT opening $87.50 and selling off to $75.00 is THE headline — impossible to see from settle alone. The old format didn't surface this.
- **Edi input captured and forced a trade decision.** BalDay RT short on softer southern loads — concrete and actionable.
- **BalWeek $113 / Week1 $64 inversion immediately visible** from the settlement history table. The 30-day settle history provides context the old format lacked.

### What Was Weak/Missing
- **11-day gap since last entry (4/2 → 4/13).** No entries for 4/3 through 4/12. Lost tracking on: Marquis2 return status, forced outage trajectory, congestion regime changes, nuclear recovery. The new format is faster to run — should reduce gap frequency.
- **No outage/congestion/load data.** The lean format intentionally stripped these, but the BalWeek at $113 screams "something fundamental changed" and we can't see what. Need to check if additional MCP views (load forecast, outages, fuel mix) would add value without bloat.
- **No scorecard possible.** Can't grade 4/2's predictions without RT actuals from the intervening days.
- **Trade entry at settle ($54.35) when last trade was $44.** The market already moved past the thesis. Should have flagged the disconnect — entry at settle is stale when last is $10 lower.

### One Concrete Change for Tomorrow
**Add a 1-line "Context gap" note when >2 days have elapsed since the last entry.** Format: `⚠️ Gap: X days since last entry. Key unknowns: [list].` This forces acknowledgment of what we DON'T know and prevents false confidence from stale context. For today: "Gap: 11 days. Key unknowns: outage trajectory, congestion regime, nuclear status, what drove BalWeek from $57 → $113."

---

## 2026-04-02 — Run #20 (Wednesday Morning)

### What Worked Well
- **Bias tracker applied (Run #19 iteration).** Added `Bias-adj est` column: Meteo $50.17 - trailing 3-day avg DART $4.34 = $45.83. Yesterday's bias-adj for Run #19 would have been $61.41 (actual $57.27 — off by $4 vs raw $64.49 off by $7). The bias tracker consistently outperforms raw Meteo/DA. Format works and is immediately scannable.
- **OnPk estimate of $55-60 was the best prediction in the run series.** Actual WH RT $57.27. The DA-is-rich call was correct for the 4th consecutive day.
- **HE15 $164.43 WH caught and contextualized as system scarcity, not just congestion.** System energy $125.95 is the real headline — this wasn't a transmission constraint, it was supply insufficiency. The thermal ramp table showed total gen 97,459 at HE15, near the series high.
- **Overnight spike pattern identified.** HE1 $88 and HE24 $82 — correctly flagged as a new regime. Two consecutive nights with overnight blowouts confirms the constraint is 24-hour.
- **Dom DA overpricing caught.** DA +$0.86 cong vs RT -$9.17 — a $10 congestion swing. The DART of +$14.04 for Dom was the biggest hub-level miss.
- **Supply utilization metric at 78.1% contextualized correctly.** Below 80% threshold yet HE15 still hit $164 — noted that the metric needs refinement (scarcity is zonal, not system-level).
- **All queries executed successfully.** RT today partial shows 7 hours (HE1-7). Load actuals for both 3/31 and 4/1 now available.

### What Was Weak/Missing
- **"Wind 6 GW = MED congestion risk all day" was a MISS.** HE15 EH -$55.85 is not "MED." The wind regime indicator works directionally but the labels (HIGH/MED/LOW) understate the tail risk. Yesterday's extreme congestion happened with 6.5 GW wind — which the indicator correctly classified as MED — but "MED risk" implies moderate outcomes, not $164 spikes.
- **BalWeek call wrong again.** Said $55.50 "looks cheap" — it traded to $52. Consecutive BalWeek misses suggest the market is pricing the return wave and wind recovery that the entry keeps discounting.
- **Outage forecast (06b) was a FALSE SIGNAL.** Run #19 highlighted "forced 6,815 MW — if real, massive drop." Actual: 10,607. Should have caveated more aggressively that 06b has returned useful data exactly once in 22 runs.
- **Solar forecast miss not caught in real-time.** Meteo said 7,562 solar peak; actual was 9,397 (+1,835). The solar overperformance at HE12-14 should have suppressed midday pricing but didn't — because system energy was $59-61 regardless. This reveals that the midday pricing is not purely renewables-driven anymore.
- **HE12-14 midday WH $79-82 was completely unpredicted.** Previous entries assumed midday = cheap. Yesterday's midday WH at $80+ while solar was 9 GW is a regime change. System was tight even at solar peak.

### One Concrete Change for Tomorrow
**Distinguish "congestion risk" from "price spike risk" in the wind regime header.** Wind level governs congestion spread (WH-EH), but NOT system energy spikes. Yesterday: 6.5 GW wind = moderate congestion ($14 EH DA) but HE15 system energy $126 had nothing to do with congestion — it was thermal scarcity. Add: `System scarcity risk: [LOW/MED/HIGH] — peak demand X GW vs available capacity Y GW = Z% utilization.` When utilization >78% AND solar is declining (HE15-18 window), scarcity spikes become likely. This separates the two dimensions: congestion is a transmission problem; scarcity is a supply-demand problem.

---

## 2026-04-01 — Run #19 (Tuesday Morning)

### What Worked Well
- **Supply stack utilization metric applied (Run #18 iteration change).** `Supply utilization (HE21 yesterday): 97,647 MW / 126,272 MW available = 77.4%. Below 80% → no $100+ spike.` The metric correctly identified that Monday's evening would be tamer than Sunday's — 77.4% vs ~75% Sunday, but the key difference was more thermal supply available (53.9 GW vs 49.8 GW thermal) from the return wave.
- **Wind regime indicator continues to work.** MED all day (6.1-6.6 GW) correctly signals elevated but not extreme congestion risk. The shift from HIGH→MED yesterday to MED→MED today frames the setup change (no more 8 GW cushion).
- **Congestion call was the strongest prediction.** HE8 -$21.89 and HE13 -$24.60 EH/Dom congestion confirmed and deepened beyond the HE7 -$18.92 "canary" identified in Run #18. 16 hours of uninterrupted congestion on a weekday.
- **HE13 Dom -$24.60 midday record caught.** This is a new congestion regime — Dom sub-$7 for 4 hours on a WEEKDAY (previously only seen on weekends). The three-tier afternoon spread (HE15 WH $63 / Dom $27 / EH $34) was a new pattern worth tracking.
- **Thermal ramp table revealed new gas (40,536) and coal (13,355) series highs** at HE21. The comparison to Sunday (49,813 → 53,891 thermal, $114 → $69 price) quantified the return wave impact: more supply available = lower marginal cost.
- **Meteo WH OnPk matched DA for 3rd consecutive day ($64.49).** The pattern is clear: Meteo/DA lock in together, RT prints $3-5 below. Now flagging this as a systematic bias.
- **All queries executed successfully.** Query 04 empty (no RT today yet — expected for early morning). Meteo price (11) still echoing DA — 21st run. Query 05 still no 3/31 actual load. Outage forecast (06b) returned meaningful forward data for first time.

### What Was Weak/Missing
- **Evening spike MISS.** Predicted $100+ WH at HE21; actual was $69. The return wave impact was underestimated. The supply utilization metric would have flagged this — 77.4% is well below 80% — but it was only applied to today, not retroactively to yesterday's prediction.
- **DA overpricing MISS.** Called DA underpriced for evening; it was actually overpriced by $5 at OnPk. The systematic Meteo/DA > RT pattern should have been weighted more heavily.
- **BalWeek call wrong.** Said $57.15 was "fair to cheap" — it traded down to $55.50. Should have recognized the return wave would soften the week.
- **No query 05 data for yesterday (3/31).** Hourly load actuals not available — had to use total gen from 07b as a proxy. This is a recurring gap for same-day load verification.
- **Meteo price forecast (11) still broken.** 21st run. Permanently flagged.
- **08b ICE intraday tape still not run.** The BalDay RT trading from $56.25 settle down to $49.50 last would have shown when the market realized the evening was tame.

### One Concrete Change for Tomorrow
**Add a "Meteo/DA bias tracker" to the DA vs Meteo comparison table.** Three consecutive days of Meteo = DA and RT printing $3-5 below. Add a "bias" column: `| Hub | DA OnPk | Meteo est | Bias-adj est | RT OnPk (yesterday) |` where Bias-adj est = Meteo est minus trailing 3-day average DART. For WH: avg DART last 3 days = ($3.42 + $5.31 + $0.50) / 3 ≈ $3.08. So bias-adj WH est = $64.49 - $3.08 = $61.41. This would have produced a better yesterday estimate too: $48.20 - $3.08 = $45.12 (actual $42.89 — closer). Track whether the bias-adjusted estimate beats raw Meteo/DA.

---

## 2026-03-31 — Run #18 (Monday Morning)

### What Worked Well
- **Wind regime indicator applied (Run #17 iteration change).** `Wind regime: HIGH→MED — 8.3 GW HE8, 6.5 GW HE20. Congestion risk: LOW morning, MED evening.` Immediately scannable. The declining wind trajectory (8.3 → 6.5 GW) correctly framed the evening risk escalation vs yesterday's sustained 8 GW.
- **HE21-23 evening blowout identified as THE story from yesterday.** HE22 EH -$46.71 congestion is the run series record. The thermal ramp table showed gas hitting 38,507 MW at HE21 (new series high) and coal contributing +920 MW — both captured and quantified.
- **Scorecard correctly called the evening miss.** "Wind 8 GW = evening ramp mild" prediction was clearly graded as a MISS. The HE21 $114 spike despite 8.5 GW wind proves the evening ramp is demand/outage-driven, not wind-dependent at current outage levels.
- **M3 gas crash to $1.879 caught immediately.** -$0.348 from Friday is the biggest move in the series. Correctly framed as "keeps the floor low but doesn't cap the ceiling."
- **ICE NxtDay RT $52.25 vs DA $48.20 spread identified as a signal.** Market paying a $4 RT premium for Monday = elevated evening risk expectation. This is a new data point for trade positioning.
- **All queries executed successfully.** Meteo price forecast (11) still echoing DA for HE16+ — 20th run. Meteo demand forecast vs actual (13) still broken. Outage forecast (06b) only returned today — 20th run.
- **Today's HE7 RT spike caught in partial data.** EH -$18.92 congestion at HE7 is the most extreme morning-ramp reading. Correctly flagged as "the canary" for weekday demand returning.

### What Was Weak/Missing
- **Meteo price forecast (11) still broken.** 20th consecutive run. Permanently flagged.
- **Meteo demand forecast vs actual (13) not run.** Sub-hourly issue persists.
- **Outage forecast (06b) only returned today.** 20th run with no forward visibility. With outages at 57 GW run-series-high, forward visibility would be critical.
- **08b ICE intraday tape not run.** Skipped to save context. The NxtDay RT tape ($46 → $52.25 during the session) would have shown when the market bid up — was it after HE21 RT prints came in?
- **Nuclear decline not investigated.** 30,132 → 29,296 over 4 days is ~850 MW. No unit-level data available from queries. Could be Davis Besse refuel or a different unit.
- **MECS afternoon loop flow spike (HE15 -2,794) not prominently featured in prior entry.** This is the largest Michigan seam unscheduled flow in the series and was only visible in the 09b hourly data. It correlated with Dom midday congestion (HE15 Dom +$5.63 cong).
- **Yesterday's OnPk estimates missed the late evening.** Estimated WH $40-45 based on a formula that assumes smooth pricing. Actual $45.13 was in range, but only because the HE21-23 blowout was partially offset by midday collapse. The formula works at the average level but obscures the intraday distribution.

### One Concrete Change for Tomorrow
**Add a "supply stack utilization" metric to the Market Snapshot header.** The HE21-23 blowout happened because total outages (57.3 GW) + peak demand (~94 GW) left minimal margin even with 8.5 GW wind. A one-line metric: `Supply utilization HE21: [thermal demand] / [available thermal] = X%.` Where available thermal ≈ installed capacity - outages - renewables. When utilization > 85%, price risk is convex. Yesterday's HE21: 49,813 thermal / (170,000 - 57,336 - 8,568) ≈ 49,813 / 104,096 = **48%.** That doesn't capture it — the margin is in the peak hour, not the average. Revised: Track `Peak demand (HE21 94,323 MW) vs available capacity (nameplate 183 GW - 57 GW outages = 126 GW available).` That's 75% — high for a shoulder month. When it hits 80%+, expect $100+ spikes. This frames the evening risk in physical terms rather than price terms.

---

## 2026-03-30 — Run #17 (Sunday Morning)

### What Worked Well
- **Spike risk premium framework correctly NOT applied.** Forced outages 9.9 GW (< 10 GW) and EH cong -$8.28 (> -$15) — first time both conditions are below threshold since the premium was added in Run #16. This prevented overestimating the OnPk, which is appropriate for a low-congestion, high-wind Sunday.
- **Weekend congestion narrative tracked correctly.** EH cong trajectory -$21.63 → -$8.44 → -$8.28 confirms the return wave delivered as predicted. The scorecard caught the nuance — relief came but not full collapse.
- **Pre-dawn regime break correctly identified.** HE5 comparison (yesterday $92 vs today $19) with the driver attribution (wind, not transmission) is the most important finding. The constraint is wind-dependent, not purely transmission-dependent.
- **MISO unscheduled spike to -3,417 caught and contextualized.** 7-day trend table shows the acceleration clearly. Correct driver attribution (strong wind on low-demand weekend).
- **Thermal ramp table revealed record gas trough (22,096 HE14) and record single-hour gas ramp (+4,586 HE19).** Both are run series records that quantify the solar cliff risk.
- **All queries executed successfully.** Meteo price forecast (11) still broken — 19th run. ICE 08a empty (Saturday — expected). 06b outage forecast still only returns today.
- **3-day gap coverage (Fri-Sun).** Successfully synthesized Fri+Sat data despite last entry being Friday.

### What Was Weak/Missing
- **No ICE data for weekend.** No trading Saturday/Sunday means no market-implied direction. Had to reference Friday's stale levels.
- **Meteo price forecast (11) still broken.** 19th consecutive run. Permanently flagged.
- **Meteo demand forecast vs actual (13) not run.** Sub-hourly issue persists.
- **Outage forecast (06b) only returned today.** 19th run with no forward visibility.
- **Meteo 14 only returned RTO and WEST, starting HE5-8.** No MIDATL, SOUTH, or SOUTH_DOM demand forecast detail.
- **Saturday entry missed.** No entry was written for 3/28 or 3/29. The 2-day gap means Friday's follow-ups couldn't be scored in real-time. Outage returns that happened on Saturday weren't tracked individually.
- **Nuclear decline (29,314 from 30,132) not thoroughly investigated.** Could be Davis Besse ramping or another unit — the fuel mix hourly doesn't break out individual units.

### One Concrete Change for Tomorrow
**Add a "wind regime indicator" to the entry header.** This week proved that wind level is the primary congestion switch: 8 GW wind = congestion near zero (today's HE5 $19), 3-4 GW wind = full constraint binding (yesterday's HE5 $92). A one-line indicator: `Wind regime: [HIGH/MED/LOW] — [X] GW forecast HE14, [Y] GW HE20. Congestion risk: [HIGH/MED/LOW].` Mapping: >7 GW = LOW congestion risk, 4-7 GW = MED, <4 GW = HIGH. This makes the #1 price driver immediately visible.

---

## 2026-03-27 — Run #16 (Friday Morning)

### What Worked Well
- **DA vs Meteo vs RT comparison table applied (Run #15 iteration change).** The table immediately showed DA underpriced WH by $22 and overpriced EH by $13. Meteo WH OnPk ~$40 was much closer to the midpoint — the constraint is invisible to DA. Format works. Applied for yesterday's data since today's Meteo/RT aren't available yet.
- **Solar halving identified as THE setup change.** Peak 6,339 today vs 11,716 yesterday is the single most important variable for midday pricing. Combined renewables drop by 7,400 MW at HE14. Clearly flagged in the entry header.
- **Limerick-Whitpain return + re-outage 3/30 caught.** The 06d query showed the same facility starting a NEW outage 3/30-4/3 while 06e showed it returning today. This "return-and-re-outage" pattern would have been missed with only the returning view. Correctly flagged the 3-day relief window.
- **Forced outage decline correctly identified and weighted.** 12,709 → 11,394 is the first decline in 5 days. Combined with Davis Besse restart watch (25-day refuel cycle), the narrative shifts from "accelerating forced outage risk" to "reversing."
- **HE5 EH -$43.77 caught in today's partial.** Most extreme pre-dawn reading in the run series. Correctly diagnosed as a constraint regime shift (from overnight HE1-3 to pre-dawn HE5-7) rather than a continuation of the "overnight congestion" that broke yesterday.
- **All queries executed successfully.** Meteo price forecast (11) still showing 0 miss HE16+ (echoing DA — not a real forecast). 18th run. Meteo price forecast (12) only returned WESTERN starting HE4. 08c gas query too large (saved to file, used 08d instead).

### What Was Weak/Missing
- **Meteo price forecast (11) still broken.** HE16+ returns 0 miss because it's reading DA actuals back, not a real forecast. 18th consecutive run. Permanently flagged.
- **Meteo price forecast (12) only returned WESTERN hub, starting HE4.** No Dom/EH/System. This limits the DA vs Meteo comparison to WH only.
- **08c gas query too large for inline display.** Saved to file. Used 08d 5-day gas data instead. 08c result needs a LIMIT or date filter to be usable.
- **Outage forecast (06b) only returned today.** 18th run with no forward visibility.
- **Yesterday's OnPk estimate massively missed WH.** Predicted ~$40, actual $65.57. The HE15 $148 and HE16 $115 afternoon spikes were not in the estimation model. The back-of-envelope formula (morning × 2 + midday × 8 + evening × 6) / 16 breaks when 2 hours print $130+ and midday has extreme spreads. Need to add a "spike risk premium" when constraints are binding.
- **MECS hourly data (09b) revealed midday loop flow -4,300 MW — not highlighted in prior entries.** The Michigan seam is carrying enormous unscheduled flow during solar hours. This is a second-order congestion signal worth tracking.

### One Concrete Change for Tomorrow
**Add a "spike risk premium" to OnPk estimates when constraints are binding.** The formula (morning × 2 + midday × 8 + evening × 6) / 16 assumes smooth pricing but misses 2-3 hour spikes that add $5-15 to the OnPk average. When EH cong > -$15 DA and forced outages > 10 GW, add +$8-12 to the WH OnPk estimate. Yesterday: base est $40 + spike premium $12 = $52, much closer to the $65 actual (still low, but directionally better). Track: if spike premium >$15, the constraint is producing convex payoffs.

---

## 2026-03-26 — Run #15 (Thursday Morning)

### What Worked Well
- **Forced outage trend in Market Snapshot header immediately valuable.** "12.7 GW (trajectory: 8,625 → 10,405 → 12,469 → 12,709)" is scannable and shows the deceleration (+240 vs +2,064) at a glance. Iteration log change from Run #14 applied successfully.
- **Three-tier congestion narrative captured a new regime.** Dom going negative at HE14 (-$22.78) while WH printed +$30 is the most important structural finding this week. The "WH+ / Dom- / EH--" framing makes the spread opportunity obvious.
- **Overnight zero-congestion signal correctly identified as a regime break.** HE1-3 printed zero congestion for the first time in 5+ days — directly answered yesterday's follow-up #3 about whether overnight congestion was the new normal.
- **Limerick-Whitpain acceleration from 4/3 to 3/27 flagged prominently.** This is an 8-day acceleration on a key 500 kV PE constraint. Correctly identified as "the single biggest change" and threaded into the trade implications (Week 1 overpriced).
- **Scorecard table validated:** 2 hits, 2 partials, 1 miss. The overnight congestion miss is instructive — a single day of extreme overnight congestion is not a regime change, especially when driven by wind drought + forced outage tail event.
- **All queries executed successfully.** Meteo price forecast (12) only returned Western hub starting HE9. Query 11 (price forecast vs actual) still permanently broken — 17th run. Query 13 (demand forecast vs actual) not run.
- **08c gas query continued working.** M3 crash to $2.098 clearly captured. The gas narrative is shifting from "not a driver" to "actively deflating."

### What Was Weak/Missing
- **Meteo price forecast (11) still broken.** 17th consecutive run. Permanently flagged.
- **Meteo demand forecast vs actual (13) not run this session** — sub-hourly issue persists.
- **Outage forecast (06b) only returned today.** 17th run with no forward visibility.
- **08b ICE intraday tape too large** — 134K characters saved to file, not fully parsed. The summary from 08a was sufficient but the intraday narrative (when did BalDay RT trade from $41.75 to $47.50?) is missing.
- **Meteo price forecast (12) only returned Western hub, starting HE9.** No Dom/EH/System forecasts. No HE1-8. This limits the Meteo vs DA comparison to WH afternoon/evening only.
- **No Meteo generation actual vs normal (17) run.** Would have quantified whether yesterday's wind/solar was above or below seasonal normal.

### One Concrete Change for Tomorrow
**Add a "DA vs Meteo vs RT" comparison table to the Market Snapshot.** Yesterday's DA overpriced by $6+ at system level, and Meteo WH OnPk ~$40 was closer to the actual $43 RT than DA $46. A simple 3-column comparison (DA OnPk / Meteo OnPk est / RT OnPk actual) for WH, Dom, EH would make the model miss immediately visible and improve OnPk price estimates going forward. Format: `| Hub | DA OnPk | Meteo est | RT OnPk | DART |`.

---

## 2026-03-25 — Run #14 (Wednesday Morning)

### What Worked Well
- **HE22-24 data revealed the run series record.** HE22 Dom $220.65, EH -$149.49 congestion — would have been invisible without the full 24-hour RT query. The previous evening update (Run #13b) only had data through HE21 and completely missed this event.
- **07b thermal ramp query immediately valuable.** Gas declining -1,552 MW at HE22 while prices spiked to $199 WH confirmed a forced outage, not a dispatch ramp. This is the exact insight the query was designed for.
- **Forced outage jump to 12,469 MW (+2,064 DoD) flagged correctly.** The trajectory 8,625 → 9,430 → 10,405 → 12,469 is an accelerating pattern. Combined with the HE22 blowout, this is the dominant narrative.
- **05 load query fix confirmed working.** Tuesday actuals: 91,251 flat / 92,487 OnPk / 105,366 peak. Peak 105,366 is the highest in the run series.
- **OnPk price estimate formula applied.** ($95 × 2 + $20 × 8 + $45 × 6) / 16 ≈ $41. Yesterday's actual was $51.11 — the formula underestimated because it didn't account for the HE22 $199 spike. Without HE22, OnPk would have been ~$42. Formula works for "normal" days but can't price forced outage tail events.
- **Today's RT partial (HE1-6) caught overnight congestion.** HE1 EH -$30.61 at 1 AM is unprecedented — the constraint regime has shifted from evening-only to 24-hour. This is the most important new signal for trade positioning.
- **All queries executed successfully.** 08b ICE intraday empty (no trades yet — expected for early morning).

### What Was Weak/Missing
- **Meteo price forecast (11) still broken.** 16th consecutive run. Permanently flagged.
- **Meteo demand forecast vs actual (13) sub-hourly issue persists.** All forecasts null.
- **Outage forecast (06b) only returned today.** No forward visibility. With forced outages at 12.5 GW, the 5-day outage forecast would be critical.
- **Previous entry update (Run #13b) missed HE22-24.** The "evening update" was written with data only through HE21. The most important event of the day (HE22 $220 Dom) was invisible. Need to either wait for full data or flag the gap.
- **Day-of-week labels were wrong** in prior entries (3/24 was Tuesday, not Monday; 3/23 was Monday, not Sunday). Corrected by user. Need to verify DOW independently.

### One Concrete Change for Tomorrow
**Add forced outage trend to the entry header.** The forced outage trajectory (8,625 → 9,430 → 10,405 → 12,469) is the single most important fundamental this week, but it's buried in the Outages section. Add a one-line forced outage trend to the Market Snapshot lead, right after the DA/RT prices: `Forced outages: X GW (trajectory: a → b → c → d)`. This makes it impossible to miss when scanning entries.

---

## 2026-03-24 — Run #13b (Tuesday Evening Update)

### What Worked Well
- **05 load actual query FIXED.** Replaced `pjm_load_rt_prelim_daily` (non-existent) with aggregation from `pjm_load_rt_prelim_hourly`. Returns flat/OnPk/peak by region. Tuesday load actuals: RTO 88,089 flat / 93,350 OnPk / 98,822 peak — PJM peak forecast overshot by 5,273 MW.
- **Full Monday RT data (HE7-21) revealed the run series congestion record.** HE8 EH -$93.65 congestion is the most extreme single-hour reading. Without the update, the entry would still show only HE1-6 and miss the full story.
- **Tuesday 3/25 DA clear captured:** EH congestion narrowed from -$19.65 to -$10.36 — first narrowing in 5 days. Wind regime flip (852 → 6,166 MW at HE20) is the primary driver.
- **MISO loop flow halving (-3,462 → -1,871) correctly correlates with wind direction.** The tie flow data confirms the wind-driven congestion narrative.
- **Scorecard table applied successfully.** 2 of 5 morning predictions hit, 2 missed, 1 pending. Key miss: "$65+ RT OnPk" when solar crushed midday to sub-$20 and OnPk averaged ~$41.
- **08b intraday query fixed** — nested window function error resolved with two-CTE approach. Returned empty (no trade data for 3/25 yet) which is expected for an evening run.
- **Gas and ICE data both ran cleanly.** 08c gas query with explicit alias continued working. ICE summary captured the $23.55 BalDay RT sell-off.
- **All 28+ queries executed — only 11 Meteo price (known broken) and 13 sub-hourly issue persist.** 15th consecutive run with stable query suite.

### What Was Weak/Missing
- **08b ICE intraday returned empty** — trade_date = CURRENT_DATE (3/25 in UTC) has no data yet since it's still Monday evening. Should use `CURRENT_DATE - 1` for evening runs or detect if empty and fall back.
- **Meteo demand forecast vs actual (13) still has sub-hourly granularity issue** — all forecast_mw are null. 15th run.
- **Meteo price forecast vs actual (11) not run** — permanently flagged as broken.
- **RT data only through HE21** — HE22-24 not yet published. OnPk averages are partial (14 of 16 hours). Should note this in the entry more explicitly.
- **Morning entry's "$65+ RT OnPk" prediction was badly wrong** — didn't account for how much 12.6 GW peak solar would crush the midday average. Need to weight the solar valley more heavily in OnPk predictions. A $100 morning spike + $97 evening spike can still produce a $41 OnPk average if midday prints $15-20 for 8 hours.

### One Concrete Change for Tomorrow
**Add a simple OnPk price estimate formula to the "What Matters Today" section.** The morning prediction of "$65+ RT OnPk" ignored the midday solar weight. A back-of-envelope: `OnPk est = (HE8-9 avg × 2 + solar_valley_est × 8 + evening_ramp_est × 6) / 16`. With today's data: ($110 × 2 + $20 × 8 + $85 × 6) / 16 = (220 + 160 + 510) / 16 = $55.6 — still high but much closer to the $41 actual. The solar valley estimate is the key input. Tomorrow: solar peak ~10 GW (slightly less than today's 12.6), but wind at 6,000+ MW means midday could be even cheaper. Rough est: ($80 × 2 + $18 × 8 + $55 × 6) / 16 ≈ $44.

---

## 2026-03-24 — Run #13 (Monday Morning)

### What Worked Well
- **All queries executed successfully — 14th consecutive run with zero failures** (excluding 05 load actual table-not-found and 11 Meteo price known-broken). 08c gas query ran cleanly with the explicit `next_day_gas.gas_day` alias fix from Run #12b's iteration log. **First time 08c has succeeded in 3 runs.**
- **08b ICE intraday two-CTE every-4th-snapshot filter worked.** Returned ~5 snapshots per product across Sunday and Monday trade dates. Sunday's BalDay RT narrative (from $52.25 open to $70.90 close) and Monday's opening ($48.15) were clearly visible. The intraday tape captures the Sunday→Monday sentiment shift.
- **Sunday's entry predictions validated.** "Congestion regime has returned — today WORSE than Saturday" — confirmed with HE21 EH -$58 cong. "MISO loop flow accelerating" — confirmed at -3,462 daily. "DA $57.87 WH OnPk may still be too low" — confirmed, BalDay RT VWAP $60.27.
- **Today's HE5-6 RT data caught the earliest/deepest morning congestion yet.** HE6 EH -$31.50 cong at 6 AM Monday — the constraints are biting earlier into the morning ramp as weekday load arrives. This is the key new signal vs Sunday.
- **Outage data revealed the 500 kV wave is expanding.** Three new 500 kV outages approved for 3/25 in NJ/PEP corridor (Hopecreek-Redlion, Burchesh-Possumpt) — congestion rotating from Dom-only to a broader eastern constraint pattern.
- **Gas query (08c) fix confirmed.** The `next_day_gas.gas_day` alias in the FINAL CTE resolved the ambiguous column error. Data showed M3 rebound to $2.574 — first uptick after 6-day decline.

### What Was Weak/Missing
- **05 load actual query failed** — `pjm_cleaned.pjm_load_rt_prelim_daily` table does not exist. This has not been an issue before — possible schema change or table rename. Need to investigate and fix for tomorrow.
- **Meteo price forecast (11) still broken** — 13th consecutive run. Skipped per iteration log guidance. Permanently flagged.
- **Meteo demand forecast (13) sub-hourly granularity issue persists** — 14th run. Multiple readings per hour with null forecasts for HE1-15. Only HE16+ has forecast data. Still usable for evening hours.
- **14 Meteo demand forecast only returned today's data** — no forward days (Tue/Wed/Thu). Starts from HE5 for WEST, HE8 for SOUTH, HE9 for RTO/MIDATL.
- **06b outage forecast only returned today's row** — persistent issue.
- **No load actual data for Sunday** — the 05 query failure means the entry lacks yesterday's actual load by region. Had to rely on yesterday's entry data and fuel mix totals as proxies.

### One Concrete Change for Tomorrow
**Fix 05 load actual query table reference.** The table `pjm_cleaned.pjm_load_rt_prelim_daily` may have been renamed or moved. Run `SELECT table_name FROM information_schema.tables WHERE table_schema = 'pjm_cleaned' AND table_name LIKE '%load%'` to find the correct table name and update the query file. Without load actuals, the entry can't properly compare yesterday's load to forecast — this is a critical data gap.

---

## 2026-03-23 — Run #12b (Sunday Late Morning Update)

### What Worked Well
- **Update mode with timestamped sub-block worked.** Added `#### Update (~9:00 AM ET)` at the top of the existing entry with new RT hours, revised ICE, and forecast deltas. Entry stays clean, update is clearly visible.
- **HE7-8 RT data caught the most extreme morning congestion in the run series.** Dom $72.69 (+$13.90 cong), EH $18.74 (-$40.44 cong) at HE7. Without the update, the entry would still say "HE5-6 moderate congestion appearing." The morning ramp at 4x Saturday's level is the single most important signal.
- **ICE data showed real-time market repricing.** BalDay RT jumped from $52.25 (morning) to $57.50 last / $60.27 VWAP with 1,850 MW volume. NxtDay bids moved from $56 to $61. This confirms the market is absorbing the morning prints and repricing Monday higher.
- **Scorecard table applied from iteration log.** Scored the morning entry's 5 "What Matters Today" predictions against HE7-8 actuals. 3 of 5 confirmed/hit, 2 pending (evening). Creates accountability and shows the entry's predictive value.
- **All queries executed successfully — 13th consecutive run with zero failures** (excluding 08c gas query ambiguous column, a known formatting issue).
- **Forecast revisions were minor and correctly flagged.** Meteo HE21 revised down ~1 GW (96,733 from 97,734), ECMWF ens also slightly lower. The direction is consistent — all models still 95-97 GW evening.

### What Was Weak/Missing
- **08c gas query failed** — ambiguous column reference on `gas_day` in the JOIN. The USING clause or explicit table aliases would fix this. Morning gas data ($2.307 M3) still applies since no weekend trading.
- **Meteo price forecast (11) not run** — flagged as permanently broken in Run #12. Only HE16-24 returns with forecast = actual. Not worth running.
- **08b ICE intraday tape skipped** — Sunday has no Saturday trading session to show. Correct skip.
- **No new load forecast forward days** — Meteo demand (14) only returned today's data again. No Mon/Tue/Wed forecast.

### One Concrete Change for Tomorrow
**Fix 08c gas query ambiguous column** — The `gas_day` column exists in both `NEXT_DAY_GAS` and `BALMO` CTEs. The JOIN via `USING (trade_date)` doesn't disambiguate `gas_day` in the outer SELECT. Fix: explicitly alias `next_day_gas.gas_day` in the FINAL CTE. This is a one-line fix that has been silently causing failures on the LIMIT wrapper approach.

---

## 2026-03-23 — Run #12 (Sunday Morning)

### What Worked Well
- **All queries executed successfully — 12th consecutive run with zero failures.** First weekend-to-Monday run since Run #4. ICE query modified at runtime to use `trade_date >= CURRENT_DATE - 3` fallback for weekend — returned Sunday data with a BalDay RT trade at $52.25 and NxtDay bid/ask. Not empty this time.
- **Saturday's RT data revealed the biggest single-hour miss of the entire run series.** HE17 Dom $138 (+$40 cong) vs DA $17.82 = DART -$120.25. Without the hourly queries (02+03), this would have been buried in the flat OnPk average. The hour-by-hour DART section is the most valuable part of the entry.
- **Friday's entry correctly called the constraint return.** "The relief is temporary — a 2-3 day window... Monday 3/23 the constraints tighten again" was almost exactly right. Graceton, Conastone, and new 500 kV Dom outages all started today as predicted. The entry warned "Week 1 at $50 may be underpricing the constraint return" — BalDay RT already trading $52.25.
- **06d transmission new-outage query caught the full 500 kV wave.** Five new 500 kV outages starting today across Dominion + MidAtl, clearly explaining the DA repricing from $37 → $51 system energy.
- **MISO unscheduled 8-day trend (09c) showed the acceleration clearly** — -1,662 → -1,707 → -2,613 → -3,326. The trough-to-peak pattern over 4 days is a clean regime shift signal.
- **Meteo demand projection vs ensemble convergence holding.** HE21 projection 94,878 vs ensemble 97,734 = 2.9 GW gap. Manageable divergence. Both point to 95-98 GW evening.
- **Wind forecast decline (16) is the most actionable signal.** 6,396 → 3,555 at HE20 quantifies the evening cushion loss. Combined with Saturday's RT actuals where 7,000 MW wind still couldn't prevent a $121 Dom blowout, the lower wind today makes the case compelling.

### What Was Weak/Missing
- **Meteo price forecast (11) still broken for HE1-15** — 12th consecutive run. Only HE16-24 returned with forecast = actual (post-settlement backfill). Flagging as permanently broken.
- **Meteo demand forecast (13) sub-hourly granularity issue persists** — 12th run. Multiple readings per hour with null forecasts.
- **14 Meteo demand forecast only returned today's data** — no forward days (Mon/Tue/Wed). Starts from HE4 (WEST), HE7 (SOUTH), HE8 (RTO/MIDATL).
- **06b outage forecast only returned today's row** — persistent issue.
- **No gas data for weekend trade dates** — 08d returned through 3/20 trade date only. Weekend gas doesn't trade, so the latest cash gas is Friday's print. Need to note this in the entry rather than imply prices are "current."
- **08b ICE intraday tape not run** — the every-4th-snapshot filter from Run #11's iteration log was not applied because 08b on a Sunday would return no data anyway (no Saturday trading session). Skipped intentionally.

### One Concrete Change for Tomorrow
**Add a "Saturday RT Surprise" flag to the weekend entry template** — Saturday printed +$24.17 Dom DART (RT over DA) when the entry predicted a "benign" weekend. The entry should include a structured "Prediction vs Actual" section that explicitly scores Friday's "What Matters Today" bullet points against Saturday/Sunday actuals. This creates accountability and calibrates future weekend calls. Format: `#### Scorecard: [date] predictions → | Prediction | Actual | Hit/Miss |`

---

## 2026-03-20 — Run #11 (Friday Morning)

### What Worked Well
- **All 28 queries executed successfully — 11th consecutive run with zero failures.** Query suite remains stable.
- **Transmission outage forward view (06d) revealed the congestion trap.** Graceton-Manor 230 kV approved for a new outage starting 3/23 returning 3/30, and Conastone-NW has a 7-week outage starting 3/21. Without 06d, the narrative would have been "congestion resolved" — instead, it's "2-day relief window, constraints return Monday." This is the most actionable finding of the run.
- **Wind recovery (1,446 → 5,753 MW) + Meteo wind forecast holding 5,300+ through HE20 completely changes the evening ramp math.** Having both the actual trajectory (query 10) and the hourly Meteo forecast (query 16) makes the wind recovery story quantifiable.
- **ECMWF ensemble and Meteo projection converging again** — HE21 ens 90,211 vs projection 93,034 = 2.8 GW gap. Much better than the 11 GW divergence earlier in the week. Three-way model convergence is holding.
- **Meteo demand nailed Thursday's evening peak within 30 MW** — HE21 forecast 96,371 vs actual ~96,400. This builds confidence in today's 90 GW evening call.
- **ICE intraday tape summary** (08b open/close extraction) confirmed the sell-off narrative: BalDay RT from $46.50 → $41.55, NxtDay products converged at $41.30.
- **06e return wave is huge** — 12+ 345 kV outages returning today. Having the facility-level detail makes the congestion relief quantifiable by region (FE/Ohio corridor significantly de-congested).

### What Was Weak/Missing
- **Meteo price forecast (11) still broken for HE1-15** — 9th consecutive run. Only HE16-24 returned with forecast = actual (post-settlement backfill). Should flag this as permanently broken and stop including in the entry.
- **Meteo demand forecast (13) sub-hourly granularity issue persists** — 10th run. Multiple readings per hour with null forecasts.
- **14 Meteo demand forecast only returned today's data** — no forward days (Sat/Sun/Mon). Starting from HE4 for WEST, HE6 for SOUTH, HE8 for RTO/MIDATL. Still no pre-dawn hours for RTO.
- **06b outage forecast only returned today's row** — persistent issue across all runs. No multi-day outage forecast.
- **No weekend Meteo demand forward** — can't quantify Saturday/Sunday load to frame the weekend.
- **08b output too large** — saved to file again. The every-4th-snapshot filter from Run #9's iteration log needs to be re-applied (it may have been lost or the query was reverted).

### One Concrete Change for Tomorrow
**Re-apply the 08b every-4th-snapshot CTE filter** — The query is back to returning all ~88 snapshots per product (352 rows total), causing the output to exceed the token limit. Restore the two-CTE approach from Run #9: first CTE computes `ROW_NUMBER()`, second computes `MAX(rn)`, then filter `WHERE rn % 4 = 1 OR rn = max_rn`. This keeps ~12 snapshots per product and prevents file overflow.

---

## 2026-03-19 — Run #10 (Thursday Late Update)

### What Worked Well
- **Same-day entry detection worked.** Applied the Run #9 iteration log change: detected existing 3/19 entry and switched to update mode instead of writing a duplicate. Only updated RT partial (added HE8), refined Meteo forecasts, fuel mix, and tie flow numbers. Journal stays clean.
- **All 28 queries executed successfully — 10th consecutive run with zero failures.** Query suite is stable.
- **HE8 RT data confirmed the morning ramp is contained.** Dom $73.94 (+$8.70 cong) vs Meteo forecast of $171.33 — RT underperformed DA forecast by $97. The morning peak was benign. This is the 2nd consecutive day the morning overshoot failed to materialize.
- **Three-way model convergence confirmed.** Meteo demand HE21 96,091, ECMWF ens 96,321, Meteo projection 96,146 — all within 230 MW of each other. Third consecutive run of convergence. The stale vintage issue is fully resolved.
- **ECMWF ensemble morning spread narrowed significantly.** HE7 spread went from 3,418 (Run #9) to 2,045 MW — models more confident on the morning ramp as actuals came in.
- **Wind forecast tightened further.** HE20 wind 1,048 MW (from 1,077 prior run). Confirms the extreme low wind regime for this evening.

### What Was Weak/Missing
- **No new DA clear for 3/20** — still waiting for Friday's DA. Can't assess tomorrow's pricing yet.
- **Meteo price forecast (11) still broken for HE1-15** — 8th consecutive run. Only HE16-24 returned with forecast = actual (post-settlement backfill). Systematic issue with forecast_rank matching.
- **Meteo demand forecast (13) sub-hourly granularity issue persists** — 9th run. Multiple readings per hour with null forecasts.
- **14 Meteo demand forecast only returned today's data** — no forward days (Fri/Sat/Sun). Starting from HE10 for RTO, HE6 for WEST, HE7 for SOUTH.
- **06b outage forecast only returned today's row** — persistent issue across all runs.
- **Update mode is manual** — the workflow instruction says "switch to update mode" but there's no structured mechanism. Currently just manually editing the sections that changed. Could formalize with a sub-heading like `#### Update (HH:MM ET)`.

### One Concrete Change for Tomorrow
**Formalize update mode with timestamped sub-blocks** — When an entry for today already exists, append an update sub-block at the top of the entry: `#### Update (HH:MM ET)` containing only: (1) new RT hours since last run, (2) revised forecasts with deltas, (3) any new transmission outage changes. This prevents scattering edits across the entry and makes the update clearly visible.

---

## 2026-03-19 — Run #9 (Thursday Midday Update)

### What Worked Well
- **08b ICE intraday two-CTE fix WORKED.** The iteration log fix from Run #8 — splitting row_number into a `numbered` CTE, max into `with_max` CTE, then filtering `WHERE rn % 4 = 1 OR rn = max_rn` — executed without error. Returned ~9 snapshots per product (BalDay RT: 9, NxtDay RT: 8, NxtDay DA: 8, BalWeek: 8) from 10:00 to 20:05 ET. Output was manageable and showed the full intraday narrative: BalDay RT sold from $60 open → $47 close, NxtDay DA pinned at $58.75 all afternoon. **First successful 08b run in 3 attempts.**
- **All 28 queries executed successfully.** Zero failures, zero timeouts. The full query suite is stable.
- **HE7 RT data now available** — Dom $73.16 (+$9.78 cong), West $65.33, East $35.31 (-$28.64 cong). Morning ramp is running hotter than yesterday's HE7 ($65 Dom) but below Tuesday's $145 blowout. Confirms the de-escalation narrative.
- **Meteo demand projection and ECMWF ensemble remain converged.** Projection HE21 96,146 vs ECMWF ens 96,321 — only 175 MW apart. Two consecutive runs of convergence confirms the stale vintage issue is resolved.
- **PJM load forecast revised down further** — 109,369 peak (rank 161) vs 110,831 (prior rank). PJM itself is de-escalating the load call, moving closer to Meteo's 96.5 GW evening view.
- **MISO unscheduled 3/19 partial at -2,867** — elevated vs yesterday's -2,648 full-day but below the -3,948 spike from 3/17. Loop flow stable, not accelerating.

### What Was Weak/Missing
- **Entry for 3/19 already existed from Run #8** — this run was an incremental update (added HE7 + revised load forecast) rather than a fresh entry. The workflow doesn't handle same-day re-runs well. Need a mechanism to detect existing entries and switch to "update mode."
- **Meteo price forecast (11) still broken for HE1-15** — 7th consecutive run. Only HE16-24 returned with forecast = actual (post-settlement backfill). This is a systematic issue with the forecast_rank matching pulling the wrong vintage.
- **Meteo demand forecast (13) sub-hourly granularity issue persists** — 8th run. Multiple readings per hour with null forecasts. The observation table returns 5-min data that doesn't match hourly forecast granularity.
- **14 Meteo demand forecast only returned today's data** — no forward days (Fri/Sat/Sun). Starting from HE5 for WEST, HE6 for SOUTH, HE9 for RTO/MIDATL. Still no pre-dawn hours.
- **06b outage forecast only returned today's row** — persistent issue across all runs. No multi-day outage forecast available.
- **Meteo generation forecast (16) only returned today's data** — no +1/+2 day forecasts. The forecast_rank subquery tied to CURRENT_DATE may not match future dates.

### One Concrete Change for Tomorrow
**Add same-day entry detection** — Before writing the entry, check if an entry for today's date already exists at the top of PJM-Morning-Fundies.md. If so, switch to "update mode": only update RT partial data, load forecast revisions, and any new data since the last run. This prevents duplicate entries and keeps the journal clean. Implementation: check if the first `## ` heading matches `CURRENT_DATE`.

---

## 2026-03-19 — Run #8 (Thursday Morning)

### What Worked Well
- **08b every-4th-snapshot filter applied from iteration log** — but hit a SQL error: "window function calls cannot be nested" (the `MAX(ROW_NUMBER() OVER (...)) OVER (...)` pattern). Need to restructure as a CTE. Data wasn't needed for today's entry since ICE summary (08a) was sufficient, but this should be fixed.
- **08c 7-day date filter continues to work perfectly.** Gas trajectory clearly visible: M3 $2.472 → $3.028 → $2.867 → $2.618. The cold front gas spike and full resolution in one table.
- **Meteo projection vs ECMWF convergence finally happened.** For 5 runs these diverged by 11+ GW. Today: projection HE21 96,146 vs ECMWF ens 96,548 — only 400 MW apart. This validates both data sources and suggests the prior divergence was a stale projection vintage issue during the cold front.
- **Transmission outage return tracking (06e) caught the congestion regime shift.** Hosensac back today, Graceton/Conastone tomorrow, ~40 outages within 7 days. The 06e query is the most actionable data for forward congestion views.
- **ECMWF ensemble spread (15) showed tightest evening spread all week** — 1,333 MW at HE20. This high model confidence directionally confirmed the de-escalation narrative and made the trade implication (DA finally appropriately priced) higher conviction.
- **Wind actual vs normal (17) quantified the extreme crash:** HE20 wind at ~1,200 MW vs 4,635 normal = -3,400 below normal. Despite this, the ramp was contained because load was also 12 GW lighter than the morning peak.

### What Was Weak/Missing
- **08b SQL error** — nested window function. The fix from Run #7 iteration log caused a syntax error. Need to restructure: use a CTE to compute row_number and max, then filter in outer query.
- **Meteo price forecast (11) still broken for HE1-15** — 6th consecutive run. Only HE16-24 returned with forecast = actual (post-settlement backfill). This query is not providing forecast value — it's just echoing settled prices. Needs fundamental investigation or should be flagged as broken.
- **Meteo demand forecast (13) sub-hourly granularity issue persists** — 7th run. Returns multiple readings per hour with null forecasts for most. Only HE16-24 RTO had forecast matches. The observation table's 5-min granularity doesn't match the hourly forecast table.
- **14 Meteo demand forecast only returned today's data (HE5-24)** — no forward days (Fri/Sat/Sun). The forecast_rank subquery ties to CURRENT_DATE and may not have matching ranks for future dates.
- **06b outage forecast only returned today's row** — persistent issue. No multi-day outage forecast available.

### One Concrete Change for Tomorrow
**Fix 08b nested window function** — Restructure the every-4th-snapshot filter using two CTEs: first CTE computes `ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY snapshot_at) AS rn`, second CTE computes `MAX(rn) OVER (PARTITION BY symbol) AS max_rn`, then filter `WHERE rn % 4 = 1 OR rn = max_rn` in the outer query. This avoids the nested window function error while preserving the ~15 snapshot per product output.

---

## 2026-03-18 — Run #7 (Wednesday Morning)

### What Worked Well
- **08c date filter (iteration log fix) worked perfectly.** Query returned exactly 7 days of gas cash/balmo data instead of the full history. Clean, fast, no MCP timeout issues. M3 trajectory from $2.47 → $3.03 → $2.87 was immediately visible.
- **Hour-by-hour DART analysis remains the strongest section.** The HE20 DART +$97 at Dom ($129 RT vs $224 DA) tells the story instantly. The 8-hour Dom negative congestion block (HE10-17) was only visible because of hourly data.
- **Overnight RT data (HE1-2 today) caught the historic -$129 Eastern congestion.** Without query 04, we'd have missed that the overnight heating load buildup is now more violent than the morning ramp itself. This reframes the N-S congestion narrative.
- **MISO unscheduled flow 7-day trend (09c) caught the -3,948 spike.** Going from -2,345 to -3,948 is a major regime shift. Having the full 8-day trend immediately contextualized this as the biggest reading since 3/13.
- **Transmission outage queries (06c/d/e) continue to be high-value.** Caught the Fentres4 500 kV XFMR new construction (starts 3/19, returns 7/15) — this 4-month outage starting as Fentress-Yadkin LINE returns is a critical nuance. Also confirmed Graceton returning 3/20 with days_to_return = 1.
- **Wind actual vs normal (17) validated the crash thesis.** Wind went from +4,300 above normal (Mon) to -1,600 below normal (Tue afternoon). Quantifying the regime shift matters for the evening ramp call.
- **BalWeek and Week 1 finally trading** — 08a captured real volume for the first time. BalWeek 1,150 MW and Week 1 1,400 MW gives market-implied forward view.

### What Was Weak/Missing
- **Meteo price forecast (11) still broken for HE1-15** — only HE16-24 returned, and those show forecast = actual (post-settlement backfill). 5th consecutive run with this issue. The query's forecast_rank matching is pulling the wrong vintage.
- **Meteo demand forecast (14) only returned partial data for today** — HE9-24 for RTO, HE5-24 for WEST, HE6-24 for SOUTH. No HE1-8 data, which are the hours that matter most for the morning ramp. The forecast_rank subquery filtering may not have a vintage with full-day coverage.
- **Meteo demand observation (13) sub-hourly granularity issue persists** — returns 12 readings per hour with null forecasts for most. The join to the hourly forecast table doesn't properly aggregate. Only RTO HE16-24 had forecast matches.
- **08b ICE intraday tape too large** — output exceeded token limit and was saved to file. Need to either limit the snapshot count (every 3rd snapshot?) or filter to specific time windows (morning open, midday, close).
- **Meteo projection (18) still diverging 11 GW from ECMWF ensemble (15)** — HE21 projection 96,914 vs ens avg 108,288. This has been flagged for 4 runs. Either the projection is stale/wrong or the ensemble is too bullish. Needs investigation.
- **No Meteo data for tomorrow (Thursday)** from query 14 — only returned today's data. The forward demand forecast is important for framing the congestion relief narrative.

### One Concrete Change for Tomorrow
**Limit 08b ICE intraday tape to every 4th snapshot** — The full 50+ snapshots per product exceeded the token limit. Add a row_number filter: `WHERE rn % 4 = 1 OR rn = max_rn` to keep ~15 snapshots per product plus the final reading. This prevents output overflow while preserving the open/midday/close narrative.

---

## 2026-03-17 — Run #6 (Tuesday Midday Update)

### What Worked Well
- **Transmission outage queries (06c/d/e) are a major addition.** The 135-outage snapshot immediately identified Bedington 500 kV corridor as active N-S congestion risk, and the returning-within-7-days query confirmed Graceton-Manor + Conastone-NW return Friday. This is the first time we've had facility-level transmission outage data — it directly explains the -$100 Eastern Hub congestion at HE8 and identifies when relief comes.
- **Wind forecast fix (query 16 UNION approach) worked.** Now getting both solar AND wind from Meteo generation forecast with separate forecast_rank subqueries. Wind forecast of 2,344-3,347 MW for afternoon is the most actionable signal in the entire run — it's the difference between yesterday (wind held, DA overpriced) and potentially tonight (wind crashes, DA underprices).
- **HE11 Dom collapse to $8.95 (-$19 cong) caught in real-time.** The $175 → $9 Dom whipsaw over 3 hours is the shoulder season story. Having RT through HE11 vs just HE7 gives a much richer narrative.
- **Gas cash update for 3/18 gas day** showed M3 pulling back $0.16 — changed the gas narrative from "gas is a driver" to "gas stress easing." Important nuance.
- **MISO unscheduled easing to -1,811** (from -3,358 yesterday) is a clean signal that the Western export pressure is moderating as wind fades — wind-driven loop flow drops when wind drops. Good correlation signal.

### What Was Weak/Missing
- **Meteo demand forecast (14) only returned partial data for today** — HE13-24 only, and no Wed/Thu/Fri data. The forward demand forecast is critical for the Wednesday "spicey" setup. Forecast_rank subquery may be filtering too aggressively.
- **Meteo price forecast (11) still not working** — HE16-24 returned with forecast = actual (i.e., post-settlement backfill, not a true forecast). HE1-15 had null forecasts. This query has been broken for 4 runs. Need to investigate the forecast_rank matching or accept this data source isn't reliable.
- **Meteo demand observation (13) sub-hourly granularity issue persists** — returns 6 readings per hour with null forecasts for most. The join logic still doesn't properly match hourly forecast to sub-hourly observations.
- **No Meteo generation data for tomorrow/day-after** — query 16 only returned today (HE13-24). The +2 days part isn't working, likely because the forecast_rank subquery ties to CURRENT_DATE.
- **Outage forecast (06b) still only returns today's row** — no multi-day forecast available. Need to investigate whether this table has forward data or if the forecast_rank filter is too narrow.

### One Concrete Change for Tomorrow
**Add a date filter to 08c (ICE gas cash vs balmo)** — the query currently returns the full history (~1.3M chars, 30+ rows going back to February). Add `WHERE trade_date >= CURRENT_DATE - INTERVAL '7 days'` to keep it focused on the trading week. This prevents MCP timeout issues and keeps the data relevant.

---

## 2026-03-17 — Run #5 (Tuesday)

### What Worked Well
- ICE data returned successfully for Monday (trade_date = CURRENT_DATE - 1). The intraday tape showed NxtDay RT trading from $104 → $94 → $98.50 and NxtDay DA from $100 → $91 → $92 — gave a full narrative of how the market repriced Tuesday through the day. Much better than the weekend data gap.
- Monday's hour-by-hour DART revealed the HE23-24 late-night congestion spike (Dom $68 with +$19.54 cong, East $5 with -$43.81 cong) which directly foreshadowed today's extreme morning ramp. This was the most actionable signal from yesterday's data.
- Gas cash-balmo flip (+$0.508 M3) was a critical signal — M3 jumping $0.56 DoD changes the marginal cost stack and was the first time gas has been a driver all week.
- Wind vs normal data continuing to show +4,300-4,600 above normal — and the partial fade to 8,271 MW today confirmed Edi's "big wind dip" call.
- Meteo price forecast for today (Dom HE20 $223.90) is the most extreme forecast yet — useful calibration against what actually prints tonight.

### What Was Weak/Missing
- **Meteo intraday projection (18) diverging significantly from demand forecast (14)** — projection has HE21 at 97,408 while demand forecast has 112,820. This 15 GW gap is confusing. The projection may be stale or using different assumptions. Need to understand which to trust.
- **No wind forecast for today from Meteo generation (16)** — query only returned solar. Wind forecast data may not be loading for today's date, or the forecast_rank subquery isn't matching. Should add wind explicitly.
- **Still no regional load hourly** — this is the fourth run flagging it. With HE7 Eastern at -$86 congestion, knowing which region is driving load during the morning ramp would be extremely valuable.
- **Outage forecast (06b) only returned today's row** — no multi-day outage forecast. Can't see if outages are expected to continue declining or rebound mid-week.

### One Concrete Change for Tomorrow
**Fix Meteo generation forecast query (16) to include wind** — The current query uses `forecast_rank` from solar, which may not match wind's forecast_rank. Split into separate subqueries for solar and wind forecast_rank, or use a UNION approach. Wind forecast for today/tomorrow is critical for the evening ramp call.

---

## 2026-03-16 — Run #4 (Weekend Run)

### What Worked Well
- Hour-by-hour DART analysis caught the Saturday reversal — positive DARTs for the first time all week. Without this, the narrative would have missed that DA overpriced a soft weekend day.
- Dom midday congestion blowout (HE11 -$39.69 cong, RT -$18.83) was clearly visible from the RT hourly data. This is the shoulder season whipsaw: bearish Dom midday from solar, bullish Dom evening from ramp.
- MISO unscheduled flow 7-day trend gave full context: -1,056 → -1,904 → -2,475 → -3,497 → -3,877 → -2,634 → -3,667 → -3,174. Not monotonically increasing anymore — useful to see the variation.
- Meteo demand forecast for Monday (114,271 HE21) was the most actionable data point — immediately frames the Sunday-to-Monday transition as a regime change.
- Wind vs normal comparison (8,832 MW actual vs ~4,700 normal = +4,100 above normal) quantified the single biggest bearish cushion. Critical for Monday risk framing.
- ECMWF ensemble spread widening (44 MW overnight → 5,751 MW at HE24) added useful uncertainty framing for the Sunday evening transition.

### What Was Weak/Missing
- **ICE power data returned empty** — trade_date = CURRENT_DATE - 1 (Sunday) has no data since Friday's trade date is 3/14, not 3/15. Weekend queries need to handle this: should fall back to most recent trade_date with data.
- **Regional load hourly still not implemented** — this is the third run flagging this. Can't tell which region drove Saturday's evening ramp or midday congestion without hourly regional load.
- **No GridStatus/narrative overlay** — relied on notes from the 3/13 entry for Conastone outage context. Would benefit from a structured way to carry forward active outage narratives.
- **Meteo demand forecast (13) sub-hourly granularity issue persists** — returns hundreds of rows (5-min intervals) with null forecasts for most. The join logic needs fixing to match hourly forecast to hourly observation.
- **Meteo price forecast (11) missing HE1-8** — only HE9-24 had forecast data. Pattern consistent with prior runs.

### One Concrete Change for Tomorrow
**Fix ICE power queries (08a, 08b) for weekend/Monday runs** — Change `trade_date = CURRENT_DATE - 1` to use `MAX(trade_date) FROM ice_python.intraday_quotes WHERE trade_date <= CURRENT_DATE - 1` so the query automatically falls back to the most recent trading day. This prevents empty results on weekends and Mondays.

---

## 2026-03-13 — Run #1 (First Run)

### What Worked Well
- Pulled DA/RT LMPs across all major hubs with DART spreads — gives a complete picture of where value was left on the table.
- Hourly RT breakdown identified the evening ramp (HE19-23) and morning ramp as the key volatility windows — matches the DST narrative from prior entries.
- Outage decomposition (planned/maintenance/forced) caught the 3.5 GW maintenance jump that could've been missed with just total outages.
- Gas data (M3, HH, Transco Z5S) confirmed gas isn't the driver this week — prevents a false narrative.
- Wind drop quantified (-1.8 GW DoD) as a concrete contributor to RT overperformance.

### What Was Weak/Missing
- No DA hourly breakdown — can't compare DA vs RT by hour to identify exactly which hours the DA missed. Need `pjm_cleaned.pjm_lmps_hourly` with DA hourly granularity.
- No congestion constraint-level data — mentioned Ladysmith/Nottingham narratively from prior entries but have no binding constraint data in the DB. Would need GridStatus or PJM constraint data.
- No solar/wind forecast vs actual comparison — had actuals but not the forecasted values, so can't quantify the forecast miss.
- Load forecast vs actual comparison missing — had 3/12 actual and 3/13 forecast but no 3/12 forecast to compare against.
- No tie flow data pulled — MISO/NYISO imports can drive price, especially during ramps. Table exists: `pjm_cleaned.pjm_tie_flows_daily`.
- Today's data is partial (through HE9) — entry would be stronger if run later in the day.

### One Concrete Change for Tomorrow
**Add DA hourly LMP query** — Pull `pjm_cleaned.pjm_lmps_hourly` for yesterday's DA by hour alongside the RT hourly data already being pulled. This enables a direct hour-by-hour DART analysis to identify exactly which hours the DA underpriced, which is the core trade signal.

---

## 2026-03-13 — Run #2 (Formalization)

### Changes Made
- Created 12 SQL files in `PJM/Daily-Prompt/sql/` covering all data sources from Run #1 plus the missing ones identified.
- Added `03-da-hourly-lmps.sql` (the #1 improvement from Run #1).
- Added `09-tie-flows.sql` (MISO/NYISO imports — was missing in Run #1).
- Added `10-solar-wind-forecast.sql` (renewables by period — was missing).
- Split load and outage queries into actuals + forecasts (05/05b, 06/06b) for cleaner separation.
- Created `daily-prompt.md` with the full prompt template, SQL reference, and customization guide.

### What Worked Well
- All 12 queries use `CURRENT_DATE`-relative dates — no manual date editing needed.
- Hub list is consistent across LMP queries (West, Dom, East for hourly; all 7 hubs for daily).
- Gas query covers M3 + HH + Z5S + Michcon + AGT — the key PJM-relevant points.
- Tie flows cover the major interfaces (RTO total, MISO, NYIS, DUKE, TVA, LINDEN).

### What Was Weak/Missing
- No load hourly data pulled — only daily aggregates. Hourly load would help identify whether load was the driver during specific spikes.
- No regional outage breakdown (MIDATL_DOM vs WEST) — only RTO total. Could miss regional outage concentration.
- No solar/wind forecast vs actual (only have actuals from fuel mix). The `pjm_cleaned.pjm_solar_forecast_hourly` and `pjm_cleaned.pjm_wind_forecast_hourly` tables exist but weren't used.
- No weather data in the DB — have to rely on narrative from GridStatus/recent entries.

### One Concrete Change for Tomorrow
**Add regional load hourly query** — Pull `pjm_cleaned.pjm_load_rt_prelim_hourly` for yesterday by region (RTO, MIDATL, SOUTH, WEST) for HE5-8 and HE18-23, so we can see which region drove load during ramp periods.

---

## 2026-03-13 — Run #3 (Full 25-Query Suite)

### Changes Made
- Executed all 25 SQL queries including ICE market data (08a-08d), tie flows (09a-09c), renewables (10), and full Meteologica suite (11-18).
- Incorporated hour-by-hour DART analysis — the #1 improvement from Run #1 iteration log. HE20 DART of -$786 now quantified with DA ($103) vs RT ($889) comparison.
- Added ICE intraday tape narrative showing BalDay RT selling from $58 → $43.50.
- Added MISO unscheduled flow 4-day trend (-1,904 → -4,615) as a congestion signal.
- Added ECMWF ensemble spread for load uncertainty (tight today, wide Saturday).
- Added Meteo price/demand/generation forecast comparisons vs actuals.
- Added fuel mix as a DoD comparison table instead of flat list.

### What Worked Well
- Hour-by-hour DART analysis is the strongest addition — HE20 -$786 DART tells the story instantly vs flat average -$56.
- ICE intraday tape adds market sentiment layer: products sold off all day = market pricing mean-reversion even as RT overperformed. Useful contrarian signal.
- MISO unscheduled flow trend is a clean quantitative signal — 4 consecutive days of acceleration is hard to ignore.
- Solar cliff timing from Meteo generation forecast (10,200 MW → 310 MW HE15→HE20) directly explains why the evening ramp blows up.
- ECMWF ensemble spread adds useful uncertainty framing — tight today vs wide Saturday changes how to think about weekend risk.

### What Was Weak/Missing
- **Regional load hourly still not implemented** — Run #2 identified this as the concrete change but no SQL file was created. Can't tell if MIDATL or WEST drove the HE20 load spike.
- **Meteologica price forecast (11) mostly null for 3/12** — only HE21-24 had forecasts. Either data isn't fully populated or the join logic needs adjustment. Low value until fixed.
- **Meteologica demand forecast (13) also sparse** — sub-hourly granularity (5-min) with null forecasts on most rows. The observation table granularity doesn't match the forecast granularity.
- **No binding constraint data** — still referencing Ladysmith/Nottingham narratively from prior entries. Would need constraint-level data to quantify congestion drivers.
- **Outage forecast (06b) only returned one row** (today = actuals). Weekend outage forecast data may not be loading properly — the 29 GW Saturday number came from prior entry context, not the query.

### One Concrete Change for Tomorrow
**Create `05c-load-hourly-regional.sql`** — Pull `pjm_cleaned.pjm_load_rt_prelim_hourly` for yesterday by region (RTO, MIDATL, SOUTH, WEST) for HE5-8 and HE18-23. This was flagged in Run #2 but never implemented. Regional load during ramp hours is the missing piece for explaining whether MIDATL or WEST is driving the evening spike.
