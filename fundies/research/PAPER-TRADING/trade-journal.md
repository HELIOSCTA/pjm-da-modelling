# PJM Paper Trade Journal

Append-only journal for paper trades.

## Entry Template (copy per new trade)

```markdown
## TRADE_ID: PT-YYYYMMDD-HHMMSS
Timestamp (local):
Timestamp (UTC):
Market: PJM
Instrument/Contract:
Side: long|short
Entry:
Exit Target:
Stop (optional):
Thesis:
Outcome: open
Notes:
Status: open
```

## Update Template (copy for revisions)

```markdown
## UPDATE: PT-YYYYMMDD-HHMMSS
Timestamp (local):
Timestamp (UTC):
Event: UPDATE
Changed Field(s):
Reason:
Notes:
```

## Close Template (copy to close a trade)

```markdown
## CLOSE: PT-YYYYMMDD-HHMMSS
Timestamp (local):
Timestamp (UTC):
Outcome: win|loss|breakeven
P/L:
What worked:
What failed:
Notes:
Status: closed
```

---

## MISSED: PT-20260313-050000
Timestamp (local): 2026-03-13 05:00 ET
Timestamp (UTC): 2026-03-13T09:00:00Z
Market: PJM
Instrument/Contract: PJM RT
Side: long
Entry: $83
Exit Target: TBD (today's settlement)
Thesis: RT exploded overnight — clear signal to go long RT at open.
Outcome: TBD
Why Missed: Needed to be online before 5AM to place this trade.
Notes: Will update with actual settlement to compute would-have P/L.
Status: missed

## MISSED: PT-20260312-160000
Timestamp (local): 2026-03-12 16:00 ET
Timestamp (UTC): 2026-03-12T20:00:00Z
Market: PJM
Instrument/Contract: PJM RT
Side: short
Entry: ~$46 (selling into close before 4pm)
Exit (Actual): ~$120 (RT settled at HE20)
Thesis: Calm day — some traders would sell into close. Risk/reward was not there.
Outcome: would-loss
Would-P/L: -$74/MWh
Why Missed: Correctly identified risk/reward wasn't favorable. Good miss.
Notes: RT exploded at HE20. Traders who sold before 4pm at ~$46 would have lost huge.
Status: missed

---

## TRADE: PT-20260316-070000
Timestamp (local): 2026-03-16 07:00 ET
Timestamp (UTC): 2026-03-16T11:00:00Z
Market: PJM
Instrument/Contract: BalDay RT (PDP D0)
Side: long
Entry: $60
Exit Target: settlement (hedge, no specific target)
Stop: —
Thesis: Morning ramp hedge. Buying RT at $60 against Monday's 100+ GW load forecast with 45 GW outages. PJM has been under-committing morning ramps all week — HE8 printed $1,339 on 3/13. DA cleared at $54 today; if RT overperforms the morning ramp again, this trade captures the upside. Strong wind (9+ GW) is the risk to the downside.
Outcome: open
Notes: RT through HE6 has been soft ($19-22) but load ramps from HE7. Meteo price forecast has Dom HE8 at $73.63. This is a hedge, not a directional bet — protecting against the pattern of RT morning spikes that have persisted all week.
Status: open

## CLOSE: PT-20260316-070000
Timestamp (local): 2026-03-16 17:00 ET
Timestamp (UTC): 2026-03-16T21:00:00Z
Outcome: loss
P/L: -$19.39/MWh
Exit: $40.61 (RT Western Hub settlement)
What worked: Thesis was directionally right on the morning ramp — HE8 printed $48/$52 (W/D), above the soft HE1-6 prints. Identified the right risk window.
What failed: Bought at $60 but RT settled $40.61. Wind at 9,300+ MW all day was the killer — prevented any evening ramp blowout (HE20 only $52 West). The morning ramp overperformance wasn't enough to pull the flat average above $60. DA at $54 also overpriced — the entire day was DA > RT. Lesson: buying RT above DA on a day when wind is 2x seasonal normal is fighting the supply cushion.
Notes: RT flat avg was $35.49 West in the daily data, but ICE BalDay RT settled $40.61. The $60 entry was too high for a day with 9+ GW wind. Better entry would have been waiting for the midday selloff ($56-58 range on ICE) or sizing smaller as a pure evening option.
Status: closed

---

## MISSED: PT-20260316-150000
Timestamp (local): 2026-03-16 15:00 ET
Timestamp (UTC): 2026-03-16T19:00:00Z
Market: PJM
Instrument/Contract: NxtDay DA (PDA D1)
Side: long
Entry: $94.50
Exit (Actual): $102.78 (DA clear)
Thesis: PJM needs strong commitment for Tuesday — 114 GW Meteo load forecast, cold front deepening, 45+ GW outages. DA should clear well above where it was trading. Edi flagged Tuesday morning as "strong Graceton" with a wind dip into morning ramp. South load ripping all day today confirmed the demand setup.
Outcome: would-win
Would-P/L: +$8.28/MWh
Why Missed: Didn't pull the trigger on the lift.
Notes: DA cleared $102.78 vs $94.50 — the thesis was right. Strong commitment needed = strong DA clear. This is the kind of trade where the fundamental view was clear but execution didn't follow.
Status: missed

---

## TRADE: PT-20260317-121500
Timestamp (local): 2026-03-17 12:15 ET
Timestamp (UTC): 2026-03-17T16:15:00Z
Market: PJM
Instrument/Contract: BalDay RT (PDP D0)
Side: long
Entry: $80
Exit Target: settlement (evening ramp play)
Stop: —
Thesis: Evening ramp hedge. Solar cliff 9,989 → 306 MW over HE14-20. Meteo forecasts Dom HE20 at $223.90, System $197.64. Wind fading from 9,300 → 8,271 MW — the cushion that prevented yesterday's blowout is weakening. 42 GW outages + 114 GW peak load + M3 gas at $3.03. Asymmetric risk/reward: lose $10-20 if muted, gain $30-50+ if the ramp fires.
Outcome: open
Notes: BalDay sold off from $90 open → $79 bid as solar came online. VWAP $84. Buying at $80 = ~$4 below VWAP, capturing the solar-driven dip ahead of the evening ramp. This morning HE7 printed $145 Dom / $128 West with -$86 Eastern congestion — the stack is tight. If wind drops below 7 GW into HE19-21, this could be a repeat of 3/12 ($889 HE20). Yesterday's evening ramp was contained ($57 HE20) because wind held at 9,300 — today wind is already fading.
Status: open

## CLOSE: PT-20260317-121500
Timestamp (local): 2026-03-17 18:00 ET
Timestamp (UTC): 2026-03-17T22:00:00Z
Outcome: loss
P/L: -$25/MWh
Exit: $55 (RT onpeak settlement)

What worked: Nothing. The thesis was wrong.

What failed: **Everything aligned fundamentally and RT still didn't blow up.**
- Wind crashed to 3.1 GW (from 9.3 yesterday) — the exact catalyst I identified. Didn't matter.
- Solar cliffed 9,175 → ~300 MW as forecast. Didn't matter.
- 42 GW outages, 114 GW load, Graceton/Conastone binding. Didn't matter.
- Meteo forecast $224 Dom HE20. Actual nowhere close.
- Settled $55 onpeak — below even my bear case ($59) at the $74 re-evaluation. I gave bear 20% probability.

**Root cause analysis — why the thesis keeps failing:**
1. **DA commitment is the hedge.** DA cleared $94 for today — PJM committed heavily. When DA is high, PJM brings on enough generation to cover the ramp. The 3/12 $889 print happened when DA was $53 and PJM was under-committed. High DA = committed stack = managed ramp.
2. **Buying RT above DA is structurally wrong in this regime.** Entry at $80 vs DA at $90.50 West flat — RT needs to beat a DA that already priced the stress. Two consecutive days of positive DARTs (DA > RT) = the DA is capturing the setup and RT underperforms.
3. **The "evening ramp blowout" is a low-frequency event being traded as base case.** 3/12 was the outlier ($889 HE20). Every other day this week: HE20 printed $52, $57, $72, and now ~$55 range. The blowout is a 1-in-5 day event, not a daily trade.
4. **Wind dropping doesn't automatically = price spike.** PJM adjusts commitment. Tie flows can increase. Load can underperform. The single-variable thesis (wind drops → ramp explodes) is too simplistic.

**Pattern recognition — 2 losses on same thesis:**
- PT-20260316-070000: Long BalDay $60, settled $40.61. Loss -$19.39.
- PT-20260317-121500: Long BalDay $80, settled $55. Loss -$25.00.
- Combined: **-$44.39/MWh on BalDay RT longs.**
- Meanwhile the missed NxtDay DA trade (PT-20260316-150000) would have made +$8.28.

**Key lesson: Trade the DA, not the RT.** DA clears have been predictable and consistently above ICE trading levels. RT is a coin flip driven by real-time commitment decisions you can't forecast. The edge is in DA clearing mechanics, not RT evening ramp speculation.

Status: closed

---

## TRADE: PT-20260318-110000
Timestamp (local): 2026-03-18 11:00 ET
Timestamp (UTC): 2026-03-18T15:00:00Z
Market: PJM
Instrument/Contract: PJM WH DA April (monthly)
Side: long
Entry: $55
Exit Target: TBD — watching for re-rate on stronger cash
Stop: —
Thesis: Buying April on risk of stronger cash. Key supports:
1. **Outage season peaks Mar-Apr.** 28+ GW planned outages are structural — doesn't go away when the cold breaks. Edi: "April rarely will be weather dependent — all outages and renewables."
2. **March cash has printed $40-100+.** The $55-90 DA clears this week show what outage season can do. Even without cold weather, outage-driven tightness persists.
3. **Gas cost floor is higher.** M3 at $3.03 vs $2.27-2.58 in early March. At 10 HR that's $30/MWh floor vs $23 — raises the marginal gas unit cost by ~$7.
4. **Transmission outages persist into April.** Brunswick ticket runs through early April. Conastone network work ongoing. Congestion premium doesn't disappear.
5. **This is the type of trade the paper trading journal says to take** — structural view on DA/monthly pricing, not a single-day RT ramp bet. Lower variance, more predictable drivers.
Outcome: open
Notes: Apr at $55 while today's BalDay RT is at $57 and NxtDay DA at $61.50. The forward is trading below current spot — implies the market expects cash to soften. The trade is that outage season keeps cash elevated above $55 avg through April. Risk is a warm April with low outages and strong renewables settling $45-50.
Status: open

---

## TRADE: PT-20260319-100000
Timestamp (local): 2026-03-19 10:00 ET
Timestamp (UTC): 2026-03-19T14:00:00Z
Market: PJM
Instrument/Contract: NxtDay DA (PDA D1) — Saturday 3/20
Side: short
Entry: $45 (intended — order did not fill)
Exit Target: DA settlement
Stop: —
Thesis: Sell Saturday DA. Like-day analog forecast: OnPk $45.93, P50 $44.05. Order book heavily stacked with offers $47-58 (~500 MW sell interest) vs thin bids ($44/$40/$37). Graceton outages ending today removes the N-S congestion premium. Warm pattern per met, weekend load drop 7-10 GW. Last Saturday (3/15) West DA was $38.75 with Graceton still out — without it, should be softer.
Outcome: open
Notes: Order at $45 did not fill — ask was $46, market never came down to $45 before DA cleared. Logging as paper trade at intended $45 entry.
Status: open

## CLOSE: PT-20260319-100000
Timestamp (local): 2026-03-19 16:00 ET
Timestamp (UTC): 2026-03-19T20:00:00Z
Outcome: win
P/L: +$3.60/MWh
Exit: $41.40 (DA clear)
What worked: The thesis was right on every count. Stacked offers signaled bearish sentiment. Like-day P50 of $44 was close to the mark — actual cleared $41.40, below P50 (roughly P35-40 band). Graceton ending + warm Saturday + weekend load = soft DA. The order book depth analysis correctly identified the directional lean.
What failed: Order didn't fill at $45. In practice would have needed to sell the $46 ask to get done — which would have made +$4.60. Lesson: when conviction is clear and the book confirms it, lift/hit the market rather than waiting for a fill.
Notes: **First DA-based paper trade win.** Validates the learning from the BalDay RT losses — DA trades have more predictable outcomes. The like-day analog model was useful as a fair-value anchor. Selling at fair value ($46) when the book is asymmetrically bearish = positive edge.
Status: closed

---

## MISSED: PT-20260320-070000
Timestamp (local): 2026-03-20 07:00 ET
Timestamp (UTC): 2026-03-20T11:00:00Z
Market: PJM
Instrument/Contract: BalDay RT (PDP D0)
Side: long
Entry: $43
Exit Target: TBD — keeping open to evaluate
Thesis: Early Conastone congestion firing on Saturday morning + lower gen commitment. Conastone 500kV constraint still active despite Graceton ending Friday — congestion premium persisting into the weekend. Lower Saturday gen commitment = tighter stack if load surprises or renewables underperform. DA HE8 at $84.92 suggests PJM sees a morning ramp.
Why Missed: Didn't take the trade. Concerned about buying RT above DA OnPk (~$41.30) given the week's pattern of DA > RT. Paper trading record on BalDay RT is 0/2 with -$44.39. Saturday load soft. However, the Conastone thesis is different from the evening ramp thesis that failed — this is a congestion-driven morning play, not a supply-scarcity bet.
Notes: Will evaluate when RT settles. The interesting question: does Conastone congestion + lower gen on a Saturday morning create enough of a spike at HE7-8 to pull the OnPk average above $43? DA has HE8 at $84.92 — if RT overperforms there, $43 could work. But if the rest of the day prints $30-35 (as HE1-6 suggests at $15-30), the math is hard.
Status: missed

---

## TRADE: PT-20260324-083000 (**FIRST ACTUAL TRADE**)
Timestamp (local): 2026-03-24 08:30 ET
Timestamp (UTC): 2026-03-24T12:30:00Z
Market: PJM
Instrument/Contract: BalDay RT (PDP D0) — Monday 3/24
Side: long
Entry: $50.50
Exit Target: settlement
Stop: —
Thesis: Monday weekday load (100+ GW forecast) + 9 Dom 500 kV outages + Mosby-Wishstar 500 kV starting today. Sunday morning ramp printed $65-73 Dom at HE7-8 with -$40 Eastern congestion ON A SUNDAY with 96 GW load. Monday with higher load + one more 500 kV outage should be structurally tighter. N-S congestion driving RT above DA.
Outcome: open
Notes: First real (non-paper) trade. 50 MW position. Entry at $50.50. At time of evaluation: ICE BalDay VWAP $47.90, Last $46.00, Bid/Ask $45.10/$50.00. Market has sold off $22 from Sunday's $68.05 settle. See evaluation below.
Status: open

## CLOSE: PT-20260324-083000
Timestamp (local): 2026-03-24 16:00 ET
Timestamp (UTC): 2026-03-24T20:00:00Z
Outcome: loss
P/L: -$7.00/MWh (-$5,600 notional @ 50 MW × 16 hrs)
Exit: $43.50 (sold BalDay contract)
RT Settlement: $51.11 (would have been +$0.61/MWh if held)
What worked: The thesis was right — congestion and outages drove RT to settle $51.11, above the $50.50 entry. Structural setup was correct.
What failed: Exited too early. PEPCO gen (Chalk Point Units 3 & 4, ~1,318 MW nat gas) stepped in during the morning, relieving congestion and causing BalDay to sell off hard. The midday valley was very bearish. Sold at $43.50 during the selloff. Then Chalk Point stepped back out that evening — congestion returned and RT ripped into the close, settling $51.11. Got shaken out of a winning position by intraday gen cycling.
Notes: 50 MW × 16 OnPeak hours = 800 MWh. $7.00 × 800 = $5,600 loss. **Key lesson: PEPCO gen cycling (Chalk Point) can temporarily relieve congestion and create false bearish signals intraday. If the structural thesis (outages + load) is intact, don't exit on a gen stepping in — it may step back out.** This ties to the "learn when to step in and out of intraday trades" lesson — the midday valley was the wrong time to exit a congestion trade.
Status: closed

---

## TRADE: PT-20260326-110000
Timestamp (local): 2026-03-26 11:00 ET
Timestamp (UTC): 2026-03-26T15:00:00Z
Market: PJM
Instrument/Contract: NxtDay DA (PDA D1)
Side: short
Entry: $48
Exit Target: DA settlement
Stop: —
Thesis: Sold DA at $48 expecting softer clear.
Outcome: loss
P/L: -$6.54/MWh
Exit: $54.54 (DA clear)
What worked: Nothing — wrong side of the trade.
What failed: **Low solar forecast for tomorrow + strong outages left very few ramping units in PEPCO.** DA cleared $54.54 — $6.54 above the entry. In outage season, low renewables don't just reduce supply on the margin — they eliminate the ramping cushion that keeps prices anchored. With PEPCO already tight on outages, the solar shortfall meant PJM had to commit expensive units to cover the ramp, pushing DA well above expectations.
Notes: 50 MW × 16 OnPeak hours = 800 MWh. $6.54 × 800 = $5,232 loss. Key lesson: **don't sell DA when renewables are forecast low AND outages are elevated** — these two factors compound. Outage season removes baseload, low renewables remove the flex that compensates. The result is a structurally tight stack that clears higher than the recent range suggests.
Status: closed

---

## TRADE: PT-20260326-111500
Timestamp (local): 2026-03-26 11:15 ET
Timestamp (UTC): 2026-03-26T15:15:00Z
Market: PJM
Instrument/Contract: NxtDay DA (PDA D1)
Side: short
Entry: $50.50
Exit Target: DA settlement
Stop: —
Thesis: Second tranche — same thesis as PT-20260326-110000. Sold DA at $50.50 after first sell at $48.
Outcome: loss
P/L: -$4.04/MWh
Exit: $54.54 (DA clear)
What worked: Better entry than first tranche — $2.50 higher reduced the loss.
What failed: Same as first tranche. Low solar + strong outages in PEPCO = no ramping margin. Averaging into a losing thesis rather than re-evaluating after the first sell didn't fill the gap.
Notes: 50 MW × 16 OnPeak hours = 800 MWh. $4.04 × 800 = $3,232 loss. Combined loss across both DA tranches: $5,232 + $3,232 = $8,464.
Status: closed

---

## TRADE: PT-20260422-094500 (**FIRST ACTUAL WIN**)
Timestamp (local): 2026-04-22 09:45 ET
Timestamp (UTC): 2026-04-22T13:45:00Z
Market: PJM
Instrument/Contract: BalDay RT (PDP D0) — Wednesday 4/22
Side: long
Entry: $59.35
Exit Target: $60.10
Stop: —
Thesis: **Cloudy-shoulder bullish regime** — outages came in at 77k MW today, cloud cover across mid-Atlantic killing BTM + utility solar, south PJM load sticking above forecast (~500-1,000 MW above P-Forecast on Innotap Southern Region chart at HE11). Three layers stacking:
1. **Outage floor**: 77 GW offline raises the marginal unit and keeps the stack tight all day.
2. **Cloud / low solar**: BTM solar never showed up — the "phantom load" shows up on the grid rather than being netted out. Load hangs flat through midday instead of dipping into the solar belly.
3. **Temps 55-60F in south PJM**: no cooling load, no heating load, but lighting + commercial activity + DC baseload elevated. Weather-neutral with clouds = pure supply-side bull case.

This is the exact regime formalized in `TODO/weather_vs_load/weather_vs_load.md` today. RT > DA setup.

Context: was greedy with weak cash earlier in the week and missed prior entries waiting for better levels. Today pulled the trigger on a regime-consistent setup.

Outcome: win
Notes: 50 MW position. First actual trade since 3/26 (~4 weeks out of the market).
Status: open

## CLOSE: PT-20260422-094500
Timestamp (local): 2026-04-22 11:30 ET
Timestamp (UTC): 2026-04-22T15:30:00Z
Outcome: win
P/L: +$0.75/MWh (+$600 notional @ 50 MW × 16 hrs)
Exit: $60.10 (BalDay contract sold)

What worked:
- **Regime identification**: cloudy shoulder + 77k outages + low solar = textbook bullish setup. The framework we just finalized in weather_vs_load.md called this exactly. The load-hangs-flat midday pattern was visible on the chart in real time.
- **Discipline on the exit**: took the $0.75 at target rather than holding for more. Direct counter to the "greedy earlier in the week" mistake. Small win kept is better than a retrace back through entry.
- **Patience paid off**: skipping the weak-cash days earlier in the week meant capital wasn't tied up or tilted when the actual setup arrived.

What failed: Nothing on this trade — size could have been larger given regime conviction, but first trade back after a 4-week gap and a $14k losing streak. Conservative sizing is correct.

Notes: **First actual winning trade.** Breaks the 0/3 losing streak on actuals. +$600 on 50 MW × 16 hrs = +$0.75/MWh. Combined actual P/L now: -$14,064 + $600 = **-$13,464**. Small dent but the more valuable result is the pattern validation — the weather_vs_load framework produced a live, tradeable call that worked on the first attempt.

**Key lesson reinforced:** Trade the regime, not the price. The entry at $59.35 wasn't a "cheap" level — it was a regime-consistent level where the structural drivers (outages + cloud + sticky load) favored longs. That's different from 3/26 where the short-DA thesis ignored the same drivers on a similar day.

Status: closed

---
