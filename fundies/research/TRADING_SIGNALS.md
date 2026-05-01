# PJM Daily Trading Signals

## How to Use
Check each section every morning before market open. Mark signals as
bullish (+), bearish (-), or neutral (=). Update confidence 1-5.

---

## Power Price Signals

### DA / RT Spread
- [ ] DA clear vs pre-clear trading level (directional signal)
- [ ] DA-RT spread direction (DA high + soft RT = PJM managing; strong RT = underpriced)
- [ ] Balweek / weekly strip moves vs DA

### Load & Weather
- [ ] Load forecast vs actual (overperformance = bullish)
- [ ] DST / seasonal peak shift (PJM slow to adjust forecast timing)
- [ ] Temperature: freezing in electric-heating-rich VA = bullish
- [ ] Cloud coverage impact on BTM solar (bullish for daytime prices)
- [ ] Storm risk: winter/shoulder = bullish, summer = bearish

### Generation Stack
- [ ] Nuclear status: Hope Creek, Calvert Cliffs, Davis Besse, Byron, LaSalle
- [ ] Forced outage MW (>15 GW = regime shift, 22x congestion)
- [ ] Total outage MW (context: high total in shoulder = low congestion)
- [ ] Coal vs gas dispatch economics (M3 gas price, NAPP coal)
- [ ] Pumped hydro mode (Bath County, Smith Mountain) — pumping vs generating

### Congestion
- [ ] N-S: Conastone-Northwest, Nottingham SD, Harrowgate-Locks
- [ ] Dominion: Ladysmith CB, Aquia Harbor-Garrisonville, Bremo-Fork Union
- [ ] COMED: west-to-east wind-driven (Byron/LaSalle nuclear impact)
- [ ] AEP/Western Hub: Hyatt-Marysville, Monroe-Lallendorf (M2M with MISO)
- [ ] Penelec morning congestion events
- [ ] DA MCC capture vs RT realization

### Transmission Outages
- [ ] Ladysmith 230kV/500kV network (spring congestion season)
- [ ] Elmont 500kV / Chesterfield 230kV (Central VA)
- [ ] Conastone network (N-S flow impact)
- [ ] Scheduled returns (outage ending = congestion relief)

---

## Gas Price Signals (PJM-relevant)

### Delivered Gas
- [ ] M3 (Transco Zone 6) price level and direction
- [ ] Eastern Gas South (Dominion South) basis
- [ ] Gas in the money? (compare delivered gas cost to power clearing price)

### Supply Fundamentals
- [ ] Appalachian production level (~36-36.5 Bcf/d steady)
- [ ] Canadian imports trend (CAD flow weakness = bullish)
- [ ] LNG feed gas demand (>20 Bcf/d = supportive)

### Macro
- [ ] TTF impact on US coal exports (higher TTF = higher coal = bullish PJM sparks)
- [ ] Spark spread vs fair value (EA: PJM-W NQ26 fair value ~$46 vs market >$69)

---

## Structural / Seasonal
- [ ] Shoulder season outage risk (spring = volatile, high planned outages)
- [ ] Flat load from data centers (upends typical demand trends)
- [ ] Solar curtailment risk in Dominion (spring/shoulder)
- [ ] Month depth: deeper into month = less liquidity, harder to exit

---

## Signal Log (2026-03-13)
| Signal | Direction | Confidence | Note |
|--------|-----------|------------|------|
| DA-RT spread | + | 5 | 3/12 DART: -$49 to -$57 flat. HE20 alone: -$786 West. DA systematically underpricing ramp hours. |
| Load forecast vs actual | + | 5 | DA missed 3/12 evening by +4.8 GW (HE20) to +7.6 GW (HE23). Cold + DST shift. |
| DST peak shift | + | 5 | Evening peak shifted later than DA model. HE20-21 actual peak vs DA expecting earlier. See [analysis](TODO/rt-price-cap-friday-mar-13/rt-price-cap-mar-12-13.md). |
| Temperature (VA freezing) | + | 4 | Sub-freezing temps drove electric heating overperformance. GridStatus flagged VA specifically. |
| Forced outage MW | + | 4 | 11.7 GW forced (3/13). 61% of forced concentrated in WEST. Total 45 GW. |
| Solar cliff timing | + | 4 | Solar lost 6,855 MW in 2 hrs (HE18→20) into load ramp. Gas couldn't replace fast enough. See [analysis](TODO/rt-price-cap-friday-mar-13/rt-price-cap-mar-12-13.md). |
| MISO unscheduled flows | + | 4 | Accelerating: -1,904 (3/10) → -4,615 (3/13). NIPS 2 GW unscheduled at HE20. Drains W. PJM supply. |
| Shoulder outage risk | + | 5 | Maint +3.5 GW overnight to 10.9 GW. Weekend planned surge to ~29 GW. Outage season accelerating. |
| Balweek / weekly strip | - | 3 | BalDay RT sold $58→$43.50. NxtDay products weak. Market pricing mean-reversion. |
| Nuclear status | = | 3 | Steady ~30.4 GW. No new trips. Hope Creek watch. |
| M3 gas | = | 2 | M3 $2.585 flat DoD. Cash below balmo ($2.775). Not the driver — outages and load are. |

---

## Paper Trading Linkage

Signals feed directly into paper trades. When a signal triggers a trade idea, log it via `/pjm-new-trade` and reference the signal in the thesis. When reviewing trades via `/pjm-review-trades`, check which signals were behind each trade's thesis.

- **Trade log:** [[PJM/PAPER-TRADING/trade-log.csv]]
- **Trade journal:** [[PJM/PAPER-TRADING/trade-journal.md]]
- **Review log:** [[PJM/PAPER-TRADING/review-log.md]]

### Signal → Trade Map (running)
| Date | Signal(s) Fired | Trade ID | Outcome | Lesson |
|------|-----------------|----------|---------|--------|
| 2026-03-13 | DA-RT spread (strong RT overnight) | PT-20260313-050000 (missed) | TBD | Need to be online pre-5AM when overnight RT spikes |
| 2026-03-12 | DA-RT spread (calm day, tail risk) | PT-20260312-160000 (missed) | would-loss | Correctly avoided — RT exploded HE20 to **$889** (not $120). See [full analysis](TODO/rt-price-cap-friday-mar-13/rt-price-cap-mar-12-13.md). |
