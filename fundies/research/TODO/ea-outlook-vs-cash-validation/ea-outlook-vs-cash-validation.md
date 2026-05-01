---
timestamp_local: "2026-03-13 10:00 ET"
timestamp_utc: "2026-03-13T14:00:00Z"
market: "power"
source: "Energy Aspects, PJM cash, GridStatus"
tags: [sparks, congestion, nuclear, coal, outage, datacenter, energy-aspects]
summary: "Cross-reference EA March NA Power Outlook forecasts against PJM cash observations — spark forecast ($49.18 vs $69+ market), nuclear outage congestion (Hope Creek/Calvert Cliffs), fossil call (73.7 GW), coal geopolitical risk, Dominion structural congestion, and DART regime persistence"
signal_relevance: "Validates or challenges summer spark positioning; identifies whether spring cash tightness is transient (outage season) or structural (fossil call, data centre load, coal risk)"
confidence: 3
status: "todo"
---

<!-- TODO: Determine whether EA's $49.18 NQ26 spark forecast is too conservative given persistent cash strength, or whether $69+ market is overvalued as EA claims -->
<!-- NOTE: EA raised forecast from prior but still calls market overvalued; cash printing $53 DA, $889/$1,339 RT spikes; Edi says June nearly $10 from last year's record settle -->
<!-- REVIEW: Discuss with Edi whether spring cash tightness (outages, DST mispricing) is bleeding into summer strip or if it mean-reverts -->

# EA March Outlook vs PJM Cash Validation

## Question / Hypothesis

EA's March NA Power Outlook raised the NQ26 PJM-W spark forecast to $49.18/MWh and peak-summer fossil call to 73.7 GW, but still calls $69+ market overvalued (fair value ~$46-49). Meanwhile, PJM cash is printing strong: $53 DA, RT spikes to $889 (HE20) and $1,339 (HE8), persistent negative DARTs (-$49 to -$786), and 45 GW outages climbing.

**Core question:** Is the spring cash regime validating the summer rally, or is it transient (outage season + DST mispricing) and will fade as nukes return and maintenance winds down?

Sub-questions:
1. **Spark forecast gap:** EA at $49.18 vs market at $69+. What's the right number? Cash strength supports bulls, but is it seasonal?
2. **Nuclear outage timeline:** Hope Creek (NRC investigation, no return date) and Calvert Cliffs (mid-March return). When these come back, does congestion normalize?
3. **Fossil call 73.7 GW:** Higher than 2025 realisation. Is the stack tight enough to sustain elevated pricing into summer, or does outage season ending release capacity?
4. **Coal geopolitical risk:** Iran war → global coal tightening. EA doesn't have prolonged war as base case. How much coal optionality is priced into summer?
5. **Dominion congestion — structural vs seasonal:** EA says data centre load driving structural congestion. CVOW (Q3 26) is the offset. Does congestion persist through summer or ease with outage returns?
6. **MISO unscheduled flows:** -1,904 → -4,615 over 4 days. EA doesn't cover this. Is it reinforcing W. PJM congestion structurally?

## Research Notes

### EA Forecast vs Cash Reality
| Metric | EA Outlook | Cash Observation | Gap |
|--------|-----------|-----------------|-----|
| NQ26 PJM-W spark | $49.18/MWh | Market $69+ | EA ~$20 below market |
| DA on-peak realisation | $34.74/MWh (last 2 wks) | $53.17 (3/13) | Cash accelerating |
| Peak-summer fossil call | 73.7 GW | 45 GW outages (spring) | Stack tight now |
| Nuclear gen (spring) | +0.5 GW y/y expected | Hope Creek out, Calvert out | Downside risk to EA |
| Gas share | >70% shoulder | M3 $2.585, gas in merit | Confirmed |
| Dominion congestion | Rising structurally | Dom Hub -$379 (3/4) | Confirmed, extreme |

### Key Timelines to Track
- **Hope Creek return:** Unknown — NRC investigation ongoing
- **Calvert Cliffs return:** Expected mid-March (ramping per Icon, but data unreliable per Edi)
- **CVOW commissioning:** Q3 2026 — structural Dominion congestion offset
- **NEXUS maintenance:** Apr 6 - May 4 (from EA weekly, not this outlook)
- **Outage season peak:** April expected; currently 45 GW and climbing

### Links to Morning Fundies Observations
- 3/13: HE8 $1,339 Dom, outages 45 GW, MISO unscheduled -4,615
- 3/12: HE20 $889, DART -$786, DST mispricing persistent
- 3/5: EA called $69+ overvalued, fair value ~$46
- 3/4: Dom Hub -$379, Ladysmith CB record low
- 3/2: Hope Creek emergency shutdown, EA flagged nuclear downside risk

## Data Sources
- [[PJM/LOGGED/2026-03-15-ea-power-na-outlook]] — EA March NA Power Outlook
- [[PJM/LOGGED/2026-03-11-energy-aspects-power-gas-outlook]] — EA weekly power & gas outlook
- [[PJM/PJM-Morning-Fundies]] — Daily cash observations
- GridStatus — congestion, outage, load data
- ICE — forward curves, trading activity

## Conclusion
<!-- Pending — revisit as nukes return and outage season progresses -->
