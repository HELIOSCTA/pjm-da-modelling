---
timestamp_local: "2026-04-11 12:00 ET"
timestamp_utc: "2026-04-11T16:00:00Z"
market: "power"
source: "PJM (Hot Weather Alert), internal analysis (MCP data)"
tags: [pjm, power, outage, transmission, congestion, hwa, m13, dominion, east]
summary: "PJM Hot Weather Alert issued for 04/14-04/15 citing unseasonably warm temps + high gen & tx outages. M-13 compliance drove ~12,000 MW planned outage reduction for Apr 14 in a single vintage (Apr 9→10). 146 active tx outages (>=230kV), 56 cancelled in last 7 days including mass Doubs 500kV cancellation. Eastern hub DA on-peak congestion persistently negative (-3.84 to -12.10), Dominion RT congestion swinging violently (49.96 to -7.65). Forced outages at 78th percentile and climbing despite M-13 pullback — the uncontrolled risk."
signal_relevance: "Directly validates EA's spring outage forecast (70 GW peak late April) and the Apr 6 scarcity mechanism (outages + ramp). M-13 is working on planned outages but forced outages are the gap. Eastern 500kV cluster (HOPECREE, HOPATCON, BURCHESH) is structural through May — no returns within 7 days. New HUNTERST 500kV starting on HWA day 1 is a contradiction worth monitoring. Dominion 500kV congestion pattern (POSS560, LADYSMTH, FENTRES4) aligns with Cloud XF / data center corridor constraints."
confidence: 5
status: "logged"
original_source_path: ""
original_url: ""
---

# PJM Hot Weather Alert + Transmission Outage Analysis — M-13 Effect on System Stress (2026-04-11)

## Summary

PJM issued a **Hot Weather Alert for 00:00 Apr 14 through 23:59 Apr 15**, explicitly citing **"unseasonably warm temperatures coupled with high generation and transmission outages."** Per Manual M-13, members are directed to review whether maintenance/testing on transmission and generating equipment can be deferred or cancelled. The outage vintage data shows this directive produced a **~12,000 MW single-day reduction in planned generation outages** for Apr 14 (from 53,703 MW to 41,563 MW between the Apr 9 and Apr 10 forecasts). On the transmission side, **56 outages were cancelled in the last 7 days**, including a mass cancellation of ~20 Doubs 500kV/230kV outages on Apr 7. Despite M-13 compliance, **forced outages are at the 78th percentile (9,115 MW) and climbing**, and major 500kV construction outages in Eastern PJM and Dominion cannot be deferred.

## Key Points

### Hot Weather Alert Details
- **Period**: 00:00 Apr 14 through 23:59 Apr 15, 2026
- **Stated cause**: Unseasonably warm temperatures + high generation and transmission outages
- **M-13 actions**: TOs/generators must review deferral of maintenance; generators update unit parameters, fuel availability, early return times in eDART

### M-13 Effect — Generation Outages (~12,000 MW Pullback)

Planned outage forecast for **Apr 14** (HWA day 1):

| Vintage | Planned MW | Forced MW | Total MW |
|:--|--:|--:|--:|
| Apr 8 | 55,162 | 4,818 | 68,237 |
| Apr 9 | 53,703 | 5,140 | 66,163 |
| **Apr 10** | **41,563** | **8,169** | **55,571** |
| Apr 11 (Current) | 41,025 | 10,688 | 57,900 |

- **~12,140 MW planned outage reduction** between Apr 9 and Apr 10 vintages for Apr 14
- **~12,604 MW planned outage reduction** for Apr 15 over the same period
- The HWA was communicated to members around Apr 9-10; compliance was immediate

### M-13 Effect — Transmission Outage Cancellations (56 in 7 days)

**Doubs 500/230kV mass cancellation (Apr 7):** ~20 outages cancelled in a single day at the Doubs substation (BGE-PEPCO), including DOUBS→GOOSECRE 500kV (x6), BRIGHTON→DOUBS 500kV (x4), DOUBS 500kV transformers (x2), and multiple AQUEDUCT→DICKERSH/DOUBS 230kV lines. Doubs sits on the 500kV backbone between Mid-Atlantic and Eastern hubs. Cancelled on the same day East-West DA on-peak congestion spread was -16.36.

**Short-notice 345kV cancellations (Apr 8-10):** TANNERSC→EBEND (AEP), 138 SILV→144 WAYN (ComEd), GLENWILL→INLAND and CHAMBROE transformers (FirstEnergy), ATLNTA→ADKINS (Ohio Valley) — all same-week or near-term work pulled back.

**MTSTORM→PRUNTYTO 500kV (Dominion, cancelled Apr 7):** Short window for cable repair at a major generation node. Preserves Dominion export capacity during a week with 30 active Dominion tx outages.

### The Gap — Forced Outages Moving the Wrong Direction

- **Current forced outages**: 9,115 MW — 78th percentile for April, z-score 0.84
- **Forced outage forecast for Apr 13**: 11,964 MW (peak)
- **Forced outages vs 24h ago**: +1,964 MW
- **YoY**: April 2026 forced outages averaging 9,540 MW vs 8,536 MW in April 2025

M-13 can pull back planned outages but cannot control forced outages. The forced outage surge is eating into the supply headroom created by planned outage deferrals.

### Active Transmission Constraints (Cannot Be M-13'd)

**146 active tx outages >= 230kV.** The major 500kV construction/repair outages driving congestion are in-progress and cannot be deferred:

**Eastern 500kV cluster (no returns within 7 days):**
- HOPECREE→SALEM 500kV (path) — insulator repair, 20 days left
- HOPATCON→LACKAWAN 500kV x2 (path) — insulator repair, 19-20 days left
- BURCHESH→POSSUMPT 500kV (path) — 18 days left
- SMITHBUR 500kV transformer (capacity) — 34 days left

**Dominion 500kV cluster:**
- POSS560→POSSUMP4 500kV (path) — construction, 18 days left
- LADYSMTH→ELMONT4 500kV x2 (path) — construction, 19 days left
- FENTRES4→YADKIN4 500kV (path) — returning Apr 16 (relief)
- FENTRES4 500kV transformer (capacity) — NEW, 110 days left
- MARS2 500kV transformer (capacity) — emergency, 9 days left

**Contradiction — HUNTERST→VINCO 500kV starting on HWA day 1 (Apr 14):**
New 500kV path outage in BGE-PEPCO, 30-day maintenance inspection. Joins already-out BEDINGTO 500kV transformer (65 days remaining) in the same region. Either approved before the HWA or deemed critical enough to proceed.

### Regional Congestion Confirms System Stress

**DA congestion on-peak (Apr 11):**
- Western Hub: +4.01 (constrained, generation trapped)
- AEP Gen: +2.03
- Dominion: +0.41 (collapsed from +4.75 on Apr 4)
- **Eastern Hub: -3.84** (persistent congestion credit — flow blocked from reaching East)

**RT congestion volatility:**
- Dominion RT on-peak swung from **+49.96** (Apr 4) to **-7.65** (Apr 10) — extreme intra-week reversal
- Eastern RT on-peak hit **-42.31** (Apr 4), **-9.48** (Apr 10)
- Apr 4 DA/RT divergence: Dominion DA cong 4.75 vs RT 49.96 — massive mis-pricing

**East-West DA on-peak spread:** Narrowing from -20.78 (Apr 4) to -7.85 (Apr 11) but still wide. Driven by Eastern hub negative congestion from the 500kV outage cluster.

**Dom-AEP spread flipped negative:** -1.62 on Apr 11 (Dominion cheaper than AEP). First negative readings of the week, coinciding with Dominion RT congestion going sharply negative.

## Trading Implications

- **The ~12,000 MW planned outage pullback is priced in by now** — generators complied immediately (visible in Apr 10 vintage). The market has adjusted. The residual risk is forced outages, which are at 78th percentile and climbing to a forecast 11,964 MW peak on Apr 13.
- **Eastern congestion is structural through May 1** — the HOPECREE, HOPATCON, and BURCHESH 500kV outages all return late April / early May. No relief within the HWA window. Eastern hub will continue to trade at a congestion discount to Western Hub. The East-West DA spread should remain negative (currently -7.85).
- **Dominion is the wild card** — the FENTRES4→YADKIN4 500kV return on Apr 16 provides partial relief, but the new FENTRES4 transformer (110 days) and the Possum Point dual outage (500kV line + 230kV transformer) keep the substation constrained. Dominion RT congestion volatility (49.96 to -7.65 in one week) signals transient constraints that DA is not capturing. This connects to the Cloud XF / data center corridor congestion documented in [[2026-04-07-power-datacenter-cloud-xf-dominion-congestion]].
- **The Apr 6 scarcity mechanism is still live** — EA documented 59.1 GW outages + 14 GW evening ramp producing $3,300/MWh RT. Current outages are 61,135 MW (higher than Apr 6). If heat on Apr 14-15 lifts load and the evening ramp repeats with forced outages at 10,000+ MW, the same scarcity pattern is available. The HWA is PJM's acknowledgment.
- **Watch HUNTERST→VINCO 500kV (Apr 14 start)** — if PJM pulls this outage before the HWA begins, it signals greater reliability concern than the alert alone conveys. If it proceeds, the Mid-Atlantic 500kV backbone has two concurrent outages (HUNTERST + BEDINGTO) during the alert period.

## Sources

- PJM Hot Weather Alert (M-13 notification, Apr 11 2026)
- PJM MCP endpoints: transmission outages, outage term bible, outage forecast vintages, Western Hub LMP 7-day lookback, regional congestion (all pulled 2026-04-11)

## Related

- [[2026-04-07-ea-power-pjm-reserve-scarcity-apr6]] — Apr 6 scarcity event: 59.1 GW outages + 14 GW ramp = $3,300/MWh RT. Same mechanism in play for HWA period.
- [[2026-04-07-power-datacenter-cloud-xf-dominion-congestion]] — Cloud XF / data center corridor congestion in Dominion. Dominion 500kV outages (POSS560, LADYSMTH) overlap with this constraint geography.
- [[2026-04-02-ea-power-ad-west-spread-congestion]] — AD-West spread analysis. East-West congestion spread currently -7.85 DA on-peak, consistent with EA's -$10/MWh fair value estimate.
- [[2026-04-06-ea-power-pjm-w-march-spark-record]] — March spark record driven by outages + fossil call. April outages remain elevated.
- [[2026-04-09-power-pjm-dlr-updates]] — DLR go-live 8/1/2026 on 34 Dominion 230kV lines. Would provide relief on exactly the corridors currently constrained, but not until August.
- [[2026-03-20-ea-power-pjm-reserve-reform-lmp-impact]] — RCSTF reform delayed to winter 2027. Current reserve design persists through the HWA period and all of NQ26.
