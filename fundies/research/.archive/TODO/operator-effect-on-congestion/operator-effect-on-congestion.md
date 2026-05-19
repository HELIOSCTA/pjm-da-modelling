---
timestamp_local: "2026-03-18 09:45 ET"
timestamp_utc: "2026-03-18T14:45:00Z"
market: "power"
source: "PJM, GridStatus"
tags: [congestion, power]
summary: "PJM RT operators systematically manage congestion below DA-modeled levels — structural driver of positive DART congestion component"
signal_relevance: "DA congestion overprices when multiple transmission outages create overlapping constraints but load isn't extreme enough to exhaust operator tools. Strongest signal during shoulder season with elevated outages."
confidence: 3
status: "todo"
---

<!-- TODO: Quantify the DA vs RT congestion gap historically — is it persistent or event-driven? Build a signal from outage count + load level to predict when the operator effect is strongest -->
<!-- NOTE: Identified during 3/18 morning fundies. GridStatus confirmed PJM "kept volatility under wraps" and limited N-S congestion despite Bedington/Graceton/Conastone outages. DA had priced Eastern -$129 cong at HE1 but operators managed it down by morning -->
<!-- REVIEW: Discuss with Edi — is this a known/traded pattern? Could inform DA congestion fade trades -->

# Operator Effect on DA vs RT Congestion

## Question / Hypothesis

**Hypothesis:** PJM's DA market systematically overprices congestion because the SCED model treats transmission limits as hard constraints, while RT operators have discretionary tools to soften those limits in real-time. This creates a persistent positive DART on the congestion component.

**Observed this week (3/15-3/18):**
- DA Eastern Hub congestion: -$32 to -$38 (flat avg)
- RT Eastern Hub congestion: -$10 to -$28 (flat avg)
- DA consistently priced deeper N-S congestion than materialized in RT
- GridStatus (3/18): "PJM kept volatility under wraps this morning, limiting RT energy price volatility as well as limiting real-time congestion, particularly limiting N-S congestion into Virginia"

## Research Notes

### How the DA Model Prices Congestion

The DA market clears 12-36 hours ahead using a security-constrained economic dispatch (SCED) model. It takes forecasted load, generation offers, and transmission topology, then solves for least-cost dispatch respecting all thermal limits. When it sees a binding constraint — e.g., Bedington 500 kV out, Graceton 230 kV out, heavy N-S flows into Dominion — it prices congestion to reflect the cost of redispatching around that constraint.

The DA model treats these limits as **hard walls**. It cannot anticipate operator discretion.

### RT Operator Tools That Soften Constraints

In real-time, PJM operators have tools the DA model doesn't account for:

1. **Transmission Loading Relief (TLR) / redispatch:** Operators call on specific generators to ramp up or down outside normal economic dispatch to manage flows. Especially relevant on the MISO seam where unscheduled loop flows can be curtailed.

2. **Emergency energy purchases:** PJM can buy emergency energy from neighboring RTOs (MISO, NYISO, TVA) to relieve constraints the DA model didn't anticipate.

3. **Voltage schedule adjustments:** Operators adjust voltage setpoints at substations to change reactive power flows, which affects real power flow distribution across parallel paths.

4. **Phase angle regulator (PAR) adjustments:** On controllable ties (phase shifters on the MISO seam), operators actively steer flows away from constrained corridors.

5. **Post-contingency acceptance:** The DA model must respect N-1 (and sometimes N-2) contingency limits statically. Operators in RT can accept temporarily elevated flows on a monitored basis, knowing they can intervene if an actual contingency occurs. This effectively loosens the constraint vs the conservative DA assumption.

### Why This Creates a Systematic DA Congestion Premium

**DA sees:** Bedington 500 kV out + Graceton 230 kV out + 118 GW load = hard binding constraint on N-S flows → prices Dom congestion at +$27, East congestion at -$58.

**RT operator sees:** Same outages, but manages actual flows by adjusting PAR settings on MISO seam, calling TLR to reduce loop flows, asking a Dom-zone generator to ramp up, or accepting short-duration overload on a parallel path. The constraint still exists but the **effective limit is softer in RT** because of operator discretion.

**Result:** DA congestion > RT congestion on the same constraint under the same conditions.

### When the Effect is Strongest

The operator effect on congestion is **strongest** when:
- Multiple transmission outages create overlapping constraints (like this week's Bedington + Graceton + Conastone)
- Load is high enough to approach constraint limits but **not so high** that operators run out of tools
- The constraint is on a major interface with controllable elements (N-S corridor, MISO seam)
- Shoulder season: elevated outages (maintenance window) + moderate load = operators have room to maneuver

The operator effect is **weakest** (and DARTs can flip negative on congestion) when:
- Load is so extreme that operators exhaust their tools and the hard constraint binds in RT too (like 3/12's $889 HE20 — operators couldn't prevent scarcity)
- A forced outage creates a constraint the DA model **didn't see at all** (DA underprices, RT overprices)
- Winter peak / summer peak conditions where every MW of transfer capability is needed

### Evidence from This Week

| Date | DA East Cong (flat) | RT East Cong (flat) | Cong DART | Context |
|------|--------------------|--------------------|-----------|---------|
| 3/15 Sat | -$8.75 | -$2.44 | +$6.31 | Weekend, light load |
| 3/16 Mon | -$26.72 | -$9.57 | +$17.15 | Weekday return, 101 GW |
| 3/17 Tue | -$38.09 | -$27.92 | +$10.17 | Peak outages, 113 GW |
| 3/18 Wed | -$32.05 | ? | ? | 115 GW actual, operators managing |

The congestion DART was positive every day — DA consistently overpriced Eastern congestion vs RT. The gap was widest Monday (+$17.15) when load was moderate (101 GW) and operators had the most room. It narrowed Tuesday (+$10.17) as load hit 113 GW and operators had less flexibility.

**3/18 morning example:** HE1 printed Eastern -$129.20 congestion in RT (overnight, before operators fully engaged). By HE7, Eastern congestion had moderated to -$24.12. Operators managed the flows down over 6 hours despite identical transmission topology.

### Potential Signal Construction

A tradeable signal could combine:
1. **Transmission outage count** (≥230 kV, active) — higher = more DA congestion priced
2. **Load level vs capacity** — moderate load (90-110% of forecast) = strongest operator effect; extreme load (>115%) = operator tools exhausted
3. **DA congestion magnitude** — deeper DA congestion = more room for RT to underperform
4. **Day of week** — weekday outage maintenance schedules create more predictable constraint patterns

**Threshold hypothesis:** When DA Eastern Hub congestion exceeds -$20 and load is forecast <115 GW, the DART congestion component is likely positive (DA overstates). When load forecast exceeds 115 GW with the same outages, the operator effect weakens and the DART narrows or flips.

## Data Sources

- `pjm_cleaned.pjm_lmps_daily` (01) — DA/RT congestion by hub
- `pjm_cleaned.pjm_lmps_rt_hourly` (02) / `pjm_lmps_hourly` (03) — Hourly congestion for intraday pattern
- `pjm.transmission_outages` (06c/d/e) — Outage count and severity
- `pjm_cleaned.pjm_load_rt_prelim_daily` (05) — Actual load for threshold analysis
- GridStatus commentary — Qualitative confirmation of operator actions

## Conclusion

<!-- pending — need to validate the threshold hypothesis across more days. Track DA vs RT congestion at Eastern Hub over the next 2 weeks as outages return (Graceton 3/20, Fentress-Yadkin 3/24) to see if the DART narrows as constraints ease -->
