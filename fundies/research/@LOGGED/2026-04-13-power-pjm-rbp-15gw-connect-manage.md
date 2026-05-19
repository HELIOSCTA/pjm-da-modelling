---
timestamp_local: "2026-04-13 10:00 ET"
timestamp_utc: "2026-04-13T14:00:00Z"
market: "power"
source: "Energy Aspects"
tags: [ea, power, pjm, datacenter, load]
summary: "PJM's initial Reliability Backstop Procurement (RBP) proposal targets 14.9 GW of new capacity by 2031, calculated from estimated summer coincident peak load growth due to large loads over 2026-29. DOM leads at 4.9 GW, AEP 3.3 GW, ComEd 2.6 GW, PPL 1.7 GW. Two-phase process: bilateral contracting (Sep 2026-Mar 2027), then pay-as-bid centralized auction for the residual."
signal_relevance: "Massive capacity procurement signal — 14.9 GW is ~10% of PJM peak load. Zonal breakdown confirms DOM/AEP as epicenters. Bilateral window Sep-Mar 2027 creates near-term contracting activity. Residual auction could set marginal capacity price signals. Directly tied to Connect and Manage / CAMSTF framework."
confidence: 4
status: "logged"
original_source_path: ""
original_url: ""
---

# PJM Reliability Backstop Procurement — Initial Proposal Targets 14.9 GW (2026-04-13)

## Summary

PJM's initial **Reliability Backstop Procurement (RBP)** proposal would obligate nearly **14.9 GW** of large loads to procure capacity to avoid curtailment risks under the Connect and Manage scheme. The target is derived from estimated summer coincident peak load growth due to large loads over **2026–2029**. Dominion leads with **4.9 GW**, followed by AEP (3.3 GW), ComEd (2.6 GW), and PPL (1.7 GW). The process runs in two phases: bilateral contracting (Sep 2026–Mar 2027), then a centralized pay-as-bid auction for the residual.

## Key Points

### Procurement Target
- **14.9 GW** total — estimated summer coincident peak load growth from large loads over 2026–2029
- This is a capacity obligation on the large loads themselves, not PJM procuring on their behalf

### Zonal Breakdown

| Zone | RBP Target (GW) | % of Total |
|------|-----------------|-----------|
| DOM | 4.9 | 33% |
| AEP | 3.3 | 22% |
| ComEd | 2.6 | 17% |
| PPL | 1.7 | 11% |
| Other | ~2.4 | 16% |

### Two-Phase Process
1. **Bilateral period** (Sep 2026 – Mar 2027): Large loads contract directly with capacity suppliers and report contracted volumes to PJM
2. **Centralized auction** (pay-as-bid): Procurement target = 14.9 GW minus bilateral contracted supply. PJM administers for the residual.

### Connect and Manage Link
- Loads that don't procure capacity face **curtailment risk** under PJM's Connect and Manage framework
- This is the enforcement mechanism — procure or be curtailable during emergencies
- CAMSTF is the stakeholder body developing the rules (Board wants implementation by year-end)

## Supply-Side Reality Check (via @mosessutton89 / X thread)

The 14.9 GW headline is **"up to" rather than firm**, and the eligible supply set was largely knowable before the announcement. The 2031 COD deadline combined with a 3-5 year interconnection queue means this is really a procurement of **what's already in the pipeline**, not a greenfield signal. OEM turbines are booked through 2028/2029.

### What's Actually in the Pipe (~12 GW UCAP gas)

| Project / Developer | Capacity | Status |
|-------------------|----------|--------|
| Homer City | 4.4 GW | Turbines secured |
| Tenaska Virginia | 1.5 GW | State permitting |
| SB Energy | 1-2 GW max | — |
| NEE | <1 GW | Most near-term turbine supply assigned to TX per earnings calls |
| Other RRI new build | >4 GW | Various stages |
| Ohio Power Siting Board projects | ~1 GW | — |
| RRI uprates (nuclear + gas) | 1.5+ GW | — |

**Total identifiable**: ~12 GW UCAP gas already in process and likely RBP-eligible.

### Filling the Gap
- Maybe 1-2 GW more pre-FID but late-stage gets to **~13+ GW**
- Remaining **~3 GW UCAP gap** likely filled by **~10 GW ICAP batteries** (lower UCAP derating)
- Investors had assumed +6 GW — the 15 GW target is **2.5x** what was priced in

### Over-Procurement Risk
- **EDCs bear over-procurement costs**, pushing them toward conservative load forecasts
- This creates a structural incentive to understate the RBP target over time
- The 14.9 GW number may shrink as EDCs push back on obligation allocation

### Process Mechanics
- **Bilateral phase** (Sep 2026 – Mar 2027): Hyperscaler-to-EDC contracts — expect large tech companies (Microsoft, AWS, Google, Meta) to contract directly with generators
- **Central auction**: Backfills whatever bilateral doesn't cover — pay-as-bid, not uniform clearing price

## Trading Implications

- **Less bullish than the headline suggests** — the ~12 GW of gas in pipeline was already known to the market. The incremental signal is really the ~3 GW battery gap and the formalization of the procurement mechanism.
- **14.9 GW is "up to"** — EDC over-procurement cost exposure means the actual target will likely be conservative. Watch for EDC load forecast submissions as the real signal.
- **DOM at 4.9 GW confirms datacenter dominance** — one-third of the total target is in Dominion alone. This aligns with the load growth data (1.1 GW YTD, 25.2 GW record, 48.5 GW queue).
- **Bilateral window creates near-term activity** — Sep 2026 to Mar 2027 means large loads need to start contracting imminently. This should tighten capacity prices in DOM, AEP, and ComEd over the next 6 months.
- **AEP deliverability is the bottleneck** — AEP is 15 GW long on ICAP, and Homer City (4.4 GW) is in western PJM. But interface limits constrain how much of this supply can actually serve DOM/Eastern load. The RBP doesn't solve the transmission problem.
- **Battery UCAP derating matters** — ~10 GW ICAP batteries filling a 3 GW UCAP gap implies heavy derating (~30% UCAP factor). Batteries help for 4-hour events but don't solve the multi-hour ramp scarcity problem (see 4/6 event: 14 GW net load ramp over 2 hours).
- **NEE turbine allocation to TX** — limited PJM supply from NextEra means their pipeline is not a meaningful RBP contributor. This is a constraint on the supply side.
- **BYONG tension** — BYONG pathway lets co-located DCs avoid RBP obligations. If large loads route through BYONG, PJM's procurement target shrinks but the reliability problem doesn't.
- **ComEd at 2.6 GW is notable** — third largest zone, consistent with the emerging ComEd datacenter growth signal (0.9 GW accepted load adjustments, 0.4 GW DC entering service).
- **PPL at 1.7 GW** — PPL is Eastern PJM (PENELEC/PPL territory), where Susquehanna nuclear outage and Conastone constraints are already creating stress.

## Sources

- Energy Aspects
- @mosessutton89 (X thread, April 2026)

## Related

- [[co-located]] — PJM co-location rules, 50 MW threshold, BYONG pathway
- [[2026-02-27-ea-datacenter-regulatory-risk]] — Regulatory headwinds: co-location, Connect and Manage, BYONG
- [[2026-04-02-ea-power-ad-west-spread-congestion]] — AEP 15 GW long capacity, interface limits to DOM
- [[2026-03-16-ea-load-datacenter-zone-growth]] — AEP +1.3 GW, DOM +1.1 GW YTD load growth
