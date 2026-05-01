---
timestamp_local: "2026-04-09 09:00 ET"
timestamp_utc: "2026-04-09T13:00:00Z"
market: "power"
source: "PJM IMM (Monitoring Analytics)"
tags: [power, pjm]
summary: "IMM inquiry into March 1 2026 synch reserve event: 72% response rate (1,834 of 2,538 MW deployed), 704 MW shortfall. Communication/hardware issues drove ~520 MW of shortfall; modeling/parameter issues ~110 MW. IMM recommends PJM stop capping over-performers when calculating metrics — full performance was 91%."
signal_relevance: "Reserve response shortfall patterns directly relevant to scarcity pricing risk. 28% non-response rate means reserve margins are effectively thinner than cleared MW suggest. Historical data shows persistent parameter/personnel issues — reserve events during tight stacks will underperform."
confidence: 4
status: "logged"
original_source_path: "PJM/@Reading/2026-04-09-power-pjm-synch-reserve-performance-inquiry.pdf"
original_url: "https://www.pjm.com/-/media/DotCom/committees-groups/committees/oc/2026/20260409/20260409-item-11---synchronized-reserve-performance-inquiry-results.pdf"
---

# PJM IMM — March 1, 2026 Synchronized Reserve Performance Inquiry Results (OC, 2026-04-09)

## Summary

The IMM investigated underperformance during the March 1, 2026 synchronized reserve event. Of **2,538 MW deployed**, only **1,834 MW responded** (72% response rate), with **704 MW shortfall**. Including over-response from other units, effective performance was **91% (2,302 MW)**. The IMM contacted 14 resources representing ~630 MW (89% of total shortfall).

## Key Points

### Event Performance
- **Deployed**: 2,538 MW
- **Responded**: 1,834 MW (72%)
- **Shortfall**: 704 MW (28%)
- **Including over-response**: 2,302 MW (91%)
- 14 resources investigated, covering ~630 MW of the shortfall

### Shortfall by Primary Cause
- **Communication / Hardware / Software**: ~520 MW shortfall (from ~820 MW deployed) — the dominant category
- **Modeling / Parameter**: ~110 MW shortfall (from ~150 MW deployed)

### IMM Category Definitions
| Category | Definition |
|----------|-----------|
| Communication | Delayed/failed action or communication by MOC operator or plant personnel |
| Hardware/Software | Mechanical problems, software misconfiguration, fuel problems limiting response |
| Modeling | Transition time to change equipment state or unit direction |
| Parameter | Ramp rates, eco max, offer amounts overstated resource ability |
| Personnel | Incorrect/no action by operator; operator didn't know procedure for spin event |

### Historical Shortfall Trends (MW by cause)

| Day | Communication | HW/SW | Modeling | Parameter | Personnel | Total |
|-----|-------------|-------|---------|-----------|-----------|-------|
| 08-Jul-2024 | 860.6 | 85.9 | 78.1 | 355.9 | 271.9 | 1,652.4 |
| 18-Aug-2024 | 56.3 | 271.5 | 49.7 | 183.2 | 36.2 | 596.8 |
| 10-Nov-2024 | 28.2 | 1.6 | 107.2 | 207.1 | 79.4 | 423.5 |
| 05-Feb-2025 | 0.0 | 113.6 | 54.9 | 83.1 | 240.5 | 492.1 |
| 01-Jul-2025 | 0.0 | 17.2 | 211.6 | 82.0 | 8.0 | 318.7 |
| 22-Jul-2025 | 35.5 | 142.1 | 178.8 | 136.8 | 29.8 | 523.0 |
| 25-Sep-2025 | 0.1 | 135.8 | 34.8 | 102.2 | 139.2 | 412.0 |
| 17-Oct-2025 | 13.1 | 26.8 | 59.9 | 105.3 | 69.4 | 274.5 |
| 28-Oct-2025 | 2.3 | 301.1 | 12.7 | 265.0 | 1.9 | 583.0 |
| 11-Nov-2025 | 32.0 | 135.8 | 11.4 | 134.7 | 4.7 | 318.6 |

### Historical Full Response (Capped vs Full Performance)

| Day | Assigned MW | Capped Response | Capped % | Full Response | Full % |
|-----|-----------|----------------|---------|-------------|-------|
| 08-Jul-2024 | 3,234 | 1,479 | 46% | 1,621 | 50% |
| 05-Feb-2025 | 1,924 | 1,252 | 65% | 1,623 | 84% |
| 01-Jul-2025 | 2,942 | 2,337 | 79% | 2,933 | 100% |
| 22-Jul-2025 | 3,312 | 2,610 | 79% | 3,304 | 100% |
| 28-Oct-2025 | 2,282 | 1,389 | 61% | 1,847 | 81% |
| 11-Nov-2025 | 2,724 | 2,254 | 83% | 2,829 | 104% |

### IMM Observations
- **Communication issues improving** — trend from 860 MW (Jul-2024) down to near-zero in recent events
- **Personnel issues persistent** — operators still don't know procedures; MOC/gen owners need training focus
- **Parameter issues fixable** — spin max exceptions, ramp rate updates, eco max corrections available today

### IMM Recommendations
1. PJM's current metric **artificially understates performance** by capping over-performers at assigned amount
2. Calculate metrics **including all units assigned reserves** (not just deployed subset)
3. Calculate metrics **without capping over-performing units** — over-response offsets shortfalls

## Trading Implications

- **28% non-response rate is material** for scarcity pricing. When PJM deploys reserves, expect ~25-30% of MW to not show up. This means effective reserve margins are thinner than cleared MW suggest.
- Historical data shows shortfall is **persistent** (275-1,650 MW per event) — this isn't getting solved quickly, especially personnel/parameter issues.
- The Jul-2024 event (1,652 MW shortfall, only 50% full performance) shows how bad it can get during summer peaks. Summer 2026 reserve events on high-outage days could see similar underperformance.
- Relevant to your ACE and reserves TODOs — reserve deployment events are exactly when ACE goes negative, and the shortfall data shows why price spikes persist longer than expected.

## Sources

- PJM Operating Committee, April 9, 2026, Item 11
- Monitoring Analytics (IMM)

## Related

- [[2026-03-20-ea-power-pjm-reserve-reform-lmp-impact]] — RCSTF reserve reform proposal
- [[area-controlled-error]] — ACE as scarcity leading indicator (TODO)
