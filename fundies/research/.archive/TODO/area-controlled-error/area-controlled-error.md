---
timestamp_local: "2026-04-07 08:00 ET"
timestamp_utc: "2026-04-07T12:00:00Z"
market: "power"
source: ""
tags: [power, load, outage, wind, nuclear]
summary: "PJM RT hit $3,169/MWh shortage pricing at HE20 on 4/6; ACE data showed persistent negative values over evening peak indicating generation couldn't ramp ahead of net load peak despite load coming in under forecast."
signal_relevance: "ACE as a leading indicator for RT scarcity events — negative ACE over ramp hours signals supply-side stress before it shows up in LMP. Ties into reserve/ancillary monitoring TODO."
confidence: 3
status: "todo"
original_source_path: ""
original_url: ""
---

<!-- TODO: Investigate ACE as a leading indicator for RT scarcity/shortage pricing events -->
<!-- NOTE: Triggered by 4/6 HE20 $3,169/MWh RT spike with persistent negative ACE over evening peak -->

# Area Control Error as a Scarcity Leading Indicator

## Question / Hypothesis

Can PJM's area control error (ACE) data serve as a leading indicator for RT price spikes and shortage pricing? On 4/6, ACE was consistently negative over the evening peak before RT hit $3,169/MWh at HE20 — suggesting generation was struggling to ramp ahead of the net load peak. If negative ACE patterns are detectable 1-2 hours before scarcity pricing, this could be a real-time trading signal.

## Event Context (2026-04-06)

- **RT price**: $3,169/MWh at HE20 (shortage pricing)
- **Load**: Slightly under RTO forecast — not demand-driven
- **Supply-side pressures**:
  - Strong generation outages including multiple nuclear refuels
  - Rapidly falling wind generation over the evening ramp
  - Rising exports tightening the supply stack
- **ACE signal**: Consistently negative over the evening peak — generation failing to keep pace with net load ramp

## Research Notes

### Questions to Investigate
- Where is ACE data available? (PJM Data Miner 2, real-time feeds?)
- What is the typical ACE threshold that precedes shortage pricing?
- How much lead time does negative ACE provide before LMP spikes?
- Is this signal distinct from what reserve data shows? (compare with `pjm-reserves-ancillary` TODO)
- How often does negative ACE persist without triggering shortage pricing (false positive rate)?

### Related Factors
- Nuclear refuel season (spring) reduces baseload, narrows the ramp margin
- Wind drop-off during evening peak is a recurring pattern — ACE may capture the net effect better than monitoring wind alone
- Export ramp timing — when does MISO/NYISO pull increase relative to PJM's evening peak?

## Data Sources

- PJM Data Miner 2 — ACE data availability TBD
- PJM RT LMP (5-min) for spike timing
- PJM wind forecast vs actuals
- Generation outage schedule

## Conclusion

*Pending investigation.*
