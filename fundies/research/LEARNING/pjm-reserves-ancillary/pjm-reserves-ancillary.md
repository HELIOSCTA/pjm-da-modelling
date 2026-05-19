---
timestamp_local: "2026-03-26 07:00 ET"
timestamp_utc: "2026-03-26T12:00:00Z"
market: "power"
source: "PJM Data Miner 2"
tags: [power, pjm, reserves, scarcity-pricing]
summary: "Build reserve/scarcity data into the daily fundies pipeline to detect adder events before they show up in LMP"
signal_relevance: "Reserve levels are the leading indicator for RT price spikes — the 3/24 HE22 $220 Dom event was a reserve scarcity adder, not a gas ramp"
confidence: 4
status: "todo"
original_url: "https://dataminer2.pjm.com/"
---

<!-- TODO: Ingest PJM reserve and ancillary services data into the daily pipeline -->
<!-- NOTE: The 3/24 HE22 blowout (Dom $220, system energy $161, EH cong -$149) was driven by a reserve scarcity adder — gas was actually declining at HE22. Without reserve data we're blind to these events until after the price prints. -->

## Question / Hypothesis

Can we detect reserve scarcity events forming 1-2 hours before they hit LMP by tracking real-time reserve levels, synchronized reserve prices, and penalty factor thresholds?

The HE22 3/24 event showed system energy at $161 vs ~$60-65 marginal gas cost — **~$95-105 of pure scarcity adder.** The adder fired because a forced outage (part of the +2 GW surge to 12.5 GW) crashed reserves through the penalty threshold while 500 kV constraints were binding. The hourly data masked the severity — some 5-min intervals were likely $300-500+.

## PJM Data Miner 2 Feeds to Ingest

### Priority 1 — Core Reserve Data

| Feed | Description | Granularity | Pipeline Slot |
|------|-------------|-------------|---------------|
| **`rt_reserve_summary`** | RT reserve MW by product (Synchronized, Primary, 30-min) at RTO + sub-zone | Hourly | `06f-reserves.sql` |
| **`sr_results`** | Synchronized Reserve clearing prices, MW cleared, deficit flags | Hourly | `06f-reserves.sql` |
| **`reserve_penalty_factors`** | Stepped penalty factors applied when reserves breach thresholds | Event-based | Reference table |

### Priority 2 — Shortage Pricing Events

| Feed | Description | Granularity | Pipeline Slot |
|------|-------------|-------------|---------------|
| **`rt_shortage_events`** | Flags hours/intervals where shortage pricing was active | Event-based | `06g-shortage-events.sql` |
| **`rt_ordc_results`** | Operating Reserve Demand Curve — reserve MW level and price at each step | Hourly | `06f-reserves.sql` |
| **`ancillary_services_prices`** | Combined SR, regulation, and reserve prices | Hourly | `06f-reserves.sql` |

### Priority 3 — Granularity Upgrade

| Feed | Description | Why |
|------|-------------|-----|
| **`rt_fivemin_lmp`** | 5-minute RT LMPs | Hourly averages mask the spikes — HE22 $161 avg probably had 5-min intervals at $300-500+ |
| **`gen_outages_by_type`** | Forced/planned/maint by fuel type | Tells you WHICH unit tripped, not just total MW |

## How the Adder Works

PJM's reserve penalty factor is a stepped function added to the **system energy component** of RT LMP:

1. Reserves below Synchronized Reserve Requirement → Step 1 adder (~$300/MWh)
2. Reserves below Primary Reserve Requirement → Step 2 (higher)
3. Extreme shortage → up to **$2,000/MWh** (the offer cap)

Separately, **Transmission Constraint Penalty Factors** inflate the congestion component when constraints bind with no economic redispatch available ($500-$2,000/MWh). This is what drove EH to -$149.49 at HE22.

### Evidence from 3/24 (Tuesday)

| HE | System Energy | Est. Gas Marginal | Implied Adder | Gas MW (07b) |
|----|--------------|-------------------:|---------------:|-------------:|
| 19 | $42.67 | ~$40-45 | ~$0 | 37,977 |
| 20 | $81.01 | ~$55-65 | ~$15-25 | 41,431 |
| 21 | $81.71 | ~$55-65 | ~$15-25 | 43,278 |
| **22** | **$161.16** | ~$55-65 | **~$95-105** | 41,726 (-1,552) |
| 23 | $43.12 | ~$40-45 | ~$0 | 38,227 |
| 24 | $77.68 | ~$55-65 | ~$12-20 | — |

Gas was **declining** at HE22 (-1,552 MW) while prices exploded. Not a dispatch ramp — a forced outage crashing reserves through the penalty threshold.

## Implementation Plan

1. **Check Data Miner 2 API access** — confirm feeds are available via API (not just GUI). Document auth requirements.
2. **Ingest `rt_reserve_summary` + `sr_results`** into postgres. Schema: date, hour_ending, reserve_product, reserve_mw, sr_clearing_price, deficit_flag.
3. **Build `06f-reserves.sql`** — reserve MW by hour (last 3 days) with SR clearing price. Flag hours where SR price > $0 (adder active).
4. **Add to morning entry template** — new line in Market Snapshot: `Reserves: yesterday min sync reserve X MW at HE Y. SR price spiked to $Z.`
5. **Stretch: 5-minute LMP ingestion** — heavy volume. Consider only peak hours (HE7-8, HE19-24) or storing max/min per hour.

## Data Sources

- PJM Data Miner 2: https://dataminer2.pjm.com/
- PJM Manual 11 (Energy & Ancillary Services Market Operations) — ORDC and penalty factor specs
