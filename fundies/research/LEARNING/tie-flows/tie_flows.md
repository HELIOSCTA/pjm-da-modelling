---
timestamp_local: "2026-02-26 08:04 ET"
timestamp_utc: "2026-02-26T13:04:00Z"
market: "power"
source: "PJM DataMiner2, internal DB"
tags: [tie-flows, interchange, congestion, miso, loop-flow]
summary: "Scheduled vs actual tie flows — loop flow mechanics, MISO seam, forced outage correlation (r=0.52, 22x congestion)"
signal_relevance: "Forced outages >15 GW = regime shift for congestion; MISO unscheduled flows as leading indicator"
confidence: 5
status: "logged"
---

# PJM Tie Flows: Scheduled vs. Actual

## Data Sources

### PJM DataMiner2: `five_min_tie_flows`
- 5-minute granularity feed with fields: `datetime_beginning_utc`, `datetime_beginning_ept`, `tie_flow_name`, `actual_mw`, `scheduled_mw`
- Related feed `act_sch_interchange` adds an `inadv_flow` (inadvertent flow) column

### Database: `dbt_pjm_v1_2026_feb_19.staging_v1_pjm_tie_flows_hourly`
- Hourly aggregation of 5-minute data
- Columns: `date`, `hour_ending`, `tie_flow_name`, `actual_mw`, `scheduled_mw`
- Coverage: 2025-03-30 to 2026-02-26, ~86,500 rows, 21 tie flow names
- Sign convention: positive = into PJM, negative = out of PJM

## Key Definitions

| Term | Definition |
|---|---|
| **Scheduled MW** | Pre-arranged interchange — energy transactions tagged and confirmed between PJM and a neighboring balancing authority ahead of real-time |
| **Actual MW** | Metered power flow across the physical tie line |
| **Inadvertent Interchange** | The difference between net actual and net scheduled interchange, as determined each hour |
| **Loop Flow / Parallel Path Flow** | Unscheduled power that flows through a control area because electricity follows Kirchhoff's Laws (path of least impedance), not contract paths |

## Why Actual and Scheduled Diverge

### 1. Loop Flow / Parallel Path Flow (dominant cause)
Electricity follows all parallel paths inversely proportional to impedance. When power is scheduled from A to B, it splits across every available route — including through third-party control areas. The classic example is the **Lake Erie Loop**: power circulates Ontario → Michigan → Ohio/Pennsylvania (PJM) → New York → Ontario. This is why MISO-facing ties show massive unscheduled flows.

### 2. Generation/Load Imbalances (inadvertent interchange)
Real-time generation and load never perfectly match schedules. The mismatch is captured by Area Control Error (ACE). In H1 2024, PJM's net inadvertent was ~73 GWh over 6 months.

### 3. Price-Driven Flows
Real-time price differentials across seams cause generators to respond to economic signals, shifting actual flows while schedules are fixed for the operating hour.

## When Divergence Is Worst

- High east-west price spreads
- Heavy MISO-to-PJM transfers
- Extreme weather (load forecast errors)
- Generation outages (redispatch changes flow patterns)
- Transmission congestion (out-of-market operator actions)

## Divergence by Tie Line Category

### Largest divergence: AC ties to MISO (500–1,350 MW avg abs diff)
| Tie | Avg Actual MW | Avg Sched MW | Avg Abs Diff |
|---|---|---|---|
| PJM MISO | -2,136 | -809 | 1,357 |
| NIPS | -1,390 | -52 | 1,338 |
| MECS | 844 | 3 | 1,217 |
| CPLE | 1,074 | -38 | 1,128 |
| AMIL | -68 | 36 | 751 |

These are the heart of the loop flow problem — the meshed AC network lets power flow wherever impedance takes it.

### Moderate divergence: AC ties to Southeast (370–580 MW)
LGEE, ALTW, WEC, TVA, DUKE — driven by PJM-Southeast price differentials and AC mesh effects.

### Tightest match: HVDC / merchant lines (< 4 MW avg abs diff)
| Tie | Avg Actual MW | Avg Sched MW | Avg Abs Diff |
|---|---|---|---|
| SAYR | -626 | -627 | 0.8 |
| LINDEN | -285 | -286 | 1.7 |
| HTP | -532 | -531 | 3.8 |

HVDC converters and back-to-back facilities are **physically controllable** — they can be set to exactly match schedules. Parallel path flow doesn't affect DC lines.

### System-wide: PJM RTO
PJM RTO net: avg -3,537 MW actual vs -3,552 MW scheduled (avg abs diff 74 MW). PJM is a net exporter on average.

## How Divergence Is Managed

- **Phase Angle Regulators (PARs)**: Physical transformers that steer AC power flow. The Michigan-Ontario PARs at Bunce Creek took 12 years to deploy and significantly reduced Lake Erie loop flows.
- **Dynamic interface pricing**: PJM prices interfaces using actual system conditions, not just scheduled paths.
- **ACE correction**: Continuous monitoring with unilateral/bilateral payback to reduce accumulated inadvertent.
- **Coordinated congestion management**: PJM and MISO jointly manage seam constraints.

## Relationship to LMP Congestion and Outages

### The LMP Congestion Formula

At every node in PJM, the congestion component is:

```
Congestion_price = Σ (shadow_price_k × shift_factor_k)
```

For each binding transmission constraint *k*:
- **Shadow price** = the cost ($/MWh) to relieve that constraint by 1 MW
- **Shift factor** = how much a node contributes to flow on that constraint (−1 to +1)

Example: On Aug 30, 2024, the LENOX 115kV node had a $2,000 shadow price × 0.611 shift factor = **$1,222/MWh congestion** at that single node.

### The Causal Chain

```
Outages / high load → fewer transmission paths → constraints bind
→ shadow prices spike → congestion component rises at affected nodes
→ price differentials across PJM-MISO seam change
→ actual tie flows shift (responding to price signals + physics)
→ unscheduled flows increase
```

### Forced Outages Drive Congestion (r = 0.52)

| Forced Outage Bucket | Days | Avg Western Hub RT Congestion | Avg MISO Unscheduled MW | Avg Western Hub LMP |
|---|---|---|---|---|
| High (>15 GW) | 23 | **$28.00/MWh** | −1,319 | **$199.81** |
| Mid (8–15 GW) | 112 | $4.75 | −1,428 | $58.09 |
| Low (<8 GW) | 30 | $1.26 | −936 | $41.58 |

High forced outage days see ~22x the congestion and ~5x the LMP vs. low forced outage days.

### Total Outages Inversely Correlate with Congestion (r = −0.30)

| Total Outage Bucket | Days | Avg Congestion | Avg Forced MW |
|---|---|---|---|
| High (>55 GW) | 41 | −$0.03 | 10,029 |
| Mid (35–55 GW) | 27 | $3.05 | 9,307 |
| Low (<35 GW) | 97 | $11.67 | 11,610 |

High total outages peak in the **shoulder season** (Oct–Nov) when load is low and planned maintenance is high — spare capacity means constraints rarely bind. Forced outages peak in **winter** when load is already stressed.

### Unscheduled Flows vs. Congestion by Hub

| Hub | Corr (MISO unscheduled vs RT congestion) |
|---|---|
| Chicago / N Illinois hubs | **+0.06 to +0.14** |
| AEP-Dayton / Ohio hubs | **+0.07 to +0.12** |
| Eastern / NJ hubs | **−0.06 to −0.01** |
| Western Hub | **−0.09** |

Western PJM hubs (AEP, Chicago, Ohio) see *positive* correlation — unscheduled MISO imports increase congestion at seam-adjacent nodes. Eastern hubs see the *opposite* — MISO imports add supply and relieve eastern congestion.

### Why Correlations Are Moderate, Not Strong

1. **Congestion is hyperlocal** — depends on *which specific constraint* binds, not aggregate flows. Hub-level LMPs average over many nodes.
2. **Tie flows are an effect, not just a cause** — they respond to congestion (price signals pull power toward high-price areas), creating a feedback loop.
3. **Outages are the trigger, not the flow** — a single 500 MW forced outage on a critical line can spike congestion far more than a 3,000 MW change in aggregate tie flows.
4. **Shadow prices are nonlinear** — a constraint at 99% loading has zero congestion cost; at 100.1% it can hit the $2,000/MWh penalty factor.

### Key Insight for Forecasting

The relationship is **regime-dependent**, not linear:
- **Normal conditions**: Tie flows and congestion are loosely related. Loop flows are persistent background noise.
- **Stress events** (winter cold snaps, forced outages): Forced outages remove supply → constraints bind → shadow prices spike → congestion jumps → price differentials widen → tie flows shift. The Feb 2026 cold snap shows this clearly: forced outages >18 GW, Western Hub congestion $20–65/MWh.

## References

- [PJM DataMiner2 – Five Minute Tie Flows](https://dataminer2.pjm.com/feed/five_min_tie_flows/definition)
- [PJM DataMiner2 – Actual/Schedule Summary](https://dataminer2.pjm.com/feed/act_sch_interchange/definition)
- [PJM State of the Market – Interchange (2024 Q2)](https://www.monitoringanalytics.com/reports/PJM_State_of_the_Market/2024/2024q2-som-pjm-sec9.pdf)
- [Harvard HEPG – Parallel Path Flow](https://hepg.hks.harvard.edu/faq/parallel-path-flow)
- [Grid Status – Tariffs & the Interconnected Northeast](https://blog.gridstatus.io/tariffs-challenge-the-interconnected-northeast/)
- [APPRO – Michigan PARs for Loop Flow](https://magazine.appro.org/news/ontario-news/2521-michigan-phase-angle-regulators-to-help-with-loop-flow-.html)
- [FERC – Balancing Authority Control & Inadvertent Interchange](https://www.ferc.gov/sites/default/files/2020-05/E-5_21.pdf)
- [PJM Manual 12: Balancing Operations](https://www.pjm.com/-/media/DotCom/documents/manuals/m12.ashx)
- [NARUC – Electric Transmission Seams Primer](https://pubs.naruc.org/pub/FA86CD9B-D618-6291-D377-F1EFE9650C73)
- [Yes Energy – What Is a Locational Marginal Price?](https://www.yesenergy.com/blog/what-is-a-locational-marginal-price)
- [PCI Energy – Shadow Pricing in Energy Markets](https://www.pcienergysolutions.com/2025/02/06/shadow-pricing-in-energy-markets-what-it-is-why-it-matters/)
- [PCI Energy – Understanding LMP & Congestion](https://www.pcienergysolutions.com/2025/01/17/understanding-locational-marginal-pricing-lmp-congestion-in-iso-markets/)
- [Grid Status – Analyzing LMPs with a Nodal Price Map](https://blog.gridstatus.io/analyzing-lmps-with-a-nodal-price-map/)
- [PJM – LMP Formation & Reserve Shortage Pricing](https://www.pjm.com/-/media/DotCom/markets-ops/energy/real-time/reserve-shortage-pricing-paper.pdf)
