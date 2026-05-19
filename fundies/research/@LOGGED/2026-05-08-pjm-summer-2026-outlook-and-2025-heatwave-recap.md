---
timestamp_local: "2026-05-08 ET"
timestamp_utc: "2026-05-08T00:00:00Z"
market: "power"
source: "PJM Inside Lines + Grid Status blog"
tags: [pjm, summer-outlook, demand-response, capacity, datacenter, dominion, scarcity, heatwave]
summary: "PJM Summer 2026 Outlook (released 2026-05-07): forecast peak 156.4 GW, plausible-extreme 169.1 GW vs ~180.2 GW installed gen + 7.8 GW DR. PJM warns tightening reserve margins mean less ability to export to neighbors during emergencies. Companion: Grid Status recap of June 23-24, 2025 Eastern Interconnect heat wave — PJM hit 161.3 GW (4th highest ever), reserves fell to 4.1 GW, RT prices touched $3,700/MWh above the $2,000 offer cap, DR called in 12/15/11 zones Mon/Tue/Wed."
signal_relevance: "Sets the supply/demand frame for summer 2026 trading. Headline numbers: 156.4 GW expected, 169.1 GW tail, ~7.8 GW DR cushion, 161.3 GW one-year-ago print already 4th highest ever. Margin = installed (~180.2) - extreme peak (169.1) = ~11 GW before DR — thinnest in 15+ years. Confirms (a) DR will be the marginal price-setter on the hot tail, (b) PJM no longer reliably an exporter on hot days (basis to NYISO/MISO neighbors compresses on heat), (c) Dominion remains the named load-growth driver. Frames the 'spring 2026 more frequent shortages than 2025' thesis from the 4/6 reserve scarcity logged entry."
confidence: 5
status: "logged"
original_source_path: ""
original_url: "https://insidelines.pjm.com/summer-outlook-2026-pjm-prepared-to-meet-growing-summer-demand-with-adequate-resources/ ; https://blog.gridstatus.io/early-heat-stressed-the-eastern-interconnect/"
---

# PJM Summer 2026 Outlook + 2025 Eastern Interconnect Heat Wave Recap (2026-05-08)

## Summary

PJM released its **Summer 2026 Outlook on 2026-05-07** projecting a baseline peak of **156.4 GW** with an extreme-but-plausible upside of **169.1 GW**, against ~**180.2 GW** of installed generation and ~**7.8 GW** of contracted demand response. PJM explicitly flagged tightening reserve margins driven by data-center load growth outpacing new generation, and warned that the RTO **may not be able to export to neighbors** to the same degree as in past summers.

The companion Grid Status recap (published 2025-06-27) documents how the **June 23-24, 2025 heat wave** stressed the Eastern Interconnect: PJM hit **161.3 GW** (4th highest ever; first 160+ print since the early 2010s), operational reserves fell to **4.1 GW**, and prices breached the $2,000/MWh offer cap up to **$3,700/MWh** in real-time. NYISO Long Island cleared **>$9,000/MWh**.

## PJM Summer 2026 Outlook — Key Numbers

| Metric | Value |
|--------|-------|
| Forecast peak load | **156.4 GW** |
| Plausible-extreme peak | **169.1 GW** |
| Installed generation | ~**180.2 GW** |
| Contracted demand response | **7.8 GW** |
| All-time record (2006) | 165.6 GW |
| 2025 actual peaks | 161.3 GW (Jun 23), 160.9 GW (Jun 24) |
| Non-emergency DR calls (2025) | **6** |

### Margin math
- Installed - extreme peak = **~11 GW** raw headroom before DR
- Add 7.8 GW DR → ~19 GW theoretical cushion at the tail
- Smallest gap (capacity scheduled vs forecast) in **~15 years** per Grid Status note on Jun 24 (165.2 vs 161.1)

### What PJM said
- Michael Bryson (Sr. VP Operations): *"continued load growth driven by data centers that is outpacing the addition of new generation."*
- *"PJM will have fewer resources to export electricity to neighboring systems during emergencies as a result of tightening reserve margins."*
- Hotter-than-normal regions called out: mid-Atlantic, southern states, West Virginia, Kentucky, Tennessee.

## Grid Status Recap — June 23-24, 2025 Heat Wave

### Demand
- PJM peak: **161.3 GW** (Jun 23) / 160.9 GW (Jun 24) — 4th highest ever, first 160+ since early 2010s
- Operational reserves on Jun 24 fell to **4.1 GW** with only **165.2 GW** scheduled vs **161.1 GW** forecast
- Data centers (esp. DOM) called out as the swing factor moving the event from "7th highest" to "4th highest"

### Pricing
| RTO | Peak observed |
|-----|---------------|
| PJM RT | **$3,700/MWh** (above $2,000 offer cap) |
| ISO-NE DA | $473/MWh, RT >$1,000/MWh |
| NYISO Long Island | **>$9,000/MWh** |
| NYISO zone | >$7,000/MWh |
| IESO | $2,000/MWh ceiling hit |

### Demand Response
- PJM DR activated in **12 zones Mon / 15 Tue / 11 Wed**
- NYISO activated SCRs

### Lessons (per author)
1. Eastern Interconnection wheeling capability mattered — neighbors leaned on each other
2. Storage gap on the net-load ramps is the structural problem
3. "This is only June" — temperatures typically peak later in summer

## Trading Implications

- **Tail risk priced in DA caps and RT scarcity adders.** With 11 GW raw + 7.8 GW DR cushion vs a 169 GW tail, summer 2026 will retest the $3,700/MWh print from 2025. RCSTF caps don't help — earliest go-live winter 2027 ([[2026-03-20-ea-power-pjm-reserve-reform-lmp-impact]]).
- **Exporter→non-exporter regime shift.** PJM saying it may not export to neighbors is the explicit signal that NYISO/MISO basis compresses (or flips) on heat. This is a re-rating of the historical PJM→NYISO export-tie spread on hot days.
- **Dominion concentration continues.** Combine this with [[2026-04-13-power-pjm-rbp-15gw-connect-manage]] (DOM 4.9 GW RBP target, 33% of total) and the [[2026-04-07-power-datacenter-cloud-xf-dominion-congestion]] Cloud XF binding behavior — DOM stays the named risk zone for both load and congestion.
- **DR is the marginal price-setter on the tail.** 7.8 GW DR vs ~11 GW raw margin means DR gets called every event. Watch for DR exhaustion behavior (per-zone caps, hours-used limits) — that's where prices break to the offer cap.
- **Spring 2026 more frequent shortages than 2025 thesis intact.** [[2026-04-07-ea-power-pjm-reserve-scarcity-apr6]] called this; the Summer Outlook formalizes it. The 4/6 reserve scarcity event was the leading indicator.
- **Storage build pace matters.** Multi-hour ramp scarcity is the structural problem (4/6 saw 14 GW net-load ramp over 2 hours). Summer 2026 will surface this on every cloudy-then-clearing late-afternoon — Dominion + the BESS pilots ([[2026-04-10-power-dominion-darbytown-iron-air-bess-pilot]]) are too small to matter this season.

## Sources

- PJM Inside Lines, "Summer Outlook 2026: PJM Prepared to Meet Growing Summer Demand with Adequate Resources" (2026-05-07)
- Grid Status blog, "Early Heat Stressed the Eastern Interconnect" (2025-06-27)

## Related

- [[2026-04-07-ea-power-pjm-reserve-scarcity-apr6]] — 4/6 RT scarcity event, $3,300 RT print, 14 GW net-load ramp; spring 2026 more frequent shortages thesis
- [[2026-04-13-power-pjm-rbp-15gw-connect-manage]] — RBP 14.9 GW target, DOM 4.9 GW (33%) is named load-growth zone
- [[2026-03-20-ea-power-pjm-reserve-reform-lmp-impact]] — RCSTF reserve reform, price cap lift to $4,100, earliest winter 2027 — won't help summer 2026
- [[2026-04-09-power-pjm-synch-reserve-performance-inquiry]] — Mar 1 synch reserve 72% response, 704 MW shortfall — DR/reserve performance question entering peak season
- [[2026-04-09-power-pjm-dlr-updates]] — 34 DOM 230kV DLR lines go-live 2026-08-01 — peak summer overlap
