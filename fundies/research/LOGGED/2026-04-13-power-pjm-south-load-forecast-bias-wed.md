---
timestamp_local: "2026-04-13 09:00 ET"
timestamp_utc: "2026-04-13T13:00:00Z"
market: "power"
source: ""
tags: [power, pjm, load]
summary: "PJM South load forecast likely too bearish for Wednesday — region peaking below 20 GW despite clear skies and 90+ temps for multiple consecutive days. Overnight lows only in 70s will reduce structural cooling, increasing cooling load. Low dew points (50s) will limit feels-like uplift vs actual temps. Expect upward revisions; load could overperform South while underperforming West."
signal_relevance: "DA load forecast bias in shoulder season — if PJM under-forecasts Southern load, DA will under-commit generation in DOM/BGE/PEPCO, creating positive DARTs. Consistent with operator-effect-on-congestion research. Multi-day heat buildup is a known driver of forecast misses."
confidence: 3
status: "logged"
original_source_path: ""
original_url: ""
---

# PJM South Load Forecast Bias — Too Bearish for Wednesday (2026-04-13)

## Summary

PJM's current forecast has Southern PJM peaking **below 20 GW** on Wednesday despite clear skies and **90+ temperatures for multiple consecutive days**. This looks too low. Cooling load has already developed this season at lower temperatures, and multi-day heat accumulation — with overnight lows only dropping into the **70s** — will reduce natural structural cooling and sustain demand. Low dew points (**50s**) will limit the humidity amplification of cooling load, keeping feels-like temps near actual air temps. Expect upward forecast revisions; if they don't come, load could overperform in Southern PJM while underperforming in the West.

## Key Points

### Forecast vs Conditions
- PJM South peak forecast: **<20 GW**
- Expected conditions: clear skies, **90+F highs** for multiple consecutive days
- Overnight lows: **only 70s** — insufficient for natural cooling of structures
- Cooling load has already materialized this season at temperatures well below these highs

### Weather Nuance
- **Dew points in the 50s** this week — relatively dry for Southern PJM
- Low humidity means **feels-like ≈ actual air temperature** — no humidity amplifier on cooling load
- In summer, feels-like temperature has a **much stronger impact on load** than in winter because humidity directly drives cooling demand
- The dry air partially caps the upside, but the multi-day heat buildup and warm overnight lows are the dominant factors

### Regional Divergence
- **South**: Load likely to overperform forecast (under-forecast bias)
- **West**: Load may underperform
- Creates potential for **regional DART divergence** — positive DARTs at DOM/BGE/PEPCO nodes, negative at western hubs

## Trading Implications

- If PJM doesn't revise the South forecast upward, DA will under-commit Southern generation → **positive DARTs in DOM/BGE/PEPCO** on Wednesday. This matches the pattern from your operator-effect-on-congestion and pjm-overstated-forecast research.
- Multi-day heat events are a known source of systematic forecast bias — overnight heat retention in structures creates cumulative cooling load that ramps up across consecutive warm days.
- The low dew point is the limiting factor — in a humid 90F event, this would be a much stronger call. Still, the base case is that PJM is too bearish on South.
- Watch for forecast revisions Monday/Tuesday — if PJM adjusts above 20 GW, the edge narrows. If they hold, the overperformance thesis strengthens.

## Sources

- Market observation / weather analysis

## Related

- [[area-controlled-error]] — ACE and scarcity during ramp events (TODO)
- [[pjm-overstated-forecast-mar-18]] — PJM forecast bias in shoulder season (TODO)
- [[operator-effect-on-congestion]] — DA over/under-commitment driving DARTs (TODO)
