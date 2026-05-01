---
timestamp_local: "2026-03-10 00:00 ET"
timestamp_utc: "2026-03-10T04:00:00Z"
market: "power"
source: "FERC"
tags: [ferc, congestion]
summary: "FERC Order 881 mandates AARs and requires RTOs to accept voluntary DLRs; ANOPR signals mandatory DLRs next for congested corridors"
signal_relevance: "DLR adoption on congested PJM lines could materially reduce congestion pricing and shift FTR values"
confidence: 3
status: "logged"
original_source_path: "PJM/999-Reading/2026-03-10-ferc-order-881-dlr.pdf"
original_url: ""
---

# FERC Order 881 — Dynamic Line Ratings (DLR)

## Summary

FERC Order 881 requires RTOs to use ambient-adjusted ratings (AARs) and accept dynamic line ratings (DLRs) from transmission owners who choose to deploy them. An ANOPR issued June 2024 signals mandatory DLRs are coming for congested, windy corridors.

## Key Points

### Order 881's DLR Provision

FERC Order 881 has two tiers:

1. **AARs (Mandatory)** — What PJM just implemented. Adjusts line ratings hourly based on ambient air temperature only.
2. **DLRs (Voluntary, but RTOs must accept them)** — RTOs/ISOs are required to build and maintain the systems and procedures so that if a transmission owner *chooses* to submit dynamic line ratings, the RTO can actually ingest and use them in dispatch/markets. Without this requirement, a transmission owner investing in DLR sensors would have no way to get those ratings into the market — their more accurate ratings would just be ignored.

The logic: FERC recognized that mandating DLRs everywhere would be premature (expensive sensors on every line), but they didn't want RTOs to be a bottleneck if individual transmission owners wanted to adopt DLR on their own.

### What DLRs Add Beyond AARs

AARs only account for temperature. DLRs use real-time sensors on the actual conductors to measure:
- Conductor temperature/sag/tension
- Wind speed and direction (cooling effect)
- Solar heating based on sun position and cloud cover

This captures significantly more capacity — a windy, cloudy day could allow far more power flow than temperature alone would suggest.

### FERC Is Moving Toward Mandatory DLRs

In June 2024, FERC issued an **Advance Notice of Proposed Rulemaking (ANOPR)** exploring mandatory DLR requirements for congested, windy transmission lines — specifically requiring ratings to reflect:
- Solar heating (sun position + cloud cover)
- Wind speed and direction forecasts

This hasn't become a final rule yet, but the direction is clear: AARs are the stepping stone, DLRs are next for high-value congested corridors.

## Trading Implications

- DLR adoption on congested PJM lines (e.g., N-S corridors, Penelec) could increase transfer capability and reduce congestion rents
- FTR holders on lines that get DLR may see reduced congestion value
- Windy/cloudy conditions would increase line ratings under DLR, potentially relieving constraints that currently drive price separation
- Timeline uncertainty — ANOPR not yet a final rule, but PJM already has AAR infrastructure in place

## Sources

- [FERC Rule to Improve Transmission Line Ratings](https://www.ferc.gov/news-events/news/ferc-rule-improve-transmission-line-ratings-will-help-lower-transmission-costs)
- [Federal Register: Implementation of Dynamic Line Ratings](https://www.federalregister.gov/documents/2024/07/15/2024-14666/implementation-of-dynamic-line-ratings)
- [FERC Seeks Comment on Potential DLR Framework](https://www.ferc.gov/news-events/news/ferc-seeks-comment-potential-dlr-framework-improve-grid-operations-fact-sheet)
- [Ampacimon: FERC Order 881 Handbook for DLR Implementation](https://www.ampacimon.com/news/ferc-order-881-comprehensive-handbook-for-dynamic-line-rating-implementation)
