---
timestamp_local: "2026-04-09 09:00 ET"
timestamp_utc: "2026-04-09T13:00:00Z"
market: "power"
source: "PJM"
tags: [power, pjm, congestion, ferc]
summary: "PJM DLR facilities list as of 4/1/2026: 34 Dominion 230kV facilities projected go-live 8/1/2026, all in Northern Virginia data center corridor (Ashburn, Beaumeade, Sterling Park, Goose Creek, etc). 3 DLCO 138/345kV facilities delayed indefinitely. DLR implementation follows FERC Order 881 mandate."
signal_relevance: "Dominion DLR go-live in August 2026 will increase dynamic transfer capacity on Northern Virginia 230kV lines — could partially relieve datacenter-driven congestion in data center alley. But won't help with Southern Dominion constraints (Cloud XF, Easters). Monitor for congestion relief on AD-West interface post-August."
confidence: 4
status: "logged"
original_source_path: "PJM/999-Reading/2026-04-09-power-pjm-dlr-updates.pdf"
original_url: "https://www.pjm.com/-/media/DotCom/committees-groups/committees/oc/2026/20260409/20260409-item-12---dlr-updates.pdf"
---

# PJM — Projected TO DLR Facilities List Update (OC, 2026-04-09)

## Summary

PJM's DLR facilities list (as of 4/1/2026) shows **34 Dominion 230kV facilities** projected to go live with Dynamic Line Ratings on **8/1/2026** (one facility 12/1/2026). All are in the **Northern Virginia data center corridor** — Ashburn, Beaumeade, Sterling Park, Goose Creek, Dranesville, Brambleton, etc. Three DLCO (Duquesne) 138/345kV facilities remain indefinitely delayed.

## Key Points

### Dominion 230kV DLR Facilities (34 lines)

All announced between 1/30/2025 and 11/25/2025. Projected go-live **8/1/2026** (update required by 6/1/2026). Key corridors:

**Ashburn cluster**:
- Ashburn – Beaumeade 227/274
- Ashburn – Goose Creek 227
- Ashburn – Pleasant View 274

**Sterling Park / Dranesville cluster**:
- Dranesville – Sterling Park 2079
- Paragon Park – Sterling Park 2081/2150
- Davis Drive – Sterling Park 2194
- Dranesville – Reston 2062
- Reston – Hunter 264

**Brambleton / Evergreen Mills cluster**:
- Brambleton – Evergreen Mills 2172
- Brambleton – Poland Road 2183
- Evergreen Mills – Yardley Ridge 2209

**Data center-adjacent**:
- Beco – Pacific / DTC / Paragon Park (2165/2207/2249)
- Aviator – Celestia 2137
- Celestia – Sojourner 2261
- Shellhorn – Sojourner 2218
- Cabin Run – Shellhorn / Yardley Ridge (2095/2213)
- Cumulus – Buttermilk 2203
- Buttermilk – Roundtable / Pacific (2214/2170)
- Clark – Davis Drive 2033
- Bull Run – Clifton 2212
- Belmont – Goose Creek
- Hunter – Clark 2005

**Later go-live**:
- Beaumeade – Buttermilk 2152: **12/1/2026** (update by 10/1/2026)

### DLCO (Duquesne) — Delayed
- 138kV Crescent – North Z-20, Z-21
- 345kV Collier – Tidd 301
- All three **delayed indefinitely** (announced 1/11/2023)

### Implementation Timeline
Per Manual M-03, Section 2.1.1.2:
- Facilities announced Jan 2025: implementation window Aug 2025 – Jul 2026
- Facilities announced Feb-Nov 2025: implementation window Jun 2026 – May 2027
- Updates required by 6/1/2026 for most DOM facilities

### Where to Find Active DLR Data
- Permanent/Temporary Lookup Tables (search "DLR: Y")
- Real-time DLR values
- Effective Ratings (includes DLR forecasts)
- All accessible via PJM Ratings Information page

## Trading Implications

- **All 34 DLR facilities are in Northern Virginia** — the heart of data center alley. DLR go-live on 8/1/2026 will allow real-time thermal capacity adjustments on these 230kV lines, potentially increasing transfer capacity during favorable weather.
- **This targets the Ashburn/Loudoun County corridor** specifically — the same area driving AD-West interface utilization. If DLRs materially increase capacity, it could partially relieve congestion on the west-east interface for NQ26+.
- **Does NOT address Southern Dominion** — the Cloud XF / Easters constraints you just logged are further south (Boydton area). Those facilities aren't on the DLR list.
- **DLCO delays are notable** — the Collier-Tidd 345kV line is a key AEP/Duquesne interface. Indefinite delay means no DLR relief for western PJM congestion.
- **Monitor June 1 update deadline** — DOM must confirm go-live schedules by 6/1/2026. Any further delays compress the NQ26 congestion relief timeline.

## Sources

- PJM Operating Committee, April 9, 2026, Item 12
- PJM Manual M-03, Section 2.1.1.2

## Related

- [[2026-03-10-ferc-order-881-dlr]] — FERC Order 881 mandating AARs, voluntary DLRs; ANOPR signals mandatory DLRs
- [[2026-04-02-ea-power-ad-west-spread-congestion]] — AD-West spread driven by Dominion load, limited tx expansion before 2029
- [[2026-04-07-power-datacenter-cloud-xf-dominion-congestion]] — Cloud XF constraint in Southern Dominion (not covered by these DLRs)
