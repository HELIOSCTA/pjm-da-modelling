---
timestamp_local: "2026-02-27 10:48 ET"
timestamp_utc: "2026-02-27T15:48:00Z"
market: "power"
source: "FERC filing"
tags: [co-location, datacenter, ferc, regulatory]
summary: "PJM FERC filing to amend co-location rules — 50 MW threshold, netting, interconnection requirements"
signal_relevance: "Structural change to how data centres connect to PJM grid"
confidence: 4
status: "logged"
original_source_path: "PJM/@Reading/E-1-RM20-16-000 _ Federal Energy Regulatory Commission.pdf"
---

# PJM Co-Location Rules (Proposed FERC Filing)

PJM filed with FERC to amend how co-location works — where a data centre sits directly next to (or "behind the meter" of) a power generator, drawing electricity without relying on the broader grid.

## Islanded vs Co-Located

An **islanded** setup means a data centre and its dedicated power plant operate as a self-contained electrical island — completely disconnected from the PJM grid. The generator produces power, the data centre consumes it, and no electricity flows to or from the broader transmission network.

Data centres pursue islanding to skip the interconnection queue (which can take years), avoid grid charges, and operate outside PJM oversight. The downside is no grid backup, no ability to sell excess power, and full dependence on on-site equipment.

## Key Rules in the Proposal

- **50 MW threshold**: Any existing or planned generator larger than 50 MW serving load behind-the-meter must sign an interconnection agreement with PJM, classifying it as a "Co-Located Generating Facility."
- **Netting**: Co-located load can net the capacity value of the on-site generator against transmission and other grid costs — reducing charges proportionally to the generation you bring on-site.
- **Transmission service tiers**: The filing establishes different levels of transmission service for co-located loads (specifics not yet included).

## Netting Explained

Without netting, a data centre co-located with a generator still owes full transmission and capacity charges as if it were pulling all power from the grid. With netting, the capacity value of the on-site generator is subtracted from those obligations. This is the **carrot** — PJM offers reduced grid costs in exchange for the **stick** of requiring an interconnection agreement and market participation.

| Approach | Grid costs | PJM oversight | Queue risk |
|---|---|---|---|
| Fully islanded | None | None | None |
| Co-located under new rules | Reduced (netted) | Yes | Some |
| Standard grid connection | Full | Yes | Full queue wait |

## Impact

Over 55% of data centres currently under construction in PJM are larger than 50 MW, rising to ~75% for planned projects. The vast majority of upcoming data centre capacity would fall under these rules.

PJM's goal is to bring large islanded setups back under its umbrella for grid planning and reliability purposes, while offering financial incentives to make participation worthwhile.
