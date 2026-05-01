---
description: Generate a PJM transmission outages morning brief — network-context first, then Active / Starting / Ending detail sections.
---

# PJM Transmission Outages Brief

Generate a daily brief covering active / upcoming / returning transmission
outages, framed by the PJM PSS/E network model context.

## Data sources (in order of preference)

The brief consumes four MCP endpoints from the local FastAPI server at
`http://localhost:8000`:

1. `/views/transmission_outages_network?format=json&max_neighbors=5`
   — match coverage + matched/ambiguous/unmatched outages with bus IDs
   and 1-hop neighbors
2. `/views/transmission_outages_active?format=json` — regional summary +
   notable outages
3. `/views/transmission_outages_window_7d?format=json` — 7-day forward
   outlook (locked + planned)
4. `/views/transmission_outages_changes_24h_simple?format=json` — last
   24h NEW + REVISED tickets

If the FastAPI server is down (curl exit 7 / connection refused), fall
back to running the same data path directly via Python:

```bash
cd C:/Users/AidanKeaveny/Documents/github/helioscta-pjm-da-data-scrapes && python << 'EOF'
import json
from backend.mcp_server.data import transmission_outages
from backend.mcp_server.data.network_match import load_network, match_outages_to_branches
from backend.mcp_server.views.transmission_outages import (
    build_active_view_model, build_window_7d_view_model,
    build_changes_24h_simple_view_model, build_network_view_model,
)

active_df = transmission_outages.pull_active()
window_df = transmission_outages.pull_window_7d()
changes_df = transmission_outages.pull_changes_24h_simple()
buses, branches = load_network()
enriched = match_outages_to_branches(active_df, branches, buses)

import os
os.makedirs('backend/mcp_server/briefings', exist_ok=True)
with open('backend/mcp_server/briefings/active.json', 'w') as f:
    json.dump(build_active_view_model(active_df), f, default=str)
with open('backend/mcp_server/briefings/window.json', 'w') as f:
    json.dump(build_window_7d_view_model(window_df), f, default=str)
with open('backend/mcp_server/briefings/changes.json', 'w') as f:
    json.dump(build_changes_24h_simple_view_model(changes_df), f, default=str)
with open('backend/mcp_server/briefings/network_full.json', 'w') as f:
    json.dump(build_network_view_model(enriched, branches, max_neighbors=5), f, default=str)
print('ok')
EOF
```

Either path saves four JSON files at `backend/mcp_server/briefings/*.json` for
synthesis. The path is gitignored.

## Brief structure (output format)

Lead with network context, then time-slice into three detail sections.

### 0. Top-line stats table

One table at the top:

| | | |
|---|---:|---|
| Active outages ≥230 kV | <total_active> | scope: LINE/XFMR/PS |
| Located in PSS/E network | <matched + ambiguous> (<match_rate_pct>%) | <matched> unique-matched, <ambiguous> multi-match |
| Currently in effect | <count days_out >= 0 and (days_to_return is null or > 0)> | started, not yet returning |
| Starting next 7 days | <count -7 <= days_out < 0> | flag if there's a single-day cluster |
| Ending next 7 days | <count 0 <= days_to_return <= 7> | call out today's count |

### 1. Network context

- **Substation hotspots**: substations with ≥2 concurrent active outages,
  max kV ≥ 345. Show as a table: substation, # outages, max kV, risk
  count, types.
- **Implicit hotspots**: when outages at *different* substations all
  converge on the same other bus (e.g., multiple lines into ELMONT4).
  Look for shared `to_bus_psse` across active 500 kV outages.
- **Topology framing**: 2-3 bullets identifying where redundancy is
  *thin* vs *rich*, using the `neighbors` list per matched outage.
- **Network gaps**: count of unmatched ≥345 kV outages and a
  representative list (5-7 substations).

### 2. Active — currently in effect

Top 8-10 outages by `rating_mva`, ≥500 kV. For each, include a brief
"key alternates" note pulled from the `neighbors` list of the network
endpoint — what's parallel to the outage, with rating.

Filter: `days_out >= 0` AND (`days_to_return` is None or `days_to_return > 0`)

### 3. Starting — outages that begin in next 7 days

Filter: `days_out` between -7 and -1 (negative because outage hasn't
started yet)

Show as a table sorted by start date:
- 500+ kV section first
- 345 kV section after
- Highlight any single-day clusters (e.g., "Sat 5/4 cluster")
- Flag high-risk new outages with [HR] marker
- Note compound effects: when a starting outage shares a substation
  with an already-active outage, call it out (e.g., "+ already-out
  XFMR at ELMONT4 — substation has 3 simultaneous reductions during
  5/4–5/8")

### 4. Ending — outages returning in next 7 days

Filter: `0 <= days_to_return <= 7`

Two sub-sections:
- **Returning today** (days_to_return == 0) — relief delivered,
  with note on whether parallel paths were already absorbing load
  (use neighbors list to assess)
- **Returning later this week** — sorted by days_to_return ascending

For each ≥500 kV row include rating; for 345 kV summarize by region
+ count if there's a coordinated maintenance window (multiple at same
substation).

### 5. Trading lens

3-5 bullets synthesizing the network + schedule view:
- Identify the single biggest event of the next 7 days (compound effect
  + voltage class + duration)
- Note any chronic structural constraints (>60 days out, ≥500 kV)
- Highlight regional bias (long/short for DA congestion)
- Flag any "noise" returns where parallel paths were already routing
  load (high redundancy = small DA impact)
- Calendar arc: which day of the week is the constraint trough vs peak

## Style notes

- Match the user's existing fundies-brief tone in
  `fundies/research/PJM-Morning-Fundies.md` — tight bullets, **bold**
  for numbers, concrete dates with day-of-week, "TODAY / TOMORROW"
  framing where relevant.
- ASCII-only in any tables (use `→`-style arrows in narrative prose
  only — not in code-output capture, which can hit cp1252 issues on
  Windows).
- Include reference_date at top.
- After the brief, save the markdown to
  `backend/mcp_server/briefings/transmission_outages_<YYYY-MM-DD>.md`
  (gitignored; one file per generation date — overwrite if regenerated
  the same day). Also offer to:
  - Prepend to `fundies/research/PJM-Transmission-Outages.md`
    (newest-first)
  - Or append a section to today's `PJM-Morning-Fundies.md` entry

## Caveats to surface in-brief

- The `_changes_24h_snapshot` endpoint is currently 503-disabled until
  the SCD2 snapshot accumulates ≥24h of history (re-enable target
  ~2026-05-08). Don't rely on its output until then.
- The PSS/E model is from Sept 2021. Substations newer than that
  (e.g., MARS2 in DOM-N) won't match. Surface unmatched count.

## Reference example

The first run of this brief produced output saved at
`backend/mcp_server/briefings/network_full.json` etc. The corresponding markdown is
in chat history (see "PJM Transmission Outages Brief — 2026-05-01"
with the network-first structure).
