---
description: Generate a PJM transmission outages morning brief — network-context first, then Active / Starting / Ending detail sections.
---

# PJM Transmission Outages Brief

Generate a daily brief covering active / upcoming / returning transmission
outages, framed by the PJM PSS/E network model context.

## Pre-flight: always-fresh MCP server

**First step — always.** Run the pre-flight before anything else:

```bash
python -m backend.mcp_server.ensure_running
```

The script kills any process bound to port 8000 (whether it's the
existing MCP server or a stale uvicorn), spawns a fresh detached
uvicorn against the current code on disk, and waits up to 30s for
`/openapi.json` to respond.

- Exit 0 → MCP is healthy. Continue with the data-source steps below.
- Exit non-zero → **STOP IMMEDIATELY.** Tell the user the server failed
  to come up and point them at the log:
  `backend/mcp_server/logs/server.log`. Do not synthesize a brief.

Do not call view builders directly via Python under any circumstance.
There is no fallback path — if MCP can't be brought up, this command
produces no output.

## Data sources

The brief consumes four MCP endpoints from `http://localhost:8000`:

1. `/views/transmission_outages_network?format=json&max_neighbors=5`
   — match coverage + matched/ambiguous/unmatched outages with bus IDs
   and 1-hop neighbors
2. `/views/transmission_outages_active?format=json` — regional summary +
   notable outages
3. `/views/transmission_outages_window_7d?format=json` — 7-day forward
   outlook (locked + planned)
4. `/views/transmission_outages_changes_24h_simple?format=json` — last
   24h NEW + REVISED tickets

After hitting each endpoint, save the JSON response into per-view
subfolders under `backend/mcp_server/briefings/`:

```
backend/mcp_server/briefings/
├── transmission_outages_active/
│   └── 2026-05-01.json
├── transmission_outages_window_7d/
│   └── 2026-05-01.json
├── transmission_outages_changes_24h_simple/
│   └── 2026-05-01.json
└── transmission_outages_network/
    └── 2026-05-01.json
```

All gitignored except the README.

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

- **Substation hotspots**: substations with ≥2 concurrent active outages
  meeting *either* (a) max kV ≥ 345, OR (b) any risk-flagged ticket
  regardless of voltage. The risk-flag escape hatch matters because
  some of the most important constraints are 230 kV double-circuit
  events on tie corridors (e.g., GRACETON-MANOR). Show as a table:
  substation, # outages, max kV, risk count, types.
- **Implicit hotspots**: when outages at *different* substations all
  converge on the same other bus (e.g., multiple lines into ELMONT4).
  Look for shared `to_bus_psse` across active 500 kV outages.
- **Topology framing**: 2-3 bullets identifying where redundancy is
  *thin* vs *rich*, using the `neighbors` list per matched outage.
- **Network gaps**: count of unmatched outages and a representative
  list. Two sub-counts: ≥345 kV unmatched (model-newness gap), AND
  any-kV unmatched with `risk_flag=True` (these must be surfaced
  even though we can't compute their topology). Don't drop unmatched
  risk-flagged outages from the brief just because they aren't in
  PSS/E — list them by name with their schedule.

### 2. Active — currently in effect

**Two sub-tables, in this order:**

**2a. Top 8-10 outages by `rating_mva`, ≥500 kV.** For each, include a
brief "key alternates" note pulled from the `neighbors` list of the
network endpoint — what's parallel to the outage, with rating.

**2b. Risk-flagged outages below 500 kV (separate table, do not skip).**
Any active outage with `risk_flag=True` and `kV < 500` belongs here —
this is the catch for 230 kV / 345 kV tie-line risks that the rating
sort would otherwise hide. Common examples: GRACETON-MANOR (230 kV,
BC zone), DPL-zone double-circuit work. Include any concurrent
follow-on tickets at the same facility (search the planned/Received
window for the same `from_station/to_station` pair) so a 4/27 → 5/10
event isn't missed when its 5/8 → 6/6 follow-on is also booked.

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
- Save the synthesized markdown to
  `backend/mcp_server/briefings/transmission_outages_<YYYY-MM-DD>.md`
  (top level, gitignored; one file per generation date — overwrite if
  regenerated the same day). Also offer to:
  - Prepend to `fundies/research/PJM-Transmission-Outages.md`
    (newest-first)
  - Or append a section to today's `PJM-Morning-Fundies.md` entry

## Caveats to surface in-brief

- The `_changes_24h_snapshot` endpoint is currently 503-disabled until
  the SCD2 snapshot accumulates ≥24h of history (re-enable target
  ~2026-05-08). Don't rely on its output until then.
- The PSS/E model is from Sept 2021. Substations newer than that
  (e.g., MARS2 in DOM-N) won't match. Surface unmatched count.
- **Consecutive ticket chains**: a single physical outage can be
  represented as a sequence of back-to-back tickets at the same
  facility (e.g., GRACETON-MANOR 4/27 → 5/10 followed by 5/8 → 6/6).
  When summarizing, dedupe-by-facility and report the *combined*
  effective window, plus call out any short overlap windows where
  both tickets are simultaneously active (double-circuit risk).

## Lessons learned

- **2026-05-04 GRACETON miss.** Original brief omitted GRACETON-MANOR
  entirely because (a) hotspot threshold was kV ≥ 345, (b) the line is
  unmatched in PSS/E (model gap), and (c) the Active section was
  capped at ≥500 kV. Fix: hotspot threshold now drops to 230 kV when
  any concurrent ticket is risk-flagged; Active section now includes
  a separate "risk-flagged below 500 kV" sub-table; unmatched
  risk-flagged outages are surfaced by name in Network gaps even when
  topology is unknown.

## Reference example

The first run of this brief produced output saved at
`backend/mcp_server/briefings/network_full.json` etc. The corresponding markdown is
in chat history (see "PJM Transmission Outages Brief — 2026-05-01"
with the network-first structure).
