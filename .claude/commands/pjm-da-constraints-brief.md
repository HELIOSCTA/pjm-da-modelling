---
description: Generate a PJM DA-released brief — tomorrow's binding constraints, with PSS/E network context and the active-outage intersection that flags structural vs noise.
---

# PJM DA Constraints Brief

Generate a brief covering tomorrow's binding DA transmission constraints
within the broader LMP causal chain:

> Total LMP → System Energy → **Congestion** → **DA Constraints** → **Outages**

System Energy is a single hourly scalar (same across all PJM hubs); the
hub-to-hub LMP spread is entirely Congestion + Loss. The Congestion
component is set by which transmission constraints bind in the DA
solve — and those binding constraints often correspond to (or are
amplified by) currently active transmission outages. This brief sits
at the bottom two rungs of that chain, joined together via the PSS/E
network model.

Run after the DA market clears (~13:00–13:30 EPT, ~11:00–11:30 MST).

## Data sources (in order of preference)

The brief consumes two MCP endpoints from the local FastAPI server at
`http://localhost:8000`:

1. `/views/constraints_da_network?format=json&max_neighbors=5` — DA
   binding constraints for tomorrow, parser-classified
   (DA-coded / RT-EMS / prose-l/o / interface), with PSS/E bus IDs,
   MVA ratings, and 1-hop neighbors per matched constraint.
2. `/views/transmission_outages_network?format=json&max_neighbors=5`
   — currently active outages with the same PSS/E enrichment, used
   for the constraint × outage intersection in Section 4.

Optional date override: pass `target_date=YYYY-MM-DD` on the
constraints endpoint to inspect a non-tomorrow run (e.g., re-running
on a stale day).

If the FastAPI server is down (curl exit 7 / connection refused),
fall back to running the same data path directly via Python:

```bash
cd C:/Users/AidanKeaveny/Documents/github/helioscta-pjm-da-data-scrapes && python << 'EOF'
import json, os
from datetime import date, timedelta
from backend.mcp_server.data import constraints, transmission_outages
from backend.mcp_server.data.constraint_network_match import match_constraints_to_branches
from backend.mcp_server.data.network_match import load_network, match_outages_to_branches
from backend.mcp_server.views.constraints import build_da_network_view_model
from backend.mcp_server.views.transmission_outages import build_network_view_model

today = date.today()
target = today + timedelta(days=1)
base = 'backend/mcp_server/briefings'
buses, branches = load_network()

# DA constraints — tomorrow
da_df = constraints.pull_constraints_da(target)
da_enriched = match_constraints_to_branches(da_df, branches, buses)
da_vm = build_da_network_view_model(da_enriched, branches, target,
                                    top_n=30, max_neighbors=5)

# Active outages — today
out_df = transmission_outages.pull_active()
out_enriched = match_outages_to_branches(out_df, branches, buses)
out_vm = build_network_view_model(out_enriched, branches,
                                   reference_date=today, max_neighbors=5)

def save(subdir: str, data: dict) -> None:
    path = f'{base}/{subdir}'
    os.makedirs(path, exist_ok=True)
    with open(f'{path}/{today.isoformat()}.json', 'w') as f:
        json.dump(data, f, default=str)

save('constraints_da_network', da_vm)
save('transmission_outages_network', out_vm)
print('ok')
EOF
```

Either path drops dated JSON snapshots into per-view subfolders
under `backend/mcp_server/briefings/`:

```
backend/mcp_server/briefings/
├── constraints_da_network/
│   └── 2026-05-01.json
└── transmission_outages_network/
    └── 2026-05-01.json
```

All gitignored except the README.

## Brief structure (output format)

Three sections only — keep it tight. The chain framing (LMP →
Congestion → Constraints → Outages) appears once, in the lead.

### Lead — one sentence framing

A single line tying the brief to the LMP chain and the target date.
E.g.:

> *DA cleared for `<target_date>`. These constraints will set the
> congestion component of tomorrow's LMP at every PJM hub —
> system-energy is a flat hourly scalar, so all hub spread comes
> from binding constraints + losses.*

### Section 1 — Tomorrow's DA: what's binding

The matched + ambiguous constraints from
`/views/constraints_da_network`, filtered to those with non-null
`da_total_price` (i.e., actually binding tomorrow), sorted by
`da_total_price` descending. Show top 12–15.

For each row:
- Constraint (truncated to ~30 chars)
- Contingency (truncated)
- kV
- Route — `parsed_from_station → parsed_to_station` for LINE,
  `parsed_single_station` for XFMR / PS, `interface` label otherwise
- PSS/E bus pair (`from_bus_psse ↔ to_bus_psse`)
- Total $, hours bound, on-peak vs off-peak split
- MVA rating
- Top 2–3 1-hop neighbors (from the `neighbors` list) — these are
  the parallel paths that absorb redirected flow

Group visually if useful by **voltage tier** (500+ kV first, then
345 kV, then ≤230 kV) — voltage drives load-flow impact, so the
high-voltage section deserves prominence even if shadow prices
look smaller in absolute terms there.

Also note the **interface / zone constraints** (e.g., `APSOUTH`,
`BCPEP`) separately — these are zonal aggregations, not single
branches, and PSS/E matching doesn't apply. Include them in a
short "interface constraints" mini-table at the end of Section 1
(constraint name, contingency, total $, hours).

Skip unmatched constraints in this section — they go in Section 4's
caveats.

### Section 4 — Network context: constraints × active outages

The high-signal cross-cut. For each *binding* DA constraint that
matched to PSS/E (matched OR ambiguous), join against active outages
(matched OR ambiguous) by:

1. **Primary join — PSS/E bus**: an active outage's
   `from_bus_psse` or `to_bus_psse` equals the constraint's
   `from_bus_psse` or `to_bus_psse`. Same physical branch — the
   outage *is* the constraint trigger.
2. **Secondary join — same substation**: the outage and constraint
   share a normalized station (compare `parsed_from_station`,
   `parsed_to_station`, `parsed_single_station` from the constraint
   against `from_station`, `to_station`, `station` from the outage,
   uppercased + first-token). Substation-level overlap — the
   outage stresses the same area as the constraint.

For each intersection, output:
- Constraint name + total $ + voltage
- Outage facility + outage_state + days_out / days_to_return
- Join basis (bus-level vs substation-level)
- One-line interpretation: "structural" if bus-level on the same
  branch; "amplified" if substation-level; "coincident" if
  voltage classes differ but same station

This is the section traders care about most — when the same physical
asset is both out and binding, the constraint will likely persist
through the outage window.

After the intersection table, add a brief **caveats** subsection:
- Match-coverage stats from both endpoints (DA-constraints %
  matched, outages % matched).
- Count of unmatched binding constraints + total $ they represent
  (so the trader knows what fraction of tomorrow's congestion
  isn't explainable from this brief).
- Any interface constraints (no-branch class) by name + total $.

### Section 5 — Trading lens (3–5 bullets)

Synthesis only — no new tables. Tie back to the LMP chain.
Examples of bullet shapes:
- Biggest expected DA congestion driver: `<constraint name>` at
  `<region>`, total $ `<X>`, voltage `<Y>` — backed by `<active
  outage>` (or noted as un-tied to a current outage).
- Structural cluster: when ≥2 binding constraints map to the same
  substation or share a `from_bus_psse`, call it out.
- Regional bias: which **pricing region** (mapped via the constraint's
  PSS/E `from_bus_psse` zone — AEP/West, Dominion, MidAtl, etc.)
  has the most binding $ tomorrow → INC/DEC framing.
- Voltage-tier note: if the dollars concentrate at 138 kV but the
  500 kV section is light, that's a "local" congestion day vs
  "regional" — different positioning implications.
- Caveat callout: if PSS/E match coverage is below 70% for either
  endpoint, surface it — the brief's signal is weaker.

## Style notes

- Match the user's existing fundies-brief tone in
  `fundies/research/PJM-Morning-Fundies.md` — tight bullets,
  **bold** for numbers, concrete dates with day-of-week,
  "TOMORROW" framing.
- ASCII-only in any tables. Use `→` arrows in narrative prose
  only — not in code-output capture, which can hit cp1252
  issues on Windows.
- Money formatting: `$1,234` (no decimals for shadow-price totals;
  these are sums over many hours so 4-significant-digits is enough).
- Include `target_date` and current `as_of` (today) at top.
- Save the synthesized markdown to
  `backend/mcp_server/briefings/da_constraints_<YYYY-MM-DD>.md`
  where `<YYYY-MM-DD>` is the **target_date** (tomorrow), not
  the run date. One file per target — overwrite if regenerated
  the same day.
- Also offer to:
  - Prepend to `fundies/research/PJM-DA-Constraints.md` (newest
    first) — create the file if absent.
  - Or append a section to today's `PJM-Morning-Fundies.md`
    entry under a "Tomorrow's DA Congestion" header.

## Caveats to surface in-brief

- **Hub-only LMP context**: `pjm_da_modelling_cleaned.pjm_lmps_hourly`
  carries 12 hubs only — no nodal LMPs. The brief frames constraints
  as "what sets congestion at the hubs" but cannot quantify per-hub
  congestion impact without nodal data + a bus → hub mapping.
- **PSS/E model age**: 2021 model. Substations newer than that
  (e.g., MARS2 in DOM-N) won't match. Surface the unmatched count.
- **Parser dialect coverage**: `74 KEWAN`, `94 HAURD` (zone-prefixed
  bus codes), `BURNHAM-MUNSTER2`, `CLOUD TX1 115`, `Chicago-Praxair3
  138`, `Snyder-Sullivan 345` are known PSS/E gaps as of
  2026-05-01. If they show up unmatched, that's expected — flag
  the dollars they represent so the trader knows the brief is
  partial.
- **Match coverage thresholds**: DA-constraints typically lands
  ~80%, outages ~91%. If either drops below 60%, the cross-reference
  in Section 4 will miss real intersections — call it out and
  consider refreshing the `.raw` PSS/E file.

## Reference

The constraint endpoint (and matcher) was built 2026-05-01 — see
`backend/mcp_server/data/constraint_network_match.py` for the
parser dialects and station-prefix matching logic.
