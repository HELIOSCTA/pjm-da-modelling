---
description: Generate a PJM DA-released morning brief — 5-tier funnel from zonal LMPs through hourly heatmap, binding constraints, and underlying outages, then a trader-lens synthesis.
---

# PJM DA Results Brief

Generate a daily brief that walks the LMP causal chain top-down, post-DA-release:

> Zonal LMPs (Tier 1) → Hourly heatmap (Tier 2) → Binding constraints (Tier 3) → Underlying outages (Tier 4) → Synthesis (Tier 5)

Run after the DA market clears (~13:30 EPT, ~11:30 MST). The brief
opens at the zonal LMP level, then progressively drills into *why*:
which hours are stressed, which constraints are binding in those
hours, and which transmission outages sit on or near the constraint
topology.

Each tier hands a concrete drilldown payload to the next so the brief
stays causally connected (top zones → top hubs → binding hours →
constraint bus IDs → outages on those buses).

## Data sources (in order of preference)

The brief consumes four MCP endpoints from `http://localhost:8000`:

1. `/views/lmps_daily_summary?format=json&target_date=<tomorrow>` —
   Tier 1. Zonal totals + decomposition + congestion split, top 8
   zones, plus `top_zones_for_drilldown` (5 hubs) → Tier 2.
2. `/views/lmps_hourly_summary?format=json&target_date=<tomorrow>&hubs=<csv>` —
   Tier 2. Hourly DA LMP heatmap, peak-hour callout, plus
   `binding_hours_for_drilldown` (3-5 HEs) → Tier 3.
3. `/views/constraints_da_network?format=json&target_date=<tomorrow>&binding_hours=<csv>&top_n=10&max_neighbors=5` —
   Tier 3. Top-N constraints filtered to binding hours, with PSS/E
   bus IDs. Hands `neighbor_bus_ids` (union of from/to/neighbor
   buses) → Tier 4.
4. `/views/transmission_outages_for_constraints?format=json&bus_ids=<csv>&constraint_labels=<csv>` —
   Tier 4. Outages on or near the bus set from Tier 3.

If the FastAPI server is down (curl exit 7), fall back to running the
data path directly via Python:

```bash
cd C:/Users/AidanKeaveny/Documents/github/helioscta-pjm-da-data-scrapes && python << 'EOF'
import json, os
from datetime import date, timedelta
from backend.mcp_server.data import constraints, lmp, transmission_outages
from backend.mcp_server.data.constraint_network_match import match_constraints_to_branches
from backend.mcp_server.data.network_match import load_network, match_outages_to_branches
from backend.mcp_server.views.constraints import build_da_network_view_model
from backend.mcp_server.views.lmp import (
    build_lmps_daily_summary_view_model,
    build_lmps_hourly_summary_view_model,
)
from backend.mcp_server.views.transmission_outages import (
    build_outages_for_constraints_view_model,
)

today = date.today()
target = today + timedelta(days=1)
base = 'backend/mcp_server/briefings'
buses, branches = load_network()

# Tier 1
daily_df = lmp.pull_lmps_daily(target)
tier1 = build_lmps_daily_summary_view_model(daily_df, target)
top_hubs = tier1['top_zones_for_drilldown']

# Tier 2
hourly_df = lmp.pull_lmps_hourly(target, hubs=top_hubs)
tier2 = build_lmps_hourly_summary_view_model(hourly_df, target, hubs_filter=top_hubs)
binding_hours = tier2['binding_hours_for_drilldown']

# Tier 3
con_df = constraints.pull_constraints_da(target, binding_hours=binding_hours)
con_enriched = match_constraints_to_branches(con_df, branches, buses)
tier3 = build_da_network_view_model(
    con_enriched, branches, target,
    top_n=10, max_neighbors=5, binding_hours=binding_hours,
)

# Tier 4
bus_set = set()
constraint_index = {}
for c in tier3.get('matched_constraints', []):
    for b in c.get('neighbor_bus_ids', []):
        bus_set.add(b)
        constraint_index.setdefault(b, []).append(c['constraint_name'])
out_df = transmission_outages.pull_active()
out_enriched = match_outages_to_branches(out_df, branches, buses)
tier4 = build_outages_for_constraints_view_model(
    out_enriched, branches, sorted(bus_set),
    constraint_index=constraint_index, reference_date=today,
)

def save(subdir: str, data: dict) -> None:
    path = f'{base}/{subdir}'
    os.makedirs(path, exist_ok=True)
    with open(f'{path}/{today.isoformat()}.json', 'w') as f:
        json.dump(data, f, default=str)

save('lmps_daily_summary', tier1)
save('lmps_hourly_summary', tier2)
save('constraints_da_network', tier3)
save('transmission_outages_for_constraints', tier4)
print('ok')
EOF
```

Both paths drop dated JSON snapshots into per-view subfolders under
`backend/mcp_server/briefings/`:

```
backend/mcp_server/briefings/
├── lmps_daily_summary/<YYYY-MM-DD>.json
├── lmps_hourly_summary/<YYYY-MM-DD>.json
├── constraints_da_network/<YYYY-MM-DD>.json
└── transmission_outages_for_constraints/<YYYY-MM-DD>.json
```

All gitignored except the README.

## Brief structure

### 0. Top-line stats table

5-row scan-in-3-seconds table:

| | | |
|---|---:|---|
| Zonal LMP range (onpeak) | `$<min>` – `$<max>` (`<spread>`) | `<low_zone>` to `<high_zone>` |
| RTO total / system energy / congestion | `$<rto_total>` / `$<sys>` / `$<cong>` | congestion = `<pct>%` of total |
| Top zone by \|congestion\| | `<zone>` | `$<onpeak_cong>`, `<pct>%` of LMP |
| Top binding constraint | `<constraint>` | `$<total>`, `<hours>` HE, `<kV>` kV |
| Top affected outage | `<facility>` | `<kV>` kV, `<days_out>`d, returns `<eta>` |

### 1. Day overview (Tier 1 — zonal LMPs)

From `/views/lmps_daily_summary`. Sets the regional bias for tomorrow.

- One-line frame: `<target_date> (<dow>) DA cleared. RTO total $X
  (onpeak) / $Y (offpeak), system energy $Z. Congestion is N% of total
  LMP — <high|moderate|low> network stress day.`
- **Top 8 zones table** — sorted by `|onpeak_congestion|` desc.
  Bold rows where `congestion_pct_of_total > 10%`.
- **Decomposition note** (1-2 lines): system-energy day vs.
  congestion day — different position implications.
- **Hand-off**: name the 5 hubs in `top_zones_for_drilldown`.

### 2. Hour drilldown (Tier 2 — hourly heatmap)

From `/views/lmps_hourly_summary?hubs=<5-hub csv>`. Where in the day
the stress lives.

- **Heatmap table** — 24 rows × 5 hub cols, glyph ramp `· . - + #`
  for $0/<$10/<$25/<$50/≥$50 congestion. Bold the peak hour per hub.
  Separator row between offpeak (HE 1-7, 24) and onpeak (HE 8-23).
- **Peak-hour callout** — one paragraph:
  - HE with largest spread across hubs
  - Hub leading the peak (and congestion fraction)
  - Time block — morning ramp / midday / evening ramp / overnight
- **Hand-off**: 3-5 HEs with largest `|congestion|` →
  `binding_hours_for_drilldown`.

### 3. Binding constraints (Tier 3)

From `/views/constraints_da_network?binding_hours=<csv>&top_n=10`.
Which assets set the price in the stressed hours.

- **Top-N table** (top 10-12, sorted by `binding_price`):
  - Constraint name (truncated ~30 chars), contingency, kV, route
    (`from→to` or `single`), `from_bus↔to_bus`, binding $, hours
    bound, MVA rating, top 2-3 neighbors
  - Group by voltage tier (500+ kV first, then 345 kV, then ≤230 kV)
- **Network match note**: % matched / ambiguous / unmatched. Flag
  if unmatched ≥30% — Tier 4 link will be partial.
- **Interface constraints** (zonal aggregations like `APSOUTH`) get
  a small standalone table.
- **Hand-off**: union of `neighbor_bus_ids` across matched →
  `bus_ids` for Tier 4.

### 4. Underlying outages (Tier 4)

From `/views/transmission_outages_for_constraints?bus_ids=<csv>&constraint_labels=<csv>`.
Why the constraints are binding.

- **Cross-linked table**:
  - Constraint name, total $, kV, region | Outage facility, kV,
    equipment type, state, days out, days to return | Join basis
  - Join basis: `seed-branch` (outage IS the constraint),
    `neighbor` (parallel path stressed), `substation` (correlated)
  - Interpretation: structural / amplified / coincident
- **Returning-soon callout**: outages with `days_to_return ≤ 3` —
  relief is imminent, constraint may not bind beyond that day.
- **Network gaps**: count of binding constraints with NO matching
  outage. If high, driver is load growth / weather / model gap.

### 5. Trading lens (3-5 bullets)

- **Biggest expected congestion driver**: `<constraint>` at
  `<region>`, `$<total>`, `<kV>` kV — backed by `<outage>` (or
  flagged untied).
- **Regional bias**: which pricing region has the most binding $ →
  INC/DEC framing.
- **Time-of-day positioning**: which HE block (morning ramp / midday
  / evening peak / overnight) has the most binding $.
- **Structural cluster**: when ≥2 constraints share a substation or
  `from_bus_psse`, name it — multi-day bottleneck if outage is long.
- **Caveat**: if PSS/E match coverage <70% or unmatched-binding $
  >30% of total, surface it — signal weaker.

## Style notes

- Match `fundies/research/PJM-Morning-Fundies.md` tone — tight
  bullets, **bold** for numbers, day-of-week on dates, "TOMORROW"
  framing.
- ASCII-only in tables. `→` arrows in narrative prose only — not in
  Python output capture (cp1252 issues on Windows).
- Money formatting: `$1,234` no decimals for shadow-price totals;
  `$XX.XX` (2 decimals) for LMP scalars.
- Include `target_date` (tomorrow) and `as_of` (today, post-DA) at top.
- Save synthesized markdown to
  `backend/mcp_server/briefings/da_results_<target_date>.md` —
  filename uses TARGET date (tomorrow), one file per target,
  overwrite if regenerated same day.
- Offer to:
  - Prepend to `fundies/research/PJM-DA-Results.md` (newest first,
    create if absent)
  - Or append a section under "Tomorrow's DA Results" header in
    today's `PJM-Morning-Fundies.md` entry

## Caveats to surface in-brief

- **DA refresh windows**: DA LMP scrape lands ~13:00-13:30 EPT.
  Brief running before that returns empty/partial Tier 1; abort
  cleanly with empty-rows count rather than render garbage.
- **Constraint scrape timing**: `da_transmission_constraints` fires
  at 13:00 EPT per `pjm_constraints_da.yaml`, polls every 60s with
  2-hour ceiling. PJM publication slips past 14:00 on slow days.
  Surface "constraints landed at <ts>" so reader knows freshness.
- **SCD2 disabled**: `transmission_outages_changes_24h_snapshot`
  503-disabled until ~2026-05-08. Tier 4 won't surface CLEARED
  outages from last 24h.
- **Hub-grain LMP**: `pjm_lmps_hourly` carries 12-15 PJM aggregate
  hubs only — no zone or nodal LMP. Until upstream `da_hrl_lmps`
  scrape broadens to `type=zone`, "zonal" in Tier 1 is *hub*-grain.
- **PSS/E model age**: 2021. New substations (MARS2, FAYETTE2)
  won't match — Tier 3 surfaces unmatched count, Tier 4 silently
  misses.

## Reference

The companion `pjm-da-constraints-brief` covers constraint × outage
cross-cut at greater depth; this brief embeds a condensed version as
Tier 4 and prepends the LMP funnel context (Tiers 1-2). When both run
the same day, this is the lead, the constraints brief is the appendix.
