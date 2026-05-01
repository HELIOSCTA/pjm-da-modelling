# MCP Briefings

Local-only output directory for synthesized briefings produced from the
MCP server's view endpoints. **Everything in this folder is gitignored
except this README.**

## Layout

```
briefings/
├── README.md                                       (tracked)
├── <synthesis>_<YYYY-MM-DD>.md                     synthesized brief, top level
└── <view_endpoint>/                                per-MCP-endpoint cache
    └── <YYYY-MM-DD>.json                           dated snapshot
```

One subfolder per MCP endpoint. Each holds dated JSON snapshots from
that endpoint, building a per-view history over time. Synthesized
briefs (which combine multiple endpoints) sit at the top level.

## Current subfolders

| Path | Source endpoint |
|---|---|
| `transmission_outages_active/` | `GET /views/transmission_outages_active` |
| `transmission_outages_window_7d/` | `GET /views/transmission_outages_window_7d` |
| `transmission_outages_changes_24h_simple/` | `GET /views/transmission_outages_changes_24h_simple` |
| `transmission_outages_changes_24h_snapshot/` | `GET /views/transmission_outages_changes_24h_snapshot` *(503-disabled until SCD2 ages)* |
| `transmission_outages_network/` | `GET /views/transmission_outages_network` |
| `transmission_outages_for_constraints/` | `GET /views/transmission_outages_for_constraints` *(Tier 4 of DA-results funnel)* |
| `constraints_da_network/` | `GET /views/constraints_da_network` *(extended with `binding_hours` for Tier 3)* |
| `constraints_rt_dart_network/` | `GET /views/constraints_rt_dart_network` |
| `lmp_da_hub_summary/` | `GET /views/lmp_da_hub_summary` |
| `lmp_da_outage_overlap/` | `GET /views/lmp_da_outage_overlap` |
| `lmps_daily_summary/` | `GET /views/lmps_daily_summary` *(Tier 1 of DA-results funnel)* |
| `lmps_hourly_summary/` | `GET /views/lmps_hourly_summary` *(Tier 2)* |

Add a new subfolder when you wire up a new endpoint (e.g., `fuel_mix_hourly/`).

## Top-level synthesis files

| Pattern | Generator | What |
|---|---|---|
| `transmission_outages_<YYYY-MM-DD>.md` | `/pjm-transmission-outages-brief` | Network-context-first brief: hotspots → Active → Starting → Ending |
| `da_constraints_<YYYY-MM-DD>.md` | `/pjm-da-constraints-brief` | DA-released brief: tomorrow's binding constraints → outage intersection → trading lens |
| `da_results_<YYYY-MM-DD>.md` | `/pjm-da-results-brief` | 5-tier funnel: Daily LMP → Hourly drilldown → DA constraints → outages → trader lens (filename = TARGET date) |

## Generating a brief

Use the `/pjm-transmission-outages-brief` slash command (defined at
`.claude/commands/pjm-transmission-outages-brief.md`). It hits the
relevant endpoints, drops dated JSON into each view subfolder, and
synthesizes a top-level markdown brief.

If the FastAPI server is down, the slash command falls back to running
the data layer + matcher directly via Python.

## Why local-only

Briefings are operational, time-stamped output — not shared
infrastructure. They reflect a snapshot of PJM state at a specific
moment and have no ongoing reuse value once the day passes. Keeping
them out of git also avoids inflating the repo with daily noise.
