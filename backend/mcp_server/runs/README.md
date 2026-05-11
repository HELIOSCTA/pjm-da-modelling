# Runs

Local-only output directory for the morning-brief workflow. The folder
holds synthesized briefs (what the trader reads), per-specialist
intermediate digests (subagent outputs), raw MCP view caches
(diagnostic), and feedback rules that the next run will pick up.
**Everything below is gitignored except this README.**

## Layout

```
backend/mcp_server/runs/
├── README.md                          (tracked)
│
├── synthesized/                       final orchestrator outputs
│   ├── transmission_outages_<YYYY-MM-DD>.md
│   ├── morning_brief_<YYYY-MM-DD>.md
│   ├── da_results_<YYYY-MM-DD>.md
│   └── da_constraints_<YYYY-MM-DD>.md
│
├── specialists/                       subagent intermediate outputs + feedback
│   ├── outage_constraint_overlap/
│   │   ├── <YYYY-MM-DD>.md            daily digest (one per run)
│   │   └── feedback.md                rules picked up on next run
│   ├── outage_delta/{<date>.md, feedback.md}
│   ├── outage_network/{<date>.md, feedback.md}
│   └── outage_7d_arc/{<date>.md, feedback.md}
│
├── orchestrator/                      synthesis-layer feedback (singleton)
│   └── feedback.md                    rules for headline / watchlist / ordering
│
├── views/                             raw MCP responses (diagnostic)
│   └── <endpoint>/<YYYY-MM-DD>.json   dated snapshots, one subdir per endpoint
│
└── _archive/                          one-off scripts + static reference data
    └── ...
```

## Current `views/` subdirs (one per MCP endpoint)

| Path | Source endpoint |
|---|---|
| `views/transmission_outages_active/` | `GET /views/transmission_outages_active` |
| `views/transmission_outages_window_7d/` | `GET /views/transmission_outages_window_7d` |
| `views/transmission_outages_changes_24h_simple/` | `GET /views/transmission_outages_changes_24h_simple` |
| `views/transmission_outages_changes_24h_snapshot/` | `GET /views/transmission_outages_changes_24h_snapshot` |
| `views/transmission_outages_network/` | `GET /views/transmission_outages_network` |
| `views/transmission_outages_for_constraints/` | `GET /views/transmission_outages_for_constraints` |
| `views/constraints_da_network/` | `GET /views/constraints_da_network` |
| `views/constraints_rt_dart_network/` | `GET /views/constraints_rt_dart_network` |
| `views/historical_outages_for_constraints/` | `GET /views/historical_outages_for_constraints` |
| `views/lmp_da_hub_summary/` | `GET /views/lmp_da_hub_summary` |
| `views/lmp_da_outage_overlap/` | `GET /views/lmp_da_outage_overlap` |
| `views/lmps_daily_summary/` | `GET /views/lmps_daily_summary` |
| `views/lmps_hourly_summary/` | `GET /views/lmps_hourly_summary` |
| `views/lmps_dart_realization/` | `GET /views/lmps_dart_realization` |

Add a new subfolder under `views/` when you wire up a new endpoint.

## `synthesized/` files (one per brief command)

| Filename pattern | Generator |
|---|---|
| `transmission_outages_<YYYY-MM-DD>.md` | `/pjm-transmission-outages-brief` (orchestrator + 4 specialists) |
| `da_results_<YYYY-MM-DD>.md` | `/pjm-da-results-brief` (filename uses TARGET date = tomorrow) |
| `morning_brief_<YYYY-MM-DD>.md` | `/pjm-pre-da-morning-brief` (filename = run date, looks back at yesterday) |
| `da_constraints_<YYYY-MM-DD>.md` | `/pjm-da-constraints-brief` |

## `specialists/` — the feedback loop

The transmission-outages brief is decomposed into four specialist
subagents, each persisting to its own subdir:

- `outage_constraint_overlap/` — anchor: outage → DA-constraint → $ triples
- `outage_delta/` — last-24h CLEARED / NEW / REVISED feed delta
- `outage_network/` — active-outage hotspots + topology + unmatched gaps
- `outage_7d_arc/` — week-ahead calendar arc + watchlist

Each subdir contains:

- `<YYYY-MM-DD>.md` — one digest per brief run
- `feedback.md` — rules accumulated via `/brief-feedback`. The
  specialist Reads this file before generating output and treats its
  rules as authoritative (overriding the baseline prompt where they
  conflict).

`orchestrator/feedback.md` plays the same role for the synthesis layer
(headline composition, watchlist content, section ordering).

## Generating a brief

| Command | Outputs to |
|---|---|
| `/pjm-transmission-outages-brief` | `synthesized/transmission_outages_<date>.md` + 4 files in `specialists/*/<date>.md` + JSONs in `views/*` |
| `/pjm-da-results-brief` | `synthesized/da_results_<target_date>.md` + JSONs in `views/*` |
| `/pjm-pre-da-morning-brief` | `synthesized/morning_brief_<run_date>.md` + JSONs in `views/*` |
| `/pjm-da-constraints-brief` | `synthesized/da_constraints_<target_date>.md` + JSONs in `views/*` |

The local MCP server (port 8000) is auto-started by the `PreToolUse`
hook before any `mcp__pjm-views__*` tool call — no manual pre-flight
needed.

## Logging feedback

Use `/brief-feedback` after a brief delivery to log a miss /
false-positive / style correction. The `brief-feedback-logger` subagent
appends a dated rule to the right `specialists/<view>/feedback.md` (or
`orchestrator/feedback.md`); the next brief run picks it up
automatically — no manual prompt edits required.

Promote rules into the specialist's main prompt body once they've
proven themselves across several runs, then clear them from
`feedback.md`. Weekly review pattern.

## Why local-only

Briefs are operational, time-stamped output — not shared
infrastructure. They reflect a snapshot of PJM state at a specific
moment and have no ongoing reuse value once the day passes. Keeping
them out of git also avoids inflating the repo with daily noise.
