# MCP Briefings

Local-only output directory for synthesized briefings produced from the
MCP server's view endpoints. **Everything in this folder is gitignored
except this README.**

## Contents

Two kinds of artifacts land here:

| Pattern | What |
|---|---|
| `*.json` | Cached endpoint responses for the current run (overwritten each generation) |
| `transmission_outages_YYYY-MM-DD.md` | Timestamped brief output, append-only history |

## How briefings are generated

Use the `/pjm-transmission-outages-brief` slash command (defined at
`.claude/commands/pjm-transmission-outages-brief.md`). It hits four
MCP endpoints (`network`, `active`, `window_7d`, `changes_24h_simple`),
caches the JSON here, and synthesizes a markdown brief structured as:

1. Top-line stats
2. Network context (hotspots, topology, gaps)
3. Active — currently in effect
4. Starting — next 7 days
5. Ending — next 7 days
6. Trading lens

If the FastAPI server is down, the slash command falls back to running
the data layer + matcher directly via Python.

## Why local-only

Briefings are operational, time-stamped, trader-specific output — not
shared infrastructure. They reflect a snapshot of PJM state at a
specific moment and have no ongoing reuse value once the day passes.
Keeping them out of git also avoids inflating the repo with daily
markdown noise.
