---
description: Generate a PJM pre-DA morning brief — yesterday-only 3-tier funnel (DA→RT realization, RT $ binders, underlying outages with tenure) framing today's DA clear.
---

# PJM Pre-DA Morning Brief

Generate a tight 1-day settle recap of *yesterday's* PJM market — DA→RT realization by hub, binding constraints by RT $, and underlying outages — framing today's DA clear (~13:00 ET).

> Yesterday's DA→RT realization (Tier 1) → Yesterday's worst RT $ binders (Tier 2) → Outages active during yesterday's binding hours, with tenure (Tier 3) → Today's setup (Tier 4)

Run at 5 AM ET. Backward-looking but **scope is yesterday only** — this is a settle recap, not a trend brief. The companion `/pjm-da-results-brief` runs post-DA-clear (~13:30 ET) and looks forward at *tomorrow's* prices.

Each tier hands a payload to the next so the funnel stays causally tight: yesterday's worst-realized hubs → yesterday's constraints binding into those hubs → outages overlapping yesterday's binding hours → trader synthesis.

Tenure on each outage (`days_out`) is reported inline, so durable network signatures are still visible without a multi-day window.

## Pre-flight: always-fresh MCP server

**First step — always.** Run the pre-flight before anything else:

```bash
python -m backend.mcp_server.ensure_running
```

The script kills any process bound to port 8000, spawns a fresh detached uvicorn against the current code on disk, and waits up to 30s for `/openapi.json` to respond.

- Exit 0 → MCP healthy. Continue.
- Exit non-zero → **STOP IMMEDIATELY.** Point user at `backend/mcp_server/logs/server.log`. Do not synthesize.

Do not call view builders directly via Python under any circumstance. There is no fallback path — if MCP can't be brought up, this command produces no output.

## Anchor date selection

The brief targets **yesterday** in spirit, but the constraints scrape (RT_DART) typically lands 2-3 days behind LMPs. So the implementation must pick the most recent day that has BOTH LMP and constraint data:

1. Start with `candidate = today - 1`.
2. Query `/views/constraints_rt_dart_network?start_date=<today-7>&end_date=<today-1>&top_n=200` and take `max(row.date for row in matched+ambiguous+unmatched)` — this is the latest day with binder data.
3. **Anchor date = that latest constraint date**. Tier 1 (LMPs) uses the same date so all three tiers stay aligned.
4. Surface `lag_days = (today - 1) - anchor_date` in the header. If `lag_days > 4`, abort and warn — the constraint scrape is broken.

Yesterday-no-lag is the typical case. T-2 or T-3 is normal weekend behavior.

## Data sources

The brief consumes three MCP endpoints from `http://localhost:8000`. Brief is single-day; **no rolling window**.

1. `/views/lmps_dart_realization?format=json&target_date=<anchor>&lookback_days=2` —
   Tier 1. Per-hub DA-priced vs RT-realized for `<anchor>` (lookback=2 is the endpoint minimum; only the `<anchor>` row is consumed). Hands `worst_realized_hubs` (5 hubs by daily |DART cong|) → Tier 2.
2. `/views/constraints_rt_dart_network?format=json&start_date=<anchor>&end_date=<anchor>&top_n=15` —
   Tier 2. Anchor day's binders sorted by `|dart_total_price|` desc; each row carries `rt_total_price`, `dart_total_price`, parsed bus pair, `bus_ids`, and binding HE list. **Do not pass `morning_mode=true`** — that flag enables the 7-day roll-up which is out of scope for this brief. Hands `bus_ids` union + binding-HE union → Tier 3.
3. `/views/historical_outages_for_constraints?format=json&bus_ids=<csv>&start_date=<anchor>&end_date=<anchor>&binding_hours=<repeated>` —
   Tier 3. Outages whose `[start_datetime, end_datetime]` overlaps `<anchor>` AND whose buses match the Tier-2 bus_ids set. **Tenure** (`days_out`) is the durability signal — long-tenure outages are structural even though we only see one day of binding. Note the endpoint expects `binding_hours` as **repeated query params** (`&binding_hours=1&binding_hours=2&...`), not CSV.

After hitting each endpoint, save the JSON response into per-view subfolders under `backend/mcp_server/briefings/`:

```
backend/mcp_server/briefings/
├── lmps_dart_realization/<YYYY-MM-DD>.json
├── constraints_rt_dart_network/<YYYY-MM-DD>.json
└── historical_outages_for_constraints/<YYYY-MM-DD>.json
```

All gitignored except the README. `<YYYY-MM-DD>` = run date (today), since the brief is backward-looking.

## Brief structure

### 0. Top-line stats table

| | | |
|---|---:|---|
| Yesterday's RTO mean DART cong | `$<dart_avg>` | DA `<over\|under>`-priced cong by `$X` |
| Worst hub by \|DART cong\| | `<hub>` `$<max>` | hub that took the biggest realized hit |
| Hubs with \|DART cong\| > $10 (yesterday) | `<n>` of 12 | breadth signal |
| Top binder | `<constraint>` `$<rt_$>` (`<n>` HEs) | facility taking the most RT $ |
| Active outages on binder buses | `<n>` (longest tenure: `<facility>` `<days>d`) | durable network signature |
| Today's setup signal | `<one-line>` | what this implies for today's clear |

### 1. Yesterday's DA→RT realization (Tier 1)

From `/views/lmps_dart_realization` (use the `<yesterday>` row of `daily_summary`).

- One-line frame: `Yesterday (<dow> <md>): RTO mean DART cong $X (DA <over|under>-priced by $X). N of 12 hubs realized |DART cong| > .`
- **Per-hub table** (12 hubs, sorted by |DART cong| desc):
  | Hub | DA cong | RT cong | DART cong | Peak HE | Σ\|DART\| (24h) |
  |---|---:|---:|---:|---:|---:|

  Sign-coded narrative: `+ DART cong` = DA cong > RT cong (DA over-priced cong premium / under-priced relief); `-` = opposite. Spell out 1–2 hubs with the biggest sign clearly so the reader doesn't have to do convention-math.
- **Hand-off**: top 5 hubs by |DART cong| → Tier 2 hub filter.

### 2. Yesterday's worst binders by RT $ (Tier 2)

From `/views/constraints_rt_dart_network` for `<yesterday>` only (single-day window).

- **Top-N table** (top 12, sorted by `|dart_total_price|` desc):
  | Constraint | kV | Bus Pair | RT $ | DART $ | Hours | HE Range |
  |---|---:|---|---:|---:|---:|---|

  HE Range: compact list (e.g. `HE 6-17, 20, 23`).
- **Voltage tier note**: % of total RT $ at 500+ kV vs 345 kV vs ≤230 kV. Heavy 500 kV = network-backbone story; heavy ≤230 kV = local subtransmission.
- **Match coverage**: `matched / ambiguous / unmatched of <total>` distinct (constraint, contingency) pairs. Surface unmatched count — those are constraints PSS/E couldn't tag, so Tier 3 is silent on them.
- **Hand-off**: union of `bus_ids` across top-N + union of binding HEs → Tier 3 input.

### 3. Outages active during yesterday's binding hours (Tier 3)

From `/views/historical_outages_for_constraints?bus_ids=<csv>&start_date=<yesterday>&end_date=<yesterday>&binding_hours=...`.

- **Outage table**:
  | Tenure (days_out) | kV | Facility | Constraints Hit | Started | ETR | State |
  |---:|---:|---|---|---|---|---|

  Sort by `days_out` desc — durable / multi-month outages float to the top. Each row tags whether the outage is still active or just returned (`outage_state`, `still_active_at_run`).
- **Long-tenure cluster callout**: when ≥2 outages share a substation OR sit on adjacent buses of the same constraint AND `days_out >= 5`, name the cluster.
- **Returning-soon note**: outages with `days_to_return <= 3` (relief imminent — that constraint's contribution likely fades into today's DA).
- **Network gaps**: count of Tier-2 binders with NO matching outage. High = load/weather/native-contingency-driven, not maintenance.

### 4. Today's setup (Tier 4) — trader lens

3–5 bullets framing what to look for in today's DA based on yesterday's settle:

- **Carry-over thesis**: which Tier-2 binder is most likely to repeat today, given a long-tenure outage on its buses still active. Name hub(s), direction (INC/DEC), HE block.
- **Fade thesis**: which binder had its supporting outage just return (or no underlying outage at all + a single-day pattern) — likely won't repeat. If DA hasn't un-priced it, there's a fade trade.
- **Risk flag**: any hub where yesterday's |DART cong| > $20 — DA materially mispriced and may carry into today. Position size accordingly.
- **Caveat**: surface RT settle timing for `<yesterday>`, PSS/E match rate, and any coverage degradation.

## Style notes

- Match `fundies/research/PJM-Morning-Fundies.md` and the `transmission_outages_*.md` brief tone — tight bullets, **bold** for numbers, day-of-week on every date, "TODAY" framing for the setup section.
- ASCII-only in tables. `→` arrows in narrative prose only — not in Python output capture (cp1252 issues on Windows).
- Money formatting: `,234` no decimals for shadow-price totals; `$XX.XX` (2 decimals) for LMP/DART scalars.
- Header carries `run_date` (today), `settle_date` (anchor — date of the data shown), and `lag_days` (how many days behind today-1 the anchor is — typically 0, 2, or 3).
- Save synthesized markdown to `backend/mcp_server/briefings/morning_brief_<YYYY-MM-DD>.md` — filename uses **run date** (today). Overwrite if regenerated same day.
- Offer to:
  - Prepend to `fundies/research/PJM-Pre-DA-Morning.md` (newest first, create if absent)
  - Or append a "5 AM Pre-DA Brief" section under today's `PJM-Morning-Fundies.md` entry

## Caveats to surface in-brief

- **RT settle timing**: `pjm_lmps_hourly` RT rows for yesterday land progressively through the night; full 24h RT for `<yesterday>` should be in by 4 AM ET but isn't guaranteed. Tier 1 must surface "RT coverage: H of 24 hours for `<yesterday>`" — abort the day's row if <20.
- **PSS/E model age**: 2021. New substations won't match — Tier 2 surfaces unmatched count, Tier 3 silently misses them.
- **Outage tenure is the only durability signal**: this brief deliberately doesn't compute persistence across multiple days. A binder bound only yesterday with a 169-day-old underlying outage *is* structural — the tenure column carries that. A binder bound only yesterday with no matching outage is single-day load/weather noise.
- **No today-DA visibility**: this brief deliberately runs *before* today's DA clears. Tier 4's setup is a *prior* — verify with `/pjm-da-results-brief` after 13:30 ET.

## Reference

The companion `/pjm-da-results-brief` covers the forward-looking, post-DA-clear funnel for tomorrow's prices. When both run the same day, this is the morning lead (5 AM, looking back at yesterday); the DA-results brief is the afternoon update (1:30 PM, looking forward to tomorrow).
