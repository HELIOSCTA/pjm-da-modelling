---
description: Generate a PJM pre-DA morning trend brief — backward-looking 5-tier funnel from 7-day DA→RT realization through binding-pattern constraints, outage persistence, and trend categories, framing today's setup before DA clears.
---

# PJM Pre-DA Morning Brief

Generate a daily brief that walks the LMP causal chain *backward in time* — pre-DA-release — to surface how the last 7 days of congestion has evolved before today's DA clears (~13:00 ET).

> 7-day DA→RT realization (Tier 1) → Worst RT $ binders w/ persistence (Tier 2) → Underlying outages over the window (Tier 3) → Trend signals (Tier 4) → Today's setup (Tier 5)

Run at 5 AM ET. The brief is the inverse-time companion to `/pjm-da-results-brief` (which runs post-DA-clear and looks forward at *tomorrow's* prices). This one looks back: did DA clear correctly the last 7 days? Where did RT punish DA? Which constraints kept binding? Which outages persisted? What does that pattern say about today's clear?

Each tier hands a payload to the next so the funnel stays causally tight: worst realized hubs → constraints binding into those hubs → outages on those constraint buses → categorized trend signals → trader synthesis.

## Pre-flight: always-fresh MCP server

**First step — always.** Run the pre-flight before anything else:

```bash
python -m backend.mcp_server.ensure_running
```

The script kills any process bound to port 8000, spawns a fresh detached uvicorn against the current code on disk, and waits up to 30s for `/openapi.json` to respond.

- Exit 0 → MCP healthy. Continue.
- Exit non-zero → **STOP IMMEDIATELY.** Point user at `backend/mcp_server/logs/server.log`. Do not synthesize.

Do not call view builders directly via Python under any circumstance. There is no fallback path — if MCP can't be brought up, this command produces no output.

## Data sources

The brief consumes four MCP endpoints from `http://localhost:8000`. Default `target_date` = today − 1 (yesterday); default `lookback_days` = 7.

1. `/views/lmps_dart_realization?format=json&target_date=<yesterday>&lookback_days=7` —
   Tier 1. Per-hub DA-priced vs RT-realized over 7-day window. Hands `worst_realized_hubs` (5 hubs by |mean DART|) → Tier 2.
2. `/views/constraints_rt_dart_network?format=json&start_date=<yesterday-6>&end_date=<yesterday>&morning_mode=true&top_n=10` —
   Tier 2 (EXTENDED). Worst binders by RT $ over window, with `binding_day_count`, `binding_he_pattern`, `daily_breakdown` per constraint. Hands `worst_binders` (top-N with `bus_ids` union + binding HE list) → Tier 3.
3. `/views/historical_outages_for_constraints?format=json&bus_ids=<csv>&start_date=<yesterday-6>&end_date=<yesterday>&binding_hours=<csv>` —
   Tier 3 (NEW). Outages active during binding hours of 7-day window, with `persistence_days` per outage. Hands `outage_ids_with_persistence` → Tier 4.
4. *(Optional)* `/views/morning_trend_rollup?format=json&end_date=<yesterday>&lookback_days=7` —
   Tier 4. Categorizes binders/outages into STRUCTURAL/TRANSIENT/EMERGING/INFLECTION. **For v1: synthesize brief-side from Tier 1/2/3 JSONs.**

After hitting each endpoint, save the JSON response into per-view subfolders under `backend/mcp_server/briefings/`:

```
backend/mcp_server/briefings/
├── lmps_dart_realization/<YYYY-MM-DD>.json
├── constraints_rt_dart_network/<YYYY-MM-DD>.json   (morning_mode payload)
├── historical_outages_for_constraints/<YYYY-MM-DD>.json
└── morning_trend_rollup/<YYYY-MM-DD>.json   (optional — only if endpoint exists)
```

All gitignored except the README. `<YYYY-MM-DD>` = run date (today), since the brief is backward-looking.

## Brief structure

### 0. Top-line stats table

| | | |
|---|---:|---|
| 7-day RT realization spread | `$<min>` – `$<max>` (`<spread>`) | best vs worst hub by mean \|DART\| |
| Week DART avg (RTO) | `$<dart_avg>` (`<sign>`) | DA `<over\|under>`-priced by `$X` on average |
| Persistent constraint count | `<n>` constraints bound `≥<k>` of 7 days | top: `<constraint>` (`<days>`d) |
| Structural outage count | `<n>` outages active `≥5` of 7 days | top: `<facility>` (`<days_out>`d in) |
| Today's setup signal | `<STRUCTURAL\|TRANSIENT\|EMERGING\|INFLECTION>` | one-line framing |

### 1. DA→RT realization (Tier 1) — 7-day grid

From `/views/lmps_dart_realization`. Did DA clear correctly?

- One-line frame: `Last 7 days (<start_dow> <start_md> – <end_dow> <end_md>): RTO mean DART $X (DA <over|under>-priced by $X). N of M hubs realized |DART| > $5.`
- **7-day × hub grid** (rows=date, cols=8 hubs, cell=daily mean DART). Glyph ramp `· . - + #` for $0/<$3/<$10/<$25/≥$25 |DART|. Sign-coded: `+` = DA underpriced (RT > DA), `-` = DA overpriced.
- **Worst realized hubs table** (top 5 by mean |DART|): Hub | 7d Mean DART | Worst Day | Worst $ | Days |DART|>$10
- **Hand-off**: 5 hubs in `worst_realized_hubs` → Tier 2 hub filter.

### 2. Worst binders by RT $ (Tier 2) — binding-day pattern

From `/views/constraints_rt_dart_network?morning_mode=true`. Which constraints kept hitting these hubs?

- **Top-N table** (top 12, sorted by `rt_total_price` over window):
  | Constraint | kV | Bus Pair | RT $ (7d) | Days Bound | HE Pattern |
  |---|---:|---|---:|---:|---|

  HE pattern: compact 24-glyph histogram `····###···+++++#####···+`. `#` = bound 5+ days at that HE, `+` = 2-4 days, `·` = ≤1 day.
- **Daily breakdown callout** (top 3 binders): `Mon -$120 / Tue -$180 / Wed -$95 / Thu -$210 / Fri -$300 / Sat -$50 / Sun -$25` — exposes steady-state vs single-day outlier.
- **Voltage tier note**: % of total RT $ at 500+ kV vs 345 kV vs ≤230 kV. Heavy 500 kV = network-backbone story; heavy ≤230 kV = local subtransmission.
- **Hand-off**: union of `bus_ids` across top-N + union of binding HEs → Tier 3 input.

### 3. Underlying outages + persistence (Tier 3)

From `/views/historical_outages_for_constraints?bus_ids=<csv>&binding_hours=<csv>`. Why did the binders keep binding?

- **Persistence table**:
  | Persistence | kV | Facility | Constraints Hit | Dates × HEs | State |
  |---|---:|---|---|---|---|
  Persistence: `7/7 sustained` / `4/7 intermittent` / `1/7 transient`.
- **Structural cluster callout**: when ≥2 outages share a substation OR sit on adjacent buses of the same constraint, name the cluster — multi-week network signature.
- **Returning-this-week note**: outages with `days_to_return ≤ 3` — relief is imminent, expect that constraint's contribution to fade.
- **Network gaps**: count of Tier-2 constraints with NO matching outage. High = load/weather/model-driven, not maintenance.

### 4. Trend signals (Tier 4) — categorize the week

Synthesize brief-side from Tiers 1-3 (or fetch from `/views/morning_trend_rollup` if endpoint exists). Categorization rules:

- **STRUCTURAL** — `binding_day_count >= 5` AND has matching outage with `persistence_days >= 5`. "Set it and forget it" multi-week stories. State: which way (DEC/INC) and which hours.
- **TRANSIENT** — `binding_day_count <= 2` AND `total_rt_$ > $500`. Weather/load/single-day forced outage. Don't extrapolate.
- **EMERGING** — bound only days 5-7 of the window AND a new outage started in those days. **This is today's positioning candidate** — pattern hasn't fully priced in.
- **INFLECTION** *(optional)* — bound days 1-4 then went quiet 5-7, with an outage that returned. Watch whether DA un-priced the relief.

For each item: `<category> · <constraint or facility> · <hub region> · <one-line rationale>`.

### 5. Today's setup (Tier 5) — trader lens

3-5 bullets framing what to look for in today's DA based on the week's pattern:

- **Carry-over thesis**: which STRUCTURAL story is most likely to repeat in today's DA. Name hub(s), direction (INC/DEC), HE block. Cite supporting outage and ETA-to-return.
- **Fade thesis**: which TRANSIENT or returning-INFLECTION story is most likely to *not* repeat. If DA hasn't un-priced it, there's a fade trade.
- **Fresh-money thesis**: the EMERGING bucket — what's not yet baked into the curve. Highest-conviction directional position of the day if backed by a confirmed outage.
- **Risk flag**: any hub where week's mean |DART| > $15 — signals a regime DA is consistently mispricing. Position size accordingly.
- **Caveat**: surface RT settle timing, snapshot age, PSS/E coverage if degraded.

## Style notes

- Match `fundies/research/PJM-Morning-Fundies.md` and the `transmission_outages_*.md` brief tone — tight bullets, **bold** for numbers, day-of-week on every date, "TODAY" framing for the setup section.
- ASCII-only in tables. `→` arrows in narrative prose only — not in Python output capture (cp1252 issues on Windows).
- Money formatting: `$1,234` no decimals for shadow-price totals; `$XX.XX` (2 decimals) for LMP/DART scalars.
- Header carries `run_date` (today) and `window` (`<start_date> – <end_date>`).
- Save synthesized markdown to `backend/mcp_server/briefings/morning_brief_<YYYY-MM-DD>.md` — filename uses **run date** (today), since brief is backward-looking. Overwrite if regenerated same day.
- Offer to:
  - Prepend to `fundies/research/PJM-Pre-DA-Morning.md` (newest first, create if absent)
  - Or append a "5 AM Pre-DA Brief" section under today's `PJM-Morning-Fundies.md` entry

## Caveats to surface in-brief

- **RT settle timing**: `pjm_lmps_hourly` RT rows for yesterday land progressively through the night; full 24h RT for `<yesterday>` should be in by 4 AM ET but isn't guaranteed. Tier 1 must surface "RT coverage: H of 24 hours for `<yesterday>`" — abort the day's row if <20.
- **DA constraint scrape lag**: previous DA day (target=yesterday, scraped ~13:00 ET 2 days ago) is the latest fully-settled DA. Spot-check `as_of` timestamp on RT-DART payload.
- **SCD2 disabled**: `transmission_outages_changes_24h_snapshot` is 503-disabled until ~2026-05-08. Tier 3 may miss CLEARED outages from late in the window. Surface "outage coverage: ACTIVE-only" until enabled.
- **Hub-grain LMP**: only ~12-15 PJM aggregate hubs in the mart; no zone or nodal LMP.
- **PSS/E model age**: 2021. New substations won't match — Tier 2 surfaces unmatched count, Tier 3 silently misses them.
- **No today-DA visibility**: this brief deliberately runs *before* today's DA clears. The "today's setup" section is a *prior* — verify with `/pjm-da-results-brief` after 13:30 ET.

## Reference

The companion `/pjm-da-results-brief` covers the forward-looking, post-DA-clear funnel for tomorrow's prices. When both run the same day, this is the morning lead (5 AM, backward); the DA-results brief is the afternoon update (1:30 PM, forward).
