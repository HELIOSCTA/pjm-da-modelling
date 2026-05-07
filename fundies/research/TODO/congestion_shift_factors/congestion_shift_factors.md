---
timestamp_local: "2026-05-06 08:30 ET"
timestamp_utc: "2026-05-06T12:30:00Z"
market: "power"
source: "Self-study; Yes Energy congestion primer; LP duality of SCED"
tags: [congestion, shift-factors, ptdf, lodf, lmp, study]
summary: "Primer on shift factors, the LMP_cong = -SF * mu identity, and how source/sink intuition explains who pays vs gets paid when a constraint binds"
signal_relevance: "Foundation for reading PJM constraint maps and DFAX tables; required to interpret active-binder briefs and reason about which nodes will move when an outage shifts topology"
confidence: 4
status: "todo"
---

<!-- TODO: build a worked-numbers example from a real PJM binder using mcp__pjm-views constraints + DFAX from network views -->
<!-- TODO: verify sign convention against actual da_marginal_value + LMP cong component on a known binding hour -->
<!-- REVIEW: confirm with Edi that the source = generator, sink = system reference convention matches how PJM publishes DFAX -->

# Congestion & Shift Factors — Study Note

## Goal

Be able to read a PJM constraint map (monitored element + contingency + DFAX
table) and predict, before looking at LMPs, which nodes will price up and
which will price down — and by roughly how much. Currently I lean on the
constraint name and geography; need to internalize the source/sink math so
this is reflexive.

## Core Definitions

### Shift Factor (SF)

A shift factor `SF_i,k` is the partial derivative of flow on line `k` with
respect to a 1 MW source/sink injection pair where:

- **Source** = +1 MW injected at node `i`
- **Sink** = -1 MW withdrawn at the system reference (or load distribution)

Range is roughly `-1` to `+1`. Sign convention:

- **+SF** → injecting at node i loads the line in the monitored direction
- **-SF** → injecting at node i relieves the line
- **SF = 0** → injection at i is electrically symmetric to the line

PJM publishes these as **DFAX** (distribution factors). Same concept,
specific reference (typically generator-to-load).

### Contingency Awareness

The post-contingency shift factor is what actually clears the market:

```
post_contingency_SF = base_PTDF + LODF * pre_contingency_flow_on_outage
```

Where:
- `PTDF_i,k` = base shift factor of node i onto monitored line k
- `LODF_k,c` = line outage distribution factor — what fraction of contingency
  line c's flow re-routes onto line k if c trips

This is why a constraint can bind on a line that looks underloaded in real
time. The market is pricing the **N-1 worst case**, not the current state.

## The LMP Decomposition

For each node i:

```
LMP_i = lambda  -  sum_k ( SF_i,k * mu_k )  +  loss_i

        | energy |    | congestion        |   | loss |
```

- **lambda** = system marginal energy price (same everywhere)
- **mu_k** = shadow price of constraint k, in $/MW. Always >= 0 by LP
  convention (<= constraints get non-negative duals)
- **SF_i,k** = shift factor of i onto k

So:

```
LMP_cong_i = - sum_k ( SF_i,k * mu_k )
```

### Why the minus sign

The line-flow constraint in SCED is written:

```
sum_i SF_i,k * (g_i - d_i)  <=  Limit_k
```

Load enters with a negative sign (`-d_i`), so adding 1 MW of load at a node
with `+SF` is equivalent to **tightening** the limit by SF MW (using up
headroom). Cost rises by `mu_k * SF_i,k`. Dual of the nodal balance
equation pushes that cost into LMP_i as a positive contribution → since
mu >= 0 and the formula carries a minus sign, the SF must be negative for
LMP to rise. Algebra:

- Node with **+SF** (gen here loads the line): `LMP_cong = -(+SF)(+mu) < 0`
  → LMP collapses or goes negative → gen gets penalized, load gets paid
- Node with **-SF** (gen here relieves the line): `LMP_cong = -(-SF)(+mu) > 0`
  → LMP spikes → gen gets rewarded, load pays a premium

### Rule-of-thumb table

| Node's SF to binder | Congestion LMP | Trader read |
|---|---|---|
| **+SF** (loads the line) | **negative** | gen here is being told to back down |
| **-SF** (relieves the line) | **positive** | gen here is being called to ramp up |
| SF ~ 0 | ~0 | electrically irrelevant to this binder |

## Total LMP — All Three Components

```
LMP_i  =  lambda  +  LMP_cong_i  +  LMP_loss_i
        = lambda  -  sum_k ( SF_i,k * mu_k )  +  DLF_i * lambda
          | energy |   | congestion (sums across ALL binders) |   | loss |
```

- **lambda** — system marginal energy price; same at every node
- **congestion** — sums across ALL binding constraints k. A node can have +SF
  to one binder and -SF to another; contributions partially cancel.
- **loss** — marginal loss factor times energy; small (typ +/- $1-3),
  positive at load centers, negative near gen

Sanity-check decomposition for Hamilton Liberty during the binder:
`-$357 = ~$25 (lambda) + ~-$382 (cong) + ~$0 (loss)`. Congestion dominates.

## What the Congestion Sign Tells You About the Node

| LMP_cong | Gen incentive | Load incentive | What's "short" at this node |
|---|---|---|---|
| **+cong large** | ramp UP | curtail / DR | **deliverable** supply (not necessarily absolute supply) |
| **-cong large** | ramp DOWN / shut off | consume MORE — soak up trapped MW | export / takeaway capacity |
| ~0 | dispatch on energy alone | normal | nothing |

**Important nuance — "short" doesn't mean "needs more capacity."** A +cong
node has three possible reads, all priced the same:

1. **Genuine local shortage** — not enough MW physically near the load
   (e.g. Dom Hub in a true heat dome with every local CCGT maxed)
2. **Import-bottleneck shortage** — plenty of cheap gen elsewhere, can't
   reach this node (e.g. NoVA today — Pleasant View XF won't let west
   PJM in)
3. **Transmission-out shortage** — topology change isolated the node
   electrically (e.g. behind a Bedington TX outage)

Only (1) means "build a new plant here." (2) means "build transmission."
(3) means "wait for the outage to return." The price signal alone can't
distinguish — you need the outage stack and DFAX to tell which story.

## Worked Example — Hamilton Liberty / Lenox (from Yes Energy primer)

Setup:
- **Monitored element:** Lenox 115 kV line
- **Contingency:** Etowanda-Hillside 230 kV
- Hamilton Liberty CCGT sits between the two; pre-constraint LMP ~$21
- 5:05 a.m. constraint binds → Hamilton Liberty LMP = -$357
- Some node on the relief side prints +$944

Inferred SFs (assume lambda ~ $25):

| Node | LMP | LMP_cong (approx) | Implied SF * mu | SF sign |
|---|---|---|---|---|
| Hamilton Liberty | -$357 | -$382 | +382 | **+SF, large** |
| Relief-side node | +$944 | +$919 | -919 | **-SF, large** |

What the dispatch loop did:
1. Constraint binds → mu spikes
2. Hamilton Liberty's LMP collapses (high +SF * high mu)
3. Plant ramps down because next MW costs more than it earns
4. Flow on Lenox drops → mu falls → constraint clears at 5:25
5. Plant ramps back up at $23 LMP

The article describes this geographically ("Northeastern side"). The math
is **electrical** — two plants on the same geographic side can have very
different DFAX if their impedance paths to the monitored element differ.
The price map is the truth; geography is a useful proxy.

## Worked Example #2 — Pleasant View XF on 2026-05-06 (live PJM case)

**The constraint:**
- Monitored: `PLEASNTV TX3 XFORMER H` (Pleasant View 500/230 transformer #3)
- Contingency: `500/230.GooseCreek.TX1` (parallel Goose Creek transformer)
- Voltage: 500 kV side
- This is a **vertical / voltage-level** constraint, not a line. When Goose
  Creek TX1 trips, Pleasant View TX3 has to absorb all the 500->230
  step-down feeding the NoVA / Ashburn 230 kV system.

**Verified from MCP (2026-05-06 DA):**
- Top binder by binding-hours shadow price
- Bound at HE7 and HE20 — exactly the hours where Dom Hub cong peaked
  (+$16.45 HE7, +$22.34 HE20)
- Binding HE shadow $ = -$672 sum across HE7+HE20 (~$336/hr)
- Day total shadow $ = -$7,101

### The SF picture (with my initial framework correction)

I initially assumed "Dom = import-constrained load pocket" with rest-of-PJM
gen as +SF. **That was wrong.** Confirmed flow direction:

- **Dom-south/central gen (+SF)** — Surry, North Anna, Brunswick,
  Greensville, Possum Point, Chesterfield. These push power *northbound*
  through Loudoun, choking the corridor. Their MW LOADS Pleasant View XF.
  -> LMP_cong negative -> they get told to back down.
- **Gen north of the bottleneck (-SF)** — PSEG, JCPL, BGE, Pepco. Serving
  NoVA from a different angle relieves the constraint. -> LMP_cong
  positive -> rewarded.
- **Dom Hub itself** — load-weighted toward NoVA -> sits BEHIND the
  bottleneck -> effective small **negative** SF (~-0.05 implied from
  $16.45 / $336). Individual data-center pnodes will have much steeper
  -SF.

**The lesson:** I read the constraint geographically (Pleasant View = NoVA
= load pocket) and assumed flow direction. Wrong. The binding flow is
*export-direction* (Dom-south gen flooding northbound), not
*import-direction*. **DFAX signs are the only authoritative source for
binding direction. Geography is a starting hypothesis, not an answer.**

### The outage stack that pushed mu hard

Confirmed from MCP active outages:

- **Brambleton 230 kV network (NoVA):** BRAMBLETON-NRTHSTAR (started
  5/01) + ALTAIR-BRAMBLET (started 5/05, just yesterday) — exactly what
  GridStatus flagged as "additional line outage in NoVA on the
  Brambleton 230kV network"
- **Central VA 500 kV (9 active pieces in Dominion):** LADYSMTH-ELMONT4,
  CUNNIHAM-ELMONT4, BRISTERS TX1, MARS2 TX2, FENTRES4 TX3, FENTRES4 TX5,
  FENTRES4-YADKIN4 (4-month outage), NANNA4 RSS2, ELMONT4 TX1
- **MidAtl 500 kV stack:** Bedington TX1 + TX3 + Bedington-BlackOak line

Every one of these stacks the same way: more flow forced onto 230 kV
through Loudoun -> higher mu on Pleasant View XF -> deeper Dom Hub
positive cong (and deeper Eastern/NJ Hub negative cong).

## Same Hub, Opposite Signs — Dom Hub Intra-Day Flip

Dom Hub on 2026-05-06 DA:

| Period | Dom Hub Cong | Driving binder |
|---|---|---|
| Overnight + AM (HE1-9) | **+$8 to +$16** | Pleasant View XF (NoVA bottleneck) |
| Midday (HE10-17) | **-$3 to -$12** | Cloud XF / Remington / Ladysmith CB / central VA |
| Evening peak (HE18-24) | **+$6 to +$22** | Pleasant View XF + Ox XF |

**The same hub goes positive overnight/evening and negative midday.** Why:

- Overnight + evening: Pleasant View XF binds. Dom Hub is on the relief
  side (-SF) -> +cong.
- Midday: solar pushes northward from southern VA, binding Cloud XF /
  Remington / Ladysmith. Dom Hub (some south-Dom components) is on the
  loading side (+SF) of *those* constraints -> -cong.

**Lesson:** Dom is simultaneously a **load pocket (NoVA)** and a
**generation pocket (south/central VA)** connected by a stressed 230 kV
system. Its hub sign depends on which corridor binds. The geographic
shorthand "Dom = load pocket" misses the duality.

## Mirror Hub Is Outage-Conditional

**Original hypothesis (from GridStatus narrative in fundies):** "WHUB
mirrors Dom — when Pleasant View binds, Western Hub goes deeply
negative."

**Today's data (2026-05-06):**

| Hub | OnPk Cong |
|---|---|
| EASTERN HUB | **-$13.80** |
| NEW JERSEY HUB | **-$13.75** |
| Chicago Gen / Chicago / N Illinois | -$10 |
| OHIO / AEP-DAYTON / AEP GEN | -$5 |
| DOMINION HUB | **+$4.06** (peak +$21.18 HE21) |
| WESTERN HUB | **+$2.18** (only mildly positive) |

The mirror has **migrated east**. With central VA 500 kV out + Bedington
500 kV partial + Conastone-Northwest binding, the negative side is
Eastern/NJ Hub today, not Western. Drivers:

- Eastern + NJ Hub deepest negative comes from Conastone-Northwest +
  Brighton-Conastone + BGE-PEPCO 230 kV stress, not Pleasant View
- Western Hub being only mildly positive reflects detour routing —
  power normally flowing W->E has to navigate the central VA outages,
  some of that detour loads Pleasant View, partially offsetting WHUB's
  usual mirror role

**Lesson:** Mirror-hub identity is **outage-conditional**. The clean
"WHUB ↔ Dom" rule holds when the central VA 500 kV / Bedington stack is
healthy. With the current stack, mirror migrates to Eastern/NJ. **Always
read the outage stack to know which hub eats the negative.**

## Reading a PJM Constraint Map (target workflow)

Given a row in `mcp__pjm-views__get_constraints_da_network_views_constraints_da_network_get`:

1. **Identify monitored element + contingency** — together they define the
   binder.
2. **Pull DFAX table** — PJM publishes per-pnode DFAX for the monitored/
   contingency pair.
3. **Sort by |DFAX|** — the high-magnitude rows are the levers.
4. **Sign check:**
   - +DFAX nodes → expect negative congestion LMP, generation here is
     being penalized
   - -DFAX nodes → expect positive congestion LMP, generation here is
     being rewarded
5. **Cross-reference with active outages** —
   `get_transmission_outages_for_constraints` shows whether the binder is
   structural (driven by an underlying outage) or noise.

## Lessons from Studying This (Mistakes I Made)

Captured so I don't repeat them.

### 1. Confusing "negative LMP" with "negative SF"

When asked about Hamilton Liberty's SF, my gut said negative because LMP
was -$357. **Wrong.** The two negatives in `LMP_cong = -SF * mu` are easy
to merge but they're connected, not the same:

- **+SF** node (loads the line) -> **negative LMP_cong** because of the
  formula's leading minus sign
- **-SF** node (relieves the line) -> **positive LMP_cong**

Hamilton Liberty: +SF, negative LMP, ramps down. All consistent. The
minus sign is the formula's, not the SF's.

### 2. Inferring flow direction from constraint geography

I read "Pleasant View = NoVA = load pocket" and assumed import-direction
binding. Wrong. Dom-south gen has +SF (export-direction binding through
Loudoun). **DFAX signs are authoritative; geography is a starting
hypothesis only.**

### 3. Assuming the mirror hub is universal

I expected Western Hub to mirror Dom. Today it's Eastern/NJ. **Mirror is
outage-conditional** — read the outage stack first, then predict which
hub eats the negative.

### 4. Forgetting the hub aggregates many SFs

A trading hub is a basket of pnodes. Hub-level SF is a weighted average,
typically smaller in magnitude than individual constituent pnodes. Dom
Hub's implied -SF on Pleasant View was only ~-0.05; data-center pnodes
inside it will have steeper -SF. **Hub LMPs smooth out the signal —
node-level pricing is where the real swing lives.**

### 5. Treating "+cong = short generation"

A node can be +cong because it's short of *deliverable* MW, not short of
generation in absolute terms. NoVA isn't short of capacity — it's short
of **import-deliverability** through Pleasant View XF. Same price, very
different remediation (build transmission vs build plant).

## Open Questions / TODOs

- **Build the LMP reconstruction:** pick a single hour on Pleasant View
  XF (e.g. 2026-05-06 HE20), pull mu from `da_marginal_value`, get
  per-pnode DFAX from Yes Energy / PowerSignals, compute
  `lambda + sum(-SF*mu)` for ~5 nodes (Dom Hub, Eastern Hub, a
  data-center pnode, a Dom-south gen pnode), verify it matches published
  LMPs within loss tolerance.
- **Sign convention check:** does PJM's published DFAX define source as
  gen-to-load or gen-to-system-reference? Affects sign of every SF I
  read.
- **PTDF vs post-contingency divergence:** for the binders we see most,
  how much does base PTDF differ from post-contingency SF? If close,
  geographic intuition is fine. If divergent, must think
  contingency-first.
- **DFAX for Dom Hub onto Pleasant View XF:** verify the implied
  negative sign empirically. The pjm-views MCP doesn't expose DFAX —
  need Yes Energy or PJM data publication.
- **When does the WHUB-Dom mirror hold?** Build a quick study: for the
  last N days where Pleasant View XF was a top-3 binder, what was WHUB
  vs Eastern Hub vs NJ Hub cong? Map mirror identity to outage state.
- **Same-hub-opposite-signs taxonomy:** for each PJM hub, list the
  binders where it's +SF and -SF. Currently doing this in my head;
  worth a reference table.
- **Handle transformer constraints:** Pleasant View XF is vertical
  (500->230 step-down), not horizontal. Need to internalize that some
  binders are about voltage-level transfer, not corridor flow. SF
  intuition has to think "who feeds the 500 kV side, who pulls from the
  230 kV side."

## Data Sources

- `mcp__pjm-views__get_constraints_da_network_views_constraints_da_network_get` — DA binders with monitored/contingency, supports `binding_hours` filter for funnel mode
- `mcp__pjm-views__get_constraints_rt_dart_network_views_constraints_rt_dart_network_get` — RT binders + DART comparison
- `mcp__pjm-views__get_lmp_da_hub_summary_views_lmp_da_hub_summary_get` — hub-level cong sorted by |onpeak cong|, the fastest way to spot which hubs are mirroring each other
- `mcp__pjm-views__get_lmp_da_outage_overlap_views_lmp_da_outage_overlap_get` — outage overlay on binders
- `mcp__pjm-views__get_transmission_outages_active_views_transmission_outages_active_get` — current outage stack with risk tags + days_left
- PJM Data Miner: `da_marginal_value`, `rt_marginal_value` — shadow prices per constraint
- **DFAX is NOT exposed in pjm-views MCP** — need Yes Energy / PowerSignals or PJM Data Miner DFAX feed for per-pnode shift factors

## References

- Yes Energy "Example of a Grid Constraint" — Lenox/Etowanda-Hillside walkthrough
- Operator-effect-on-congestion TODO — sibling note on why DA mu > RT mu
  systematically
- PJM-Morning-Fundies.md — GridStatus quotes on Pleasant View XF /
  Brambleton outage / WHUB mirror (snapshot of the live narrative used
  to validate this primer)
- 2026-05-06 study session — first pass building the SF/LMP framework,
  worked through Hamilton Liberty + Pleasant View XF, validated against
  pjm-views MCP. Caught corrections: Dom-south gen has +SF (not -SF);
  mirror hub is outage-conditional (Eastern/NJ today, not Western).
