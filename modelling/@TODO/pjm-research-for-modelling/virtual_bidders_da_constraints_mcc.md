  Sources:
  - PJM Manual 11 (m11.pdf) (https://www.pjm.com/-/media/DotCom/documents/manuals/m11.pdf)
  - PJM Manual 12 (m12.pdf) (https://www.pjm.com/-/media/DotCom/documents/manuals/m12.pdf)
  - PJM Manual 6 (m06.pdf) (https://www.pjm.com/-/media/DotCom/documents/manuals/m06.pdf)
  - PJM Locational Marginal Pricing Components (workshop slides) (https://www.pjm.com/-/media/training/nerc-certifications/markets-exam-materials/mkt-optimization-wkshp/locational-marginal-pricing-components.ashx)
  - PJM Learning Center — LMP (https://learn.pjm.com/three-priorities/buying-and-selling-energy/lmp.aspx)
  - PJM LMP/FTR 101 course (https://www.pjm.com/Globals/Training/Courses/ip-lmp-ftr-101)
  - PJM LMP formation / reserve-shortage pricing paper (https://www.pjm.com/-/media/DotCom/markets-ops/energy/real-time/reserve-shortage-pricing-paper.pdf)


# Virtual Bidders, DA Transmission Constraints, and How PJM Sets the MCC

## Scope

Background note explaining the mechanics behind three things our DA-price models keep bumping into:

1. **Virtual (INC / DEC / UTC) bids** — who places them, why, and how they
   move the DA clear.
2. **DA transmission constraints** — what "a constraint binds" means in the
   SCED/SCUC optimization, and the artifacts PJM publishes about it.
3. **The Marginal Congestion Component (MCC)** — the congestion term inside
   LMP, how PJM computes it from binding-constraint shadow prices and shift
   factors, and why it is the part of price that virtuals and outages
   actually move.

Companion docs: `pjm_data_sources.md` (which feeds carry this data),
`pjm-rt-modelling.md` (RT-side analog), and the constraint MCP views under
`backend/mcp_server/` that surface binding constraints + shadow prices.

---

## 1. LMP decomposition — the frame everything else hangs on

PJM (like all US ISOs running an LMP market) decomposes the locational
marginal price at every pnode into three additive components:

```
LMP_i = SE + MCC_i + MLC_i
```

- **SE** — *System Energy* (a.k.a. the marginal energy component, "λ"). One
  number for the whole RTO in each interval: the cost of the marginal MW of
  energy at the reference bus, ignoring losses and congestion. Same at every
  bus.
- **MCC_i** — *Marginal Congestion Component* at bus *i*. The dollar value of
  transmission scarcity as seen from bus *i*. **Zero everywhere when no
  constraint binds.** This is the term this note is about.
- **MLC_i** — *Marginal Loss Component* at bus *i*. Cost of marginal losses
  delivering to *i* relative to the reference bus. Small, slowly varying,
  not our focus here.

Hub and zonal LMPs are load- or equal-weighted averages of their constituent
bus LMPs, so a hub MCC is just the weighted average of bus-level MCCs.

In the DA market these components come out of PJM's **SCUC/SCED** run
(Security-Constrained Unit Commitment / Economic Dispatch): a MILP that
commits units and sets dispatch for all 24 hours of the operating day to
minimize as-bid production cost subject to (a) energy balance, (b) unit
physical limits, and (c) a set of **monitored transmission constraints**
(thermal line/transformer limits, interface limits, voltage/stability
proxies, reactive-interface limits). The shadow prices (dual variables) on
those constraints are what become the MCC.

---

## 2. DA transmission constraints

### Who owns what: PJM owns the constraint, the solve owns the shadow price

A recurring confusion is whether virtuals "bid in" DA constraints. They do
not — and the cleanest way to keep it straight is to separate two things that
both get loosely called "the constraint":

**(a) The constraint object — 100% PJM, no market input.** A DA constraint is
`Σ SF·P ≤ Limit` for a specific *monitored facility + contingency* pair, and
every piece of it is PJM's:

- the **monitored-facility list and contingency list** come from PJM's EMS
  (M12 Attachment B: BES = ≥100 kV non-radial lines/transformers, generators
  >20 MVA, etc.);
- the **limit** is PJM's rating (thermal / voltage / stability), and PJM is
  the one who lowers it for an outage or a stability concern;
- the **shift factors** are computed from PJM's network model (DA defaults to
  the state-estimator distribution from the same hour one week prior,
  M11 §2.3.2.4);
- *which* constraints get enforced in a given run = a base set the Market
  Operator enters, plus whatever the SFT contingency pass throws back as
  violations (M11 §5.2.6).

Virtual bidders cannot create a constraint, name a flowgate, or move a limit.

**(b) Whether a constraint binds, and at what shadow price — endogenous to the
clearing; virtuals are one input among several.** `μ_k` is not "set" by
anyone — it is a dual variable that falls out of the SCUC/SCED optimization,
whatever value makes the last MW of overload relief economic given *all* the
injections and withdrawals in the model: physical generation offers, physical
demand bids, imports/exports, **and cleared INCs/DECs/UTCs** (M11 §2.3.8
treats INCs as injections and DECs as withdrawals in the security analysis).
So heavy INC volume behind a flowgate is extra "generation" loading that
constraint — it can push a slack constraint into binding or push an
already-binding `μ_k` higher; DEC volume on the import side can relax it; and
either way `MCC_i = -Σ_k SF(k,i)·μ_k` moves at every nearby bus.

Even the *ceiling* on `μ_k` is PJM's, not the market's: if a constraint
physically can't be controlled, M11 §2.17 sets its marginal value to the
**transmission constraint penalty factor** — and that cap is asymmetric
between the two DA passes: **$30,000/MWh in the dispatch run, $2,000/MWh in
the price-setting pricing run** (RT is $2,000/MWh in both). The market only
determines where `μ_k` lands *below* that cap.

So the honest one-liner: **the constraint is entirely PJM; the price on the
constraint is entirely endogenous, and virtuals co-determine it the same way
any other bid in the stack does — they don't set DA congestion, they're one
of the things the solve weighs when it does.** For modeling, that means treat
the *set of monitored constraints and their limits* as exogenous (driven by
outages/topology — the 7-day feeds), treat *which bind and how hard* as the
endogenous quantity you're predicting, and don't feed realized cleared
virtual volume in as a clean regressor for DA price — it's an output of the
same solve (see §3 below).

### What "binding" means

PJM monitors thousands of transmission facilities, but in any given hour only
a handful are *active* (the optimizer is holding flow at the limit). For each
monitored facility/contingency pair, SCED carries a constraint of the form:

```
sum_g  SF(facility, contingency, g) * P_g   <=   Limit(facility, contingency)
```

where `SF(...)` is the **shift factor** (a.k.a. PTDF / DFAX): the MW change
in flow on that facility, under that contingency, per MW injected at node *g*
and withdrawn at the reference. PJM monitors facilities both for the
**base case** (no outage) and for **post-contingency** flows (N-1: "if line X
trips, does line Y overload?"). Most binding DA constraints in PJM are
*contingency* constraints — written as `MONITORED_FACILITY` on
`CONTINGENCY` (e.g. `BEDINGTON-BLACK OAK` flowgate on the trip of some 500 kV
element).

When that inequality is tight, its **shadow price** `μ ≥ 0` ($/MWh) is the
marginal system cost of 1 more MW of capability on that facility. A constraint
with `μ = 0` is non-binding and contributes nothing to price.

### Transmission outages are the supply side of constraint capability

`Limit(facility, contingency)` is not static — it drops when a parallel path
is on a planned outage, when ambient ratings change, or when PJM reduces a
limit for stability. This is exactly why our outage feeds matter to price: an
outage doesn't show up in LMP directly, it shows up by **lowering a limit so
that a constraint that used to be slack now binds**, which lights up an MCC at
every electrically-nearby bus. The constraint→outage intersection in the
`pjm-da-constraints-brief` is built on this causal chain.

### What PJM publishes (and we scrape)

- **DA binding constraints / shadow prices** — Data Miner 2 feeds list, per
  hour, each constraint that bound in the DA run, its `ConstraintType`
  (Thermal / Voltage / Interface / Reactive), the monitored facility, the
  contingency, and the **shadow price**. This is the ground truth for "what
  set congestion yesterday."
- **Transmission outages** — planned/forced outage tickets with equipment,
  voltage class, and dates. Joined to constraints via geography/topology in
  our `transmission_outages_for_constraints` view.
- **Constraint network context** — our own PSS/E-derived shift-factor cache
  (`backend/mcp_server/data/shift_factors.py`) lets us compute, locally, how
  much a given hub moves per $1 of shadow price on a named flowgate
  (`/views/hub_impact`).

### Caveat: DA constraints ≠ RT constraints

The DA SCUC sees a forecast topology and *bid-in* (not physical) supply,
including virtuals. The RT SCED sees the actual grid. So a constraint can bind
DA-only (often *because* of where virtuals cleared) or RT-only (an outage or
derate that materialized after the DA run). The DART congestion spread is
largely the residual of this mismatch — see `lmps_dart_realization`.

---

## 3. How PJM sets the MCC

### The formula

Once SCED has solved and produced a shadow price `μ_k` for every binding
constraint *k*, the congestion component at bus *i* is a **shift-factor-weighted
sum of shadow prices**:

```
MCC_i  =  - Σ_k  SF(k, i) * μ_k
```

(sign convention varies by ISO doc; the point is it's linear in the shadow
prices, weighted by how strongly bus *i* loads each binding constraint).

Read it off:

- **No constraint binds** → every `μ_k = 0` → `MCC_i = 0` at every bus → LMP
  is just `SE + MLC`, nearly flat across the footprint. (These are the
  "boring" DA days.)
- **One constraint binds** → `MCC_i = -SF(k,i) * μ_k`. Buses on the
  *receiving/import* side of the flowgate (positive effective SF) get a
  **higher** LMP; buses on the *sending/export* side get a **lower** (even
  negative) LMP. The spread between two hubs equals `(SF(k, hubA) -
  SF(k, hubB)) * μ_k`.
- **Several constraints bind** → the effects superpose. Western Hub vs.
  AEP-Dayton, NiHub, EMAAC etc. spreads on a given day are basically a small
  linear combination of that day's handful of shadow prices.

### Why the shadow price is what it is

`μ_k` is endogenous: SCED raises it until re-dispatch (backing down cheap
generation behind the constraint, dispatching expensive generation in front
of it) just relieves the last MW of overload. So `μ_k` ≈ the **redispatch
cost differential** across the constraint — the offer-price gap between the
units the optimizer has to lean on. A constraint between a cheap-gas pocket
and a load pocket with only expensive peakers behind it produces a big shadow
price; one between two similar-cost areas produces a small one. Bid shape of
the marginal units → magnitude of `μ_k` → magnitude of every MCC that
constraint touches.

### Where virtual bids enter

Virtuals are financial-only MW that participate in the DA clear and settle
against RT:

- **INC (increment offer)** — a virtual *supply* offer at a bus. If it
  clears, you're paid DA LMP and you buy back at RT LMP. Profitable when
  `DA > RT` at that bus.
- **DEC (decrement bid)** — a virtual *demand* bid at a bus. If it clears,
  you pay DA LMP and sell at RT LMP. Profitable when `RT > DA`.
- **UTC (up-to-congestion)** — a virtual that bids the *spread* between a
  source and sink point, clearing only if the DA congestion+loss spread is
  below the bid; settles on the RT spread. Explicitly a congestion play.

Mechanically, a cleared INC looks to SCUC exactly like extra generation **at
that bus**, and a cleared DEC like extra load. Because the constraint
equations are `Σ SF*P ≤ Limit`, adding injection/withdrawal at a bus with a
non-trivial shift factor on a flowgate **changes the flow on that flowgate**,
which can:

- push a slack constraint into binding (creating an MCC where there was
  none),
- relax a binding constraint (shrinking an MCC), or
- change which units are marginal (moving `μ_k` and `SE`).

So virtuals don't just "predict" the DA price — in aggregate they **co-determine
DA congestion**. The textbook equilibrium story: rational virtuals arbitrage
the DA–RT gap until DA congestion converges to *expected* RT congestion; in
practice convergence is partial, biased (persistent DA premia on some paths),
and noisy, which is where DART traders and our DART-realization mart live.
Heavy INC clearing at an export-constrained bus is a classic mechanism for the
DA price there to be *pulled down* relative to RT.

### Practical implications for the models

- **MCC ≈ "the part of DA price we can attribute."** Predicting tomorrow's
  hub LMP largely reduces to (a) predicting `SE` (gas + load + the supply
  stack — see `supply_stack_model.md`, `next_day_gas_prices.md`) and (b)
  predicting *which* constraints bind and how hard. Outages and the 7-day
  topology feeds are the leading indicators for (b).
- **Spreads are cleaner than levels.** Hub-vs-hub and hub-vs-zone spreads
  isolate `Σ (ΔSF) μ_k` and drop `SE` entirely — often more learnable than
  the absolute LMP.
- **Don't model virtuals as exogenous.** Cleared virtual MW are an *output*
  of the same optimization that sets price; using realized DA virtual volumes
  as a feature for DA price is borderline circular. Use *bid-in* virtual
  volumes (pre-clear) or treat virtuals as a regime variable, not a clean
  regressor.
- **Shadow prices are sparse and heavy-tailed.** On most hours the binding
  set is empty or one constraint; on stressed days it's several with large
  `μ`. Any MCC model should expect a spike-and-slab-ish distribution, not a
  Gaussian.

---

## References worth pulling

PJM publishes the current revision of every manual at a stable `m##` URL
(`https://www.pjm.com/-/media/DotCom/documents/manuals/m11.pdf`, etc.); the
full library index is at <https://www.pjm.com/library/manuals>.

- **PJM Manual 11 — Energy & Ancillary Services Market Operations** — DA
  scheduling, virtual transactions (INC/DEC/UTC), constraint handling, the
  market clearing engines.
  <https://www.pjm.com/-/media/DotCom/documents/manuals/m11.pdf>
- **PJM Manual 12 — Balancing Operations** — RT dispatch, transmission
  monitoring, the SE/MCC/MLC framing on the RT side.
  <https://www.pjm.com/-/media/DotCom/documents/manuals/m12.pdf>
- **PJM Manual 6 — Financial Transmission Rights** — the same
  shadow-price / shift-factor machinery seen from the FTR/ARR side; useful for
  the congestion-revenue view.
  <https://www.pjm.com/-/media/DotCom/documents/manuals/m06.pdf>
- **LMP training** — "Locational Marginal Pricing Components" deck
  (<https://www.pjm.com/-/media/training/nerc-certifications/markets-exam-materials/mkt-optimization-wkshp/locational-marginal-pricing-components.ashx>),
  the LMP/FTR 101 course (<https://learn.pjm.com/three-priorities/buying-and-selling-energy/lmp.aspx>),
  and the reserve-shortage / LMP-formation paper
  (<https://www.pjm.com/-/media/DotCom/markets-ops/energy/real-time/reserve-shortage-pricing-paper.pdf>)
  — the `SE + MCC + MLC` decomposition and shift-factor math worked through
  with examples.
- Our internal: `backend/mcp_server/` constraint + `hub_impact` views;
  `lmps_dart_realization` mart; `transmission_outages_for_constraints`.
