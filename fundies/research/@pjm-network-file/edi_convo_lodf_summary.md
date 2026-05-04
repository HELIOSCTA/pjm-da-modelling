# LODF / PTDF Network Model Conversation Summary

**Source:** [Claude conversation](https://claude.ai/share/d24465c0-188f-405c-9e90-1f04c58c9a9e)
**Date range:** February 25 -- 27, 2026
**Participants:** Edi (user) + Claude

---

## Table of Contents

1. [Project Goal](#1-project-goal)
2. [Phase 1: Can We Build an LODF Model?](#2-phase-1-can-we-build-an-lodf-model)
3. [Phase 2: CEII Network Access Changes Everything](#3-phase-2-ceii-network-access-changes-everything)
4. [Phase 3: Stack Modeling and DA Price Forecasting](#4-phase-3-stack-modeling-and-da-price-forecasting)
5. [Phase 4: Gas Basis Integration](#5-phase-4-gas-basis-integration)
6. [Phase 5: PUDL -- Unit-Level Generation Data](#6-phase-5-pudl----unit-level-generation-data)
7. [Phase 6: Coal Switching Model](#7-phase-6-coal-switching-model)
8. [Phase 7: Data Acquisition and Parsing](#8-phase-7-data-acquisition-and-parsing)
9. [Phase 8: PTDF/LODF Computation -- The Memory Problem](#9-phase-8-ptdflodf-computation----the-memory-problem)
10. [Key Datasets Collected](#10-key-datasets-collected)
11. [Architecture Diagrams](#11-architecture-diagrams)
12. [Current Status and Next Steps](#12-current-status-and-next-steps)

---

## 1. Project Goal

Build a **physics-informed congestion trading model** for PJM that combines:

- **PTDF/LODF/OTDF matrices** computed from PJM's actual network topology (PSS/E RAW file)
- **Historical and planned transmission outages** to track dynamic topology states
- **Gas price data** across all PJM delivery points for spatial marginal cost modeling
- **PUDL/EIA/EPA data** for unit-level heat rates, ramp rates, and dispatch behavior

**Primary use case:** Financial trading in PJM DA, RT, and virtual congestion markets (INCs/DECs/FTRs).

---

## 2. Phase 1: Can We Build an LODF Model?

### Concept

LODF (Line Outage Distribution Factor): when line *k* trips, what fraction of its pre-outage flow redistributes onto line *l*?

```
LODF(l, k) = DeltaFlow_l / Flow_k_pre_outage
```

### Two Approaches Identified

| Approach | Pros | Cons |
|----------|------|------|
| **Physics-based** (PyPSA + network model) | Complete LODF matrix, N-1 capable | Requires accurate topology |
| **Empirical** (from historical outage data) | Ground-truthed to real flows | Sparse -- only covers observed contingencies |
| **Combined (recommended)** | Physics fills gaps, data validates | Requires name-matching between datasets |

### Key Tools

- **PyPSA** -- has built-in `calculate_BODF()` (Branch Outage Distribution Factor = LODF generalized)
- **PyPSA-USA** -- open-source US transmission model as a proxy (before CEII access)
- **PJM public data** -- contingency list XML, TFOL, RT 5-min LMPs, generation outages

---

## 3. Phase 2: CEII Network Access Changes Everything

With access to PJM's actual CEII network data (PSS/E RAW file), the project upgraded from approximation to **faithful replication of PJM's own contingency analysis**.

### What the RAW File Contains

| Component | Count |
|-----------|-------|
| Buses | 19,773 |
| AC Lines | 14,976 |
| Transformers | 7,671 |
| Generators | 4,047 |
| Load buses | 17,503 |
| Areas | 17 |
| Zones | 75 |
| Owners | 55 |

**Snapshot:** September 9, 2021 (PSS/E v30 format, 100 MVA base, 60 Hz)

### Five Tiers of Capability Unlocked

1. **Exact LODF matrix** -- replicate PJM's EMS output within DC approximation error
2. **Reconcile against PJM's contingency list** -- map outage events to LODF columns
3. **OTDF computation** -- the actual factor PJM uses for congestion pricing:
   ```
   OTDF(m, k, bus) = PTDF(m, bus) + LODF(m, k) * PTDF(k, bus)
   ```
4. **Validate against historical outages** -- compare predicted vs actual post-outage flows
5. **Dynamic LODF** -- recompute the matrix for each topology state as outages change over time

---

## 4. Phase 3: Stack Modeling and DA Price Forecasting

### The LMP Equation

```
LMP_n = lambda_energy + SUM_k(mu_k * PTDF_nk) + loss_component
```

Where `mu_k` = shadow price on constraint *k*, and `PTDF_nk` = shift factor of bus *n* on constraint *k*.

### Three Modeling Philosophies

1. **Structural / Physics-First** -- build the actual DA market clearing problem (needs offer data)
2. **Statistical / ML-First** -- treat LMPs as time series with features (doesn't need offers)
3. **Hybrid (recommended)** -- use physics to generate features, ML to predict

### Hybrid Model Features

The topology state from outage data becomes the key differentiator. Features include:

- Number of active outages
- LODF sensitivity per major constraint
- Constraint proximity (loading % on each element)
- Post-contingency loading for each contingency
- Elevated contingency count (double-contingency risk)

### Congestion Probability / FTR Valuation

```
FTR(A->B) value = SUM_constraints P(bind) * E[shadow_price | bind] * (OTDF_B - OTDF_A)
```

---

## 5. Phase 4: Gas Basis Integration

### Why Gas is Central

Gas-fired generation is marginal 60-70%+ of hours in PJM. The energy component of every LMP is:

```
lambda_energy = heat_rate * fuel_price + VOM
```

But **which** gas price matters depends on which plant is marginal, varying by hour/season/topology.

### Gas Price Geography in PJM

| Delivery Point | Typical Price | Region |
|----------------|--------------|--------|
| Dom South | $2--3/MMBtu | Appalachia (cheap) |
| TCO Pool | $2.50--3.50 | Ohio/WV (middle) |
| Tetco M3 | $4--8 | Mid-Atlantic (constrained) |
| Transco Z6 | $5--12 | NY/NJ (most expensive) |

### Key Trading Applications

- **DA price model**: Hub price (gas-driven) + congestion spread (topology-driven)
- **DA/RT spread (virtuals)**: Exploit gas intraday moves, unplanned outages, load forecast errors
- **FTR valuation**: Path-level expected value across gas price scenarios
- **Gas x Power spread trading**: Detect dislocations between gas basis and power spread (mean-reverting z-score signals)

### Cold Snap Playbook

The single most reliable PJM trading pattern: Transco Z6 spikes -> eastern plants lose gas supply -> AP South and other interfaces bind hard -> eastern LMPs go parabolic. Modelable and positionable in advance.

---

## 6. Phase 5: PUDL -- Unit-Level Generation Data

### Data Sources in PUDL

| Source | Content | Granularity |
|--------|---------|-------------|
| EIA-860 | Every generator: capacity, fuel type, prime mover, retirement date, fuel switching capability | Annual |
| EIA-923 | Monthly generation, fuel consumption, fuel costs, fuel receipts | Monthly |
| EPA CEMS | Hourly gross load, SO2, CO2, NOx per unit (since 1995) | Hourly |
| FERC Form 1 | Non-fuel O&M costs, plant-level financials | Annual |
| FERC EQR | Bilateral contracts and transactions | Quarterly |

### What CEMS Unlocks

- **Unit-level heat rate curves**: `HR(MW) = a + b/MW + c*MW` (part-load efficiency)
- **Ramp rate estimation**: hour-over-hour MW changes, startup times, min downtime
- **Capacity factor by season/hour**: availability intelligence for stack modeling
- **Marginal offer price reconstruction**: `offer = total_HR * gas_price + VOM`

### Fuel Switching Signal

EIA-860 tracks co-firing and fuel switching capability. When gas spikes above a threshold, identify which generators switch to distillate fuel oil (DFO) -- this changes the spatial dispatch and congestion patterns.

### Retirement Signal

Track the declining switchable coal capacity from EIA-860 retirement dates. Forward stack modeling for FTR valuation.

---

## 7. Phase 6: Coal Switching Model

### Core Economics

```
Spark spread = LMP - (Gas_price * Gas_HR) - VOM_gas
Dark spread  = LMP - (Coal_price * Coal_HR) - VOM_coal
```

**Switching price** = gas price where dark spread exceeds spark spread (~$3.50--5.00/MMBtu at typical coal costs).

### What Makes Coal Harder Than Gas

1. **Delivered price != spot price** -- plants run on long-term contracts (EIA-923)
2. **Coal type varies** -- CAPP (~12,500 BTU/lb), PRB (~8,800), ILB (~11,000)
3. **Transportation cost is plant-specific** -- rail distance matters
4. **Stockpile constraints** -- 25--60 days of burn; below 20 days = constrained
5. **Minimum run constraints** -- 4--12 hour restarts, 40--60% minimum load

### Spatial Integration with PTDF

Coal plants are geographically concentrated (AEP/Appalachia). When coal dispatches heavily, power flows east through AP South. The model maps each coal unit's bus ID to its PTDF row and predicts directional flow on AP South.

### Trading Signals

- **Switching trigger**: gas crosses plant-specific threshold -> flag MW that flip from hold to dispatch
- **Stockpile stress**: screen November EIA-923 for plants with <30 days burn -> winter vulnerability
- **Retirement cliff**: remaining switchable coal declining yearly -> structural gas-to-power correlation increase

---

## 8. Phase 7: Data Acquisition and Parsing

### Files Downloaded and Parsed

**PJM Network Model CSVs (11 files):**
- PSS/E Branch Mapping (29,577 branches)
- Aggregate Definitions + LT Round 5 (177 aggregates each)
- Interface Definitions + Limits (10 interfaces)
- PAR Data (63 phase shifters at 29 substations)
- 500kV Bus Mapping
- B1-B2-B3 Mapping (24,600 entries)
- Load Apportionment (standard + LT Round 5)
- FTR Constraint Definitions (6 rows -- public stub only)
- Uncompensated Parallel Flow

**ARR 2026/2027 Files (11 files):**
- Contingencies Modeled: 1,450 N-1 contingencies (435 at 345kV, 307 at 230kV, 303 at 138kV, 232 at 500kV, 72 at 765kV)
- AP South: 15 monitored contingencies across 5 major 500kV paths
- Key System Upgrades: 47 projects (Dominion 500kV in-service Feb 28, 2026)
- Stage 1 Resources: 13,500 MW of retired capacity footprint-wide
- Historically Congested Facilities: Eugene-Bunce 345kV at 1,255 MW uncompensated flow
- Transmission Outages, Interface Contingencies, MISO M2M, NSPL, P2P

**FTR 2025/2026 Files (12 files):**
- 1,055 contingencies, 1,677 valid source/sinks, 4,927 option paths
- Rounds 1--4: 506,311 cleared FTR records, 1,331 unique binding constraints
- NSPL ARR credits ($1.81B total)

**Bus Master Crosswalk:** 15,427 named buses with zone, hub, and aggregate membership joined.

---

## 9. Phase 8: PTDF/LODF Computation -- The Memory Problem

### Why It Keeps Crashing

Dense matrix sizes at full PJM scale:

| Matrix | Dimensions | Memory |
|--------|-----------|--------|
| PTDF (branches x buses) | 22,647 x 19,773 | ~3.4 GB |
| LODF/BODF (branches x branches) | 22,647 x 22,647 | ~4.1 GB |
| Full OTDF (all contingencies) | 1,055 x 22,647 x 19,773 | ~1.9 TB |

The cloud container (8--16 GB) hits OOM. Additionally, the ~60--90 second process timeout kills the LU factorization.

### The Solution: Targeted Sparse Computation

Instead of the full matrices, compute only what matters for trading:

1. **Sparse B-matrix** (scipy CSR) for admittance matrix factorization
2. **Only PTDF rows for the ~1,331 binding constraints** to the ~1,677 valid FTR nodes
3. **OTDF on-the-fly** using rank-1 update: `OTDF(m,k,bus) = PTDF(m,bus) + LODF(m,k) * PTDF(k,bus)`
4. **Checkpointed pipeline** so partial progress is saved

This reduces memory from ~7 GB to ~50--200 MB while covering 100% of FTR-relevant analysis.

### Partial Results Already Computed

- `ptdf_all_branches_zone_hub.parquet` -- all 19,934 branches with PTDF to 20 zones + 20 hubs
- `ptdf_otdf_binding_constraints.parquet` -- 17 binding constraints with PTDF+OTDF

### What Remains

A `compute_ptdf_lodf.py` script was written with checkpointing at every expensive step (B-matrix LU, THETA solve, results). It needs to run in a **persistent environment** with sufficient memory -- the cloud sandbox kept timing out. A handoff document was created for running in Codex or locally.

---

## 10. Key Datasets Collected

### Complete File Inventory (47 parquets + RAW)

| Category | Files | Key Content |
|----------|-------|-------------|
| Network topology | RAW + 11 CSVs | Full bus/branch/gen/xfmr model |
| Bus crosswalk | 1 parquet | 15,427 buses with zone/hub/aggregate |
| ARR auction data | 11 parquets | Contingencies, outages, upgrades, retirements |
| FTR auction data | 12 parquets | Cleared FTRs, binding constraints, paths |
| Partial PTDF results | 2 parquets | Zone/hub PTDFs + 17 binding constraint OTDFs |

### AP South Interface -- Key Numbers

- **4 defining branches**: DOUBS 500kV, MEADOWBR-GREENGAP, MEADOWBR-MTSTORM4, MTSTORM4-VALLEY4
- **Base case limit**: 4,800 MW
- **N-1 limit** (Black Oak-Bedington contingency): 1,450--1,700 MW
- **15 monitored contingencies** in the ARR data
- **Bottleneck**: MTSTORM4-VALLEY4 at 2,832 MVA rating (lowest of the four)

### NY Border Phase Shifters

| Substation | Angle | Rating | Notes |
|------------|-------|--------|-------|
| WALDWICK | 28.3deg / 21.4deg / 27.5deg | ~850 MVA each | Major NY import constraint |
| RAMAPO | 26.6deg x 2 | 9,999 MVA (placeholder) | |
| GOETHALS | 32.8deg | | Highest angle -- NYC flow control |
| FARRAGUT | 36.7deg | | Highest angle -- LI flow control |
| ESSEX | 0.35deg | | Minimal control |

---

## 11. Architecture Diagrams

### Full Integrated Trading System

```
INPUTS
+------------------------------------------------------------+
|  CEII Network (RAW) --> PTDF/LODF                          |
|  Outage Data --> Topology State                             |
|  Gas Prices (all PJM basis points)                          |
|  PJM DA Load Forecast (public)                              |
|  Weather Forecasts                                          |
|  PJM Contingency List                                       |
|  PUDL: EIA-860/923, EPA CEMS, FERC Form 1                  |
+---------------------------+--------------------------------+
                            |
            +---------------+----------------+
            |               |                |
            v               v                v

     STACK MODEL      DA PRICE MODEL     RT MONITOR
     -----------      --------------     ----------
     Merit order      Hub forecast       Live constraint
     by gas point     + congestion       detection from
                      spread             5-min LMPs
     Marginal unit    topology-aware     Regime change
     identification   gas-adjusted       alerts

            |               |                |
            +---------------+----------------+
                            |
                      TRADING SIGNALS
            +---------------+----------------+
            |               |                |
            v               v                v

    VIRTUAL BIDS     FTR VALUATION    GAS x POWER SPREAD
    ------------     -------------    ------------------
    INC/DEC at       Path-level       Basis dislocation
    high-alpha       expected value   z-score signals
    nodes            by gas scenario
                                      Cold snap playbook
    DA-RT spread     ARR vs FTR       Corridor-level
    prediction       comparison       mean reversion

            |               |                |
            +---------------+----------------+
                            |
                     POSITION SIZING
                     & RISK MANAGEMENT
```

### Data Flow: RAW File to Trading Model

```
RAW FILE                     PUBLIC CSVs                CEII CSVs
--------                     -----------                ---------
Bus numbers                  PSSE Branch Mapping        Contingency List
Branch R/X/B/limits  ------> maps bus IDs to    -----> maps contingency
Generator bus IDs            facility names              names to branch IDs
Transformer data
PAR base positions   ------> PAR Data CSV       -----> Contingency
                             corrects taps              Flowgate List

         |                   Aggregate Defs     -----> FTR Auction
         v                   maps buses->hubs          Sensitivities
  PyPSA PTDF/LODF   ------> Interface Limits
  computation                (constraint caps)

         |                   Load Apportionment
         v                   weights hub LMPs
  LODF Matrix +      ------> FTR Constraint    ------> Complete
  Outage Timeline            Limits                     N-1 security
                                                        model
         |                   B1/B2/B3 Mapping
         v                   (trust boundaries)
  Gas Prices         ------> PUDL Stack Model  -------> Full DA/RT/
  Outage Data                (unit heat rates)          Congestion
  PJM LMP History            (ramp rates)               Trading
  DA Load Forecasts          (fuel types)               Model
```

---

## 12. Current Status and Next Steps

### Completed

- [x] PJM RAW file parsed (19,773 buses, 14,976 lines, 7,671 transformers, 4,047 generators)
- [x] All public PJM CSV/Excel files downloaded and parsed to parquet
- [x] ARR 2026/2027 data parsed (1,450 contingencies, upgrades, retirements)
- [x] FTR 2025/2026 data parsed (506,311 cleared records, 1,331 binding constraints)
- [x] Bus master crosswalk built (15,427 buses with zone/hub/aggregate)
- [x] Partial PTDF computed (all branches to zones/hubs + 17 binding constraint OTDFs)
- [x] Handoff document written for Codex with complete `compute_ptdf_lodf.py` script

### Blocked / In Progress

- [ ] **Full PTDF/LODF computation** -- needs persistent environment with >8 GB RAM (kept hitting OOM/timeout in cloud sandbox)
- [ ] PUDL data integration (generator master table with heat rates, ramp rates)
- [ ] Gas delivery point -> generator bus mapping
- [ ] Coal switching model integration
- [ ] FTR arbitrage analysis (compare cleared FTR prices to PTDF-implied congestion)

### Recommended Execution Order

1. **Run `compute_ptdf_lodf.py` locally** or in Codex with the 13 parquet files
2. Build generator crosswalk: EIA plant ID -> PJM bus ID -> gas delivery point
3. Pull PUDL/CEMS data for PJM generators
4. Construct merit order stack with real heat rates and local gas prices
5. Build DA price model with topology features
6. Build RT congestion monitor
7. Build FTR valuation engine
8. Integrate coal switching layer

### Key Insight

> "The topology state from planned outages alone is a substantial alpha source -- a planned outage on a key path tomorrow changes every LODF in that corridor, and that shows up in DA prices. Most models don't see this until it's already in prices."
