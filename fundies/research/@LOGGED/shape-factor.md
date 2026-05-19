---
timestamp_local: "2026-03-10 19:23 ET"
timestamp_utc: "2026-03-10T23:23:00Z"
market: "power"
source: "PJM Stack Model spreadsheet"
tags: [shape-factor, load-curve, stack-model, dispatch]
summary: "Deep dive into shape factors in PJM Stack Model — how they work, where they live, and their (informational-only) role"
signal_relevance: "Understanding load shape vs net load shape (duck curve) for price forecasting"
confidence: 5
status: "logged"
---

# Shape Factors in the PJM Stack Model

Source: `PJM_Stack_Model_v1_2026_mar_10.xlsx`

---

## What Is a Shape Factor?

A shape factor is the **fraction of daily peak load** that demand reaches at a given hour. The 24 values (one per hour) define the *shape* of the daily load curve:

- **1.000** = the peak hour (hour 18 / 6:00 PM)
- **0.658** = the trough hour (hour 4 / 4:00 AM), meaning load is only 65.8% of peak

Together they sketch the classic demand profile: low overnight, ramping in the morning, plateau during the day, peaking in early evening, then declining.

---

## Where Shape Factors Live

**Sheet:** `Hourly Dispatch`, cells **Q11:Q34**
**Header (rows 9-10):** *"HOURLY LOAD SHAPE FACTORS (editable -- fraction of peak load)"*

This is a **user-editable input table** — 24 hardcoded decimal values, one per hour:

| Hour | Time  | Shape Factor | Period         |
|------|-------|-------------|----------------|
| 0    | 00:00 | 0.710       | Overnight      |
| 1    | 01:00 | 0.688       | Overnight      |
| 2    | 02:00 | 0.672       | Overnight      |
| 3    | 03:00 | 0.663       | Overnight      |
| 4    | 04:00 | **0.658** (trough) | Overnight |
| 5    | 05:00 | 0.671       | Morning Ramp   |
| 6    | 06:00 | 0.714       | Morning Ramp   |
| 7    | 07:00 | 0.776       | Morning Ramp   |
| 8    | 08:00 | 0.840       | Morning Ramp   |
| 9    | 09:00 | 0.880       | Morning Ramp   |
| 10   | 10:00 | 0.907       | Daytime        |
| 11   | 11:00 | 0.928       | Daytime        |
| 12   | 12:00 | 0.940       | Daytime        |
| 13   | 13:00 | 0.948       | Daytime        |
| 14   | 14:00 | 0.962       | Daytime        |
| 15   | 15:00 | 0.975       | Daytime        |
| 16   | 16:00 | 0.982       | Daytime        |
| 17   | 17:00 | 0.990       | Evening Peak   |
| 18   | 18:00 | **1.000** (peak) | Evening Peak |
| 19   | 19:00 | 0.985       | Evening Peak   |
| 20   | 20:00 | 0.953       | Evening Peak   |
| 21   | 21:00 | 0.905       | Daytime        |
| 22   | 22:00 | 0.845       | Daytime        |
| 23   | 23:00 | 0.779       | Daytime        |

---

## How They Flow Through the Model

### Step 1 — Input table
`'Hourly Dispatch'!Q11:Q34` — hardcoded, editable values.

### Step 2 — Display column
`'Hourly Dispatch'!E6:E29` — each cell is a simple reference:

- `E6 = =Q11` (hour 0, 0.710)
- `E7 = =Q12` (hour 1, 0.688)
- ...through...
- `E29 = =Q34` (hour 23, 0.779)

Column E is labeled **"Shape Factor"** in the main hourly dispatch table (columns B-M).

### Step 3 — Key finding: informational only
**Column E is not referenced by any downstream formula.** No cell on any sheet pulls from `E6:E29`. The shape factors are currently a **reference benchmark**, not a calculation driver.

---

## The Actual Load Path (Independent of Shape Factors)

The real load flowing through the model comes from a completely separate chain:

```
'Forecast Inputs'!W8:W31   (ISO hourly load forecast, hardcoded)
       |
'Forecast Inputs'!U8:U31   (=W8, mirror)
       |
'Forecast Inputs'!BS8:BS31 (Net Load = Total - Wind - Solar)
       |
'Hourly Dispatch'!F6:F29   (PJM Load MW)
       |
'Hourly Dispatch'!H6:H29   (Net Dispatch = MAX(0, Load - Must-Run))
       |
'Hourly Dispatch'!I6:I29   (Clearing Price via MINIFS on Stack Model)
       |
'Hourly Dispatch'!J6:J29   (Marginal Fuel Type)
       |
Ramp Analysis sheet         (pulls from columns F, H, I, K, L)
```

---

## Why They Matter: Spotting the Duck Curve

By comparing `Shape Factor x Peak Load` to the actual net load, you can see where the real profile departs from the "typical" shape:

| Hour | SF x Peak | Actual Net Load | Ratio |
|------|----------|-----------------|-------|
| 0    | 69,806   | 70,093          | 1.00  |
| 12   | 92,419   | 77,676          | **0.84** |
| 18   | 98,318   | 87,811          | **0.89** |

The midday divergence (ratio ~0.84) is the **duck curve effect** — solar generation depresses net load well below what the gross-load shape factor predicts.

---

## Adjacent Inputs (Same Sheet)

The shape factor table sits beside other key assumptions at `'Hourly Dispatch'!O2:Q6`:

| Cell | Parameter               | Value |
|------|-------------------------|-------|
| P2   | PJM Peak Load (MW)      | `=MAX('Forecast Inputs'!BP8:BP31)` = 98,318 |
| P3   | Weekday Type            | 1 (1=Weekday, 0=Weekend) |
| P4   | Season                  | 3 (1=Summer, 2=Winter, 3=Spring/Fall) |
| P5   | Must-Run MW             | 70,494 |
| P6   | Load Uncertainty +/- MW | 2,500 |

---

## Are There Multiple Kinds of Shape Factors?

**No.** Only one shape factor table exists in the workbook. Two other incidental uses of the word "shape" are unrelated:

- `Stack Model!B81` / `PJM Raw Data!A3712`: "PPL Aluminum Shapes PV Plant" — a solar plant name
- `Forecast Inputs!B36-B37`: Instructions describing "typical diurnal shape" for wind and "bell-curve shape" for solar — descriptive text, not the shape factor table

---

## Quick-Start Walkthrough

1. **Open `Hourly Dispatch`.** Look at columns O-R, rows 9-34. This is the shape factor input table.
2. **Read column Q (rows 11-34).** These are the 24 hourly fractions. Hour 18 = 1.0 (peak). Hour 4 = 0.658 (trough). Column R labels the period (Overnight, Morning Ramp, Daytime, Evening Peak).
3. **Look at column E (rows 6-29).** These mirror the shape factors into the main dispatch table via `=Q11`, `=Q12`, etc.
4. **Compare column E to column F.** Column F is actual PJM net load from the forecast. Where `E x Peak Load` diverges from F, the real profile departs from the typical shape — usually because of renewables.
5. **Understand they are informational.** Changing `Q11:Q34` updates column E but does **not** change any load, price, or dispatch calculation. The model uses the ISO forecast directly.
6. **If you wanted shape factors to drive load** (e.g., for a scenario without a detailed forecast), you would modify column F formulas to `= Shape Factor x Peak Load` instead of pulling from the Forecast Inputs sheet.

---

## Summary

| Item | Details |
|------|---------|
| **Definition** | Fraction of daily peak load at each hour (0-1), defining the typical daily demand curve shape |
| **Key Input Location** | `'Hourly Dispatch'!Q11:Q34` — 24 editable decimal values |
| **Key Display Location** | `'Hourly Dispatch'!E6:E29` — references Q11:Q34 |
| **Key Formulas** | `E6=Q11` through `E29=Q34` (simple references; no downstream formula consumes them) |
| **Outputs Impacted** | Currently **none** — shape factors are informational only; actual load flows from `'Forecast Inputs'!W` -> `U` -> `BS` -> `'Hourly Dispatch'!F` |
| **Inspect First** | `'Hourly Dispatch'!Q11:Q34` (input table), then compare column E vs column F |

---

## Further Reading and Resources

### Foundational Concepts: Load Shapes, Load Duration Curves, and Market Clearing

- [EIA Handbook — Electricity Load Shapes (Section B3)](https://www.eia.gov/analysis/handbook/pdf/Handbook%20Section%20B3_Electricity%20Load%20Shapes.pdf) — U.S. Energy Information Administration reference on how load shapes are constructed and used in planning. Start here for the basics.
- [Load Duration Curve — ScienceDirect Overview](https://www.sciencedirect.com/topics/engineering/load-duration-curve) — Concise academic summary of load duration curves, their relationship to load shapes, and how they inform generation planning.
- [Load Duration Curve Analysis — Edward Bodmer](https://edbodmer.com/load-duration-curve-analysis/) — Practitioner-oriented walkthrough of long-term marginal cost and load duration curves with Excel examples. Closest in spirit to the PJM Stack Model workbook.
- [Load Profile — Wikipedia](https://en.wikipedia.org/wiki/Load_profile) — Quick reference linking load profiles, load factors, and shape factors together.

### How US ISO Markets Work

- [FERC — Electric Power Markets](https://www.ferc.gov/electric-power-markets) — Official overview of all US wholesale electricity markets, their structure, and how ISOs/RTOs operate.
- [FERC — Introductory Guide to Electricity Markets](https://www.ferc.gov/introductory-guide-electricity-markets-regulated-federal-energy-regulatory-commission) — Beginner-friendly guide to market design, day-ahead vs real-time, and price formation.
- [ISO-NE — How Resources Are Selected and Prices Are Set](https://www.iso-ne.com/about/what-we-do/how-resources-are-selected-and-prices-are-set) — Clear walkthrough of economic dispatch and merit order pricing from ISO New England. The mechanics are the same as PJM.
- [ISO-NE — Day-Ahead and Real-Time Energy Markets](https://www.iso-ne.com/markets-operations/markets/da-rt-energy-markets) — Deeper dive into the two-settlement system.
- [CME Group — Understanding Basics of the Power Market](https://www.cmegroup.com/education/courses/introduction-to-power/understanding-basics-of-the-power-market0.html) — Financial/trading perspective on power markets.
- [Yes Energy — How Competitive, Deregulated Energy Markets Work](https://blog.yesenergy.com/yeblog/competitive-markets-functionality) — Good plain-English explainer with diagrams.

### PJM-Specific Documents

- [PJM Manual 19: Load Forecasting and Analysis](https://www.pjm.com/-/media/DotCom/documents/manuals/m19.ashx) — PJM's own manual on how they forecast load, including load shapes and seasonal profiles. This is the authoritative source for how PJM thinks about load shapes.
- [PJM Manual 18: PJM Capacity Market](https://www.pjm.com/-/media/DotCom/documents/manuals/m18.ashx) — Covers peak load contributions and load factor calculations in the capacity market.
- [PJM — Energy Transition in PJM: Emerging Characteristics of a Decarbonizing Grid (2022)](https://www.pjm.com/-/media/DotCom/library/reports-notices/special-reports/2022/20220517-energy-transition-in-pjm-emerging-characteristics-of-a-decarbonizing-grid-white-paper-final.ashx) — PJM white paper on how renewables are reshaping the hourly load profile and shifting resource adequacy risk.
- [PJM Data Miner 2 — Hourly Load (Metered)](https://dataminer2.pjm.com/feed/hrl_load_metered/definition) — Where you can pull actual hourly load data to build or validate your own shape factors.

### The Duck Curve and Net Load Shape

- [CAISO — What the Duck Curve Tells Us About Managing a Green Grid](https://www.caiso.com/documents/flexibleresourceshelprenewables_fastfacts.pdf) — The original CAISO duck curve document. Essential reading for understanding why net load shape diverges from gross load shape.
- [NREL — Ten Years of Analyzing the Duck Chart (2018)](https://www.nrel.gov/news/program/2018/10-years-duck-curve.html) — NREL retrospective on how the duck curve prediction played out and what it means for grid operations.
- [Yes Energy — The Duck Curve Explained](https://www.yesenergy.com/blog/the-duck-curve-explained-impacts-renewable-energy-curtailments) — Accessible explainer connecting duck curve to curtailment and market price impacts.
- [Duck Curve — Wikipedia](https://en.wikipedia.org/wiki/Duck_curve) — Quick reference with diagrams.

### Academic Papers

- [Cramton (1999) — The Role of the ISO in U.S. Electricity Markets](https://www.cramton.umd.edu/papers1995-1999/99ej-role-of-the-iso-in-us-electricity-markets.pdf) — Foundational academic paper on ISO market design by Peter Cramton (UMD). Good for understanding why markets are structured the way they are.
- [NREL — Supply Curve Analysis from PJM Price-Load Data](https://docs.nrel.gov/docs/fy18osti/70954.pdf) — NREL developed rolling supply curves from 2015 PJM hourly price and load data. Directly relevant to understanding how the stack model relates to real price formation.
- [The Load Curve and Load Duration Curves in Generation Planning (2023)](https://ieomsociety.org/proceedings/2023australia/245.pdf) — Recent academic paper on load curves in generation planning.

### GitHub Repositories — Example Code

**Data Access:**
- [gridstatus/gridstatus](https://github.com/gridstatus/gridstatus) — Python library providing a unified API for pulling load, price, and fuel mix data from PJM, CAISO, ERCOT, MISO, NYISO, SPP, ISO-NE. Best starting point for pulling real data. `pip install gridstatus`
- [rzwink/pjm_dataminer](https://github.com/rzwink/pjm_dataminer) — PJM-specific data miner scripts for exporting data from PJM's public APIs.
- [CaffeineLab/isodata](https://github.com/CaffeineLab/isodata) — Another Python data collection tool with PJM support.
- [PJM Hourly Energy Consumption — IEEE DataPort](https://ieee-dataport.org/documents/pjm-hourly-energy-consumption) — Public dataset of PJM hourly consumption (2002-2018, 145k records). Good for building historical shape factors.

**Merit Order / Market Clearing Models:**
- [ksaswin/Electricity-Market-Clearing](https://github.com/ksaswin/Electricity-Market-Clearing) — Simple Python implementation: find market clearing price and quantity from a supply stack. Closest analog to the MINIFS-based clearing logic in the PJM Stack Model workbook.
- [AyrtonB/Merit-Order-Effect](https://github.com/AyrtonB/Merit-Order-Effect) — Calculates the merit order effect of renewables on electricity prices. Good for understanding how renewable penetration shifts the clearing price. [Documentation site](https://ayrtonb.github.io/Merit-Order-Effect/)
- [Merit Order Gist by hbshrestha](https://gist.github.com/hbshrestha/9cc0f983caca845269f702aed3859798) — Compact Python gist: power plants setting the market clearing price via merit order. Companion to the [Medium tutorial](https://medium.com/data-science/merit-order-and-marginal-abatement-cost-curve-in-python-fe9f77358777).
- [quintel/merit](https://github.com/quintel/merit) — Ruby-based system for calculating hourly electricity loads with a merit order (useful for understanding the logic even if not Python).

**Full Dispatch / Power System Models:**
- [Critical-Infrastructure-Systems-Lab/PowNet](https://github.com/Critical-Infrastructure-Systems-Lab/PowNet) — Python production cost model for simulating operational scheduling of large-scale power systems, including unit commitment and economic dispatch.
- [PyPSA](https://pypsa.org/) — Python for Power System Analysis. Full open-source framework for dispatch, unit commitment, optimal power flow, and capacity expansion. The most comprehensive open-source option.
- [gschivley/battery_model](https://github.com/gschivley/battery_model) — Models battery storage dispatch against market prices. Relevant if you want to understand how storage interacts with load shape.
- [UNSW-CEEM/nempy](https://github.com/UNSW-CEEM/nempy) — Python dispatch model for the Australian NEM. Different market but same economic dispatch principles.
- [rebase-energy/awesome-energy-models](https://github.com/rebase-energy/awesome-energy-models) — Curated list of open-source Python energy models. Good meta-resource.

### Suggested Reading Order

1. **EIA Load Shapes handbook** (what shape factors are in general)
2. **FERC Introductory Guide** or **ISO-NE "How Prices Are Set"** (how markets clear)
3. **PJM Manual 19** (how PJM specifically forecasts load)
4. **CAISO Duck Curve PDF** (why net load shape differs from gross)
5. **gridstatus** repo + **Electricity-Market-Clearing** repo (hands-on code)
6. **NREL Supply Curve paper** (tying it all together with real PJM data)
