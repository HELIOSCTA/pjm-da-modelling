# Next-Day Gas Prices for PJM DA Modelling

How ICE physical next-day gas trades, why our spine is keyed on `gas_day` (not `trade_date`), and what feature value the DA model should use.

## TL;DR

- **PJM DA bidders need one number per (gas_day, hub)**: the locked daily settlement that ICE publishes at session close.
- **The right query** is `WHERE gas_day = forecast_target_date` against `pjm_da_modelling_cleaned.ice_gas_day_daily` (or the hourly variant).
- **Day-over-day volatility is captured** (cold-snap step changes between consecutive gas_days).
- **Intraday HE9 vs HE10 evolution within a session is NOT captured** — the source has only `VWAP Close` per (trade_date, hub), no tick-level data. This is fine for DA modelling because the auction clears once per day on settled prices.

---

## How next-day gas trades

### The session

ICE OTC physical next-day gas (D1-IPG products) is a **morning cash market**. On any trading day T, the session opens around 7:00 AM CT and closes around 10:30 AM CT. During that ~3.5-hour window, traders price gas for delivery on the **next business day**, with the published index value being the **volume-weighted average price (VWAP) at session close**.

### What gets priced when

Per the ICE 2026 calendar:

| Trading day type | Strip the session prices | Days |
|---|---|---|
| Standard Mon–Thu | Next business day | 1 |
| Standard Friday | Sat + Sun + Mon weekend strip | 3 |
| Thu before Good Friday | Fri + Sat + Sun + Mon | 4 |
| Wed before Thanksgiving | Thu + Fri + Sat + Sun + Mon | 5 |
| Last trading Thu of month-ending-on-weekend | Fri + month-end Sat (and Sun if applicable) | 2–3 |
| Last trading Fri of month-ending-on-weekend | Starts in next month | 1–2 |

### Diagram — Mon Apr 27 timeline (PJM modelling perspective)

```
Sun Apr 26              Mon Apr 27                                       Tue Apr 28
─────────────  │  ──────────────────────────────────────────────────  │  ──────────
                ↑   ↑           ↑                  ↑          ↑                  ↑
                │   │           │                  │          │                  │
        midnight  HE7           HE10/11          HE12        HE13           HE10
        (Mon)   (07:00 CT     (~10:30 CT     (12:00 ET   (1:00 PM CT   (gas day Tue
                Mon)          Mon)           = 11:00 CT  = 2:00 PM ET   begins —
                              ICE session    Mon)         Mon — Timely  9 AM CT)
                ICE session    closes —      PJM DA       nomination
                opens          Tue gas       auction      deadline
                for Tue        VWAP Close    closes —     for Tue
                delivery       LOCKED        bidders use  delivery
                pricing                       Mon's VWAP
                                              Close

     [stale Fri close ───────][active Tue trading]──[locked]──[DA cleared]──[gas flows]
       (weekend strip price)

                              ↑ This is the number the DA model needs:
                                ice_gas_day_daily.tetco_m3_cash
                                WHERE gas_day = '2026-04-28'
```

The full window from "Mon ICE session close" to "Tue 9 AM CT gas flow" is ~22 hours. By the time the **PJM DA auction closes at 12:00 PM ET** (~1.5 hours after ICE close), Tue's gas price has been locked for over an hour. By the time gas physically flows at **9:00 AM CT Tue** (the NAESB gas day boundary), the DA market has already cleared and bidders have committed.

### Weekend / holiday strip diagram

```
Fri morning session prices a 3-day strip:

Fri Apr 24                Sat 25         Sun 26         Mon 27          Tue 28
──────────────  │  ──────────────  │  ──────────  │  ──────────  │  ──────────
HE7-HE11 CT                        ↑              ↑              ↑
ICE Fri session                    │              │              │
─────────────                      │              │              │
trades:                            └──── all 3 gas_days share Fri's VWAP Close ─────┘
  Sat delivery
  Sun delivery       gas flows     gas flows     gas flows     gas flows
  Mon delivery       Sat 9 AM CT   Sun 9 AM CT   Mon 9 AM CT   Tue 9 AM CT
                                                                ↑
                                                                (priced by Mon's
                                                                 own session)
```

In our `ice_gas_day_daily` mart, this looks like:

```
gas_day      trade_date   tetco_m3_cash
─────────────────────────────────────────
2026-04-25   2026-04-24   1.9318  ← Fri's session
2026-04-26   2026-04-24   1.9318  ← Fri's session (same VWAP)
2026-04-27   2026-04-24   1.9318  ← Fri's session (same VWAP)
2026-04-28   2026-04-27   2.1283  ← Mon's new session
```

---

## Authoritative documentation

| Concept | Source |
|---|---|
| Gas day = 9 AM CT to 9 AM CT next day | [NAESB WGQ Standard 1.3.1](https://www.naesb.org/pdf/idaywk3.pdf) |
| Pipeline nomination cycles (Timely 1 PM CT, Evening 6 PM CT, Intraday 1/2/3) | [FERC Order 809 — 152 FERC ¶61,212](https://www.ferc.gov/sites/default/files/2020-06/RM14-2-001.pdf) |
| ICE physical next-day gas calendar (which trading day prices which delivery days) | [ICE 2026 US Next Day Gas Trading Calendar](https://www.ice.com/publicdocs/support/phys_gas_calendar.pdf) |
| ICE OTC product family (D1-IPG codes, lot size, settlement) | [ICE NA Physical Gas Product Codes](https://www.ice.com/publicdocs/NA_Phys_Gas_Product_Codes.pdf), [ICE OTC Energy](https://www.ice.com/otc-energy) |
| PJM DA market timeline (bid/offer close 12:00 PM ET, results by 13:30 ET) | [PJM Manual 11](https://www.pjm.com/-/media/DotCom/documents/manuals/m11.pdf), [PCI Energy: PJM DA Timelines](https://www.pcienergysolutions.com/2024/12/12/understanding-pjm-day-ahead-market-timelines-how-they-compare-to-other-iso-rto-markets/) |

---

## Our dbt implementation: `ice_python_gas_day` package

Path: `backend/dbt/dbt_azure_postgresql/models/ice_python_gas_day/`

### Spine

`utils_v1_ice_gas_day_spine.sql` produces one row per **(gas_day, hour_ending)** with `trade_date` pointing at the actual ICE session that priced that delivery day.

The rule: each trading day T's session prices the strip `[T+1 ... next_trading_day(T)]`. Implemented via `LATERAL generate_series` over a non-trading-days seed.

| gas_day | trade_date (session) | Why |
|---|---|---|
| Tue | Mon | Standard weekday — Mon's session priced Tue gas |
| Sat / Sun / Mon | prior Fri | Friday's weekend strip |
| Fri Apr 3 (Good Friday) | Thu Apr 2 | Holiday extension — Thu absorbs the closure |
| Sat 31 (month ends Sat) | last trading Thu | Month-end split (Thu absorbs in-month weekend) |

### Source

`source_v1_ice_gas_day_hourly.sql` pivots `ice_python.next_day_gas_v1_2025_dec_16` by `trade_date` (single VWAP Close per hub) and LEFT JOINs the spine.

### Staging

`staging_v1_ice_gas_day_hourly.sql` forward-fills any nulls. Most gas_days have data; forward-fill is defensive against recent feed gaps.

`staging_v1_ice_gas_day_daily.sql` collapses to one row per gas_day (filters HE = 10).

### Marts (10 PJM-relevant hubs)

- `pjm_da_modelling_cleaned.ice_gas_day_hourly` — 13 cols: `gas_day, hour_ending, trade_date` + 10 hubs.
- `pjm_da_modelling_cleaned.ice_gas_day_daily` — 12 cols: `gas_day, trade_date` + 10 hubs. **One row per gas_day**.

### Invariants verified

- Every gas_day has exactly 24 rows in the hourly mart.
- Every gas_day has exactly 1 trade_date attribution.
- No future trade_dates.
- 2,310 distinct gas_days through current_date + 2 years.

---

## DA model usage

```sql
SELECT
    gas_day,
    trade_date,
    tetco_m3_cash,
    transco_z6_ny_cash,
    dominion_south_cash
    -- ... other PJM hubs
FROM pjm_da_modelling_cleaned.ice_gas_day_daily
WHERE gas_day = :forecast_target_date
```

Returns one row with the locked daily VWAP Close that PJM bidders used in their offers.

For an hourly join (e.g., to PJM hourly LMPs), use `ice_gas_day_hourly` and join on `(gas_day, hour_ending)`. All 24 hours of a given gas_day carry the same daily VWAP Close — which is the right thing because the DA auction sees a single locked price, not an intraday curve.

---

## Peak winter / summer — what we capture

### Day-over-day volatility ✓ captured

A polar vortex or summer heat wave producing a $4 → $14 jump in next-day gas across consecutive sessions shows up cleanly:

```
gas_day          tetco_m3_cash    Notes
──────────────────────────────────────────────────────────────────
Mon 2026-01-12       3.20         normal
Tue 2026-01-13       4.10         cold front forecast
Wed 2026-01-14       8.50         actual freeze hits
Thu 2026-01-15      14.20         extreme cold, supply constrained
Fri 2026-01-16      11.80         partial relief
```

Each row reflects the VWAP Close of its own pricing session. The DA model picks up the volatility via `WHERE gas_day = forecast_target`.

### Intraday volatility within a session ✗ NOT captured

The source (`ice_python.next_day_gas_v1_2025_dec_16`) has only `data_type = 'VWAP Close'` — one value per (trade_date, hub). Within Mon's morning session, prices for Tue gas may move sharply between HE7 CT and HE10 CT, but those ticks are not in our feed.

This is **fine for DA modelling** because:

- PJM's DA auction clears once at noon ET on settled prices, not intraday ticks.
- Bidders compose offers using the locked daily index, not the open-vs-close evolution.

It would matter for:

- Real-time / intraday market models
- Sensitivity analysis around session close
- Position management

**To extend**: `ice_python.intraday_quotes` may have tick-level data. If the next-day gas symbols are present there, we could bucket by `(trade_date, hour_ending)` and produce a true intraday curve in a future iteration.

---

## Why the gas-day-keyed spine replaces the trade-date-keyed one

We previously tried two earlier designs:

1. **Naive HE10 cutoff** (`utils_v1_ice_gas_day_dates_hourly`): keyed on calendar trade_date with `gas_day = trade_date` for HE1–9 and `gas_day = trade_date + 1` for HE10–24. This split each gas_day across two trade_dates, generated rows for non-trading Sat/Sun, and required a session-aware lookup in staging to produce the right HE9→HE10 step.

2. **Strip-aware with continuation hours**: emitted gas_day rows with HE1–9 attributed to `next_trading_day` (continuation) and HE10–24 to `session_trade_date`. Created confusing "future trade_date" labels and complex semantics.

The current `ice_python_gas_day` package is a clean rewrite keyed where it matters:

- Primary key is `(gas_day, hour_ending)`, matching how PJM modelling code joins (`gas_day = forecast_date`).
- `trade_date` is just the audit trail of which session priced this delivery day.
- No fake weekend trade_dates, no HE10 split, no session-aware staging.
- Per ICE / NAESB / PJM Manual 11 — one delivery period, one priced session, one VWAP Close.
