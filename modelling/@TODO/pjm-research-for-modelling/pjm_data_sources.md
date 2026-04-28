# PJM Data Sources for the Forward-Only KNN

## Scope

Inventory of PJM Data Miner 2 feeds (and adjacent vendors) consumed by, or relevant to, the forward-only KNN model. Each feed is annotated with: what it contributes to matching vs. labels vs. diagnostics, whether it's forward-looking or backward-looking, vintage availability, and the modeling features it unlocks. New entries should be added here when scraped.

This document tracks *coverage* — what data we have, what we just added, and what remains a gap. The companion docs explain the *use*:
- `historical_forecasts.md` — pool/query asymmetry and vintage-archive plan
- `hourly_vs_daily_features.md` — why the matching distance compresses hourly to daily
- `backward_vs_forward_looking.md` — feature-side anchoring failure mode in adjacent models
- `pjm-like-day-research.md` — feature priority tiers from the literature

## Currently scraped (pre-existing)

Inventory of `backend/scrapes/power/pjm/*.py` excluding additions in this cycle. Grouped by what they contribute to the model.

### Energy market labels and prices
| Scrape | Feed | Role | Notes |
|---|---|---|---|
| `da_hrl_lmps.py` | `da_hrl_lmps` | DA LMP labels (`lmp_h1..lmp_h24`) | First available 2000-06-01. Pool labels for the analog ensemble. |
| `rt_settlements_verified_hourly_lmps.py` | rt verified | RT LMP for DA-RT spread features | |
| `rt_unverified_hourly_lmps.py` | rt unverified | RT LMP, faster cadence | |
| `unverified_five_min_lmps_v1_2026_mar_26.py` | rt 5-min unverified | RT LMP at 5-min, RT-model only | |

### Load
| Scrape | Feed | Role | Notes |
|---|---|---|---|
| `seven_day_load_forecast_v1_2025_08_13.py` | `load_frcstd_7_day` | Forward query feature | Rolling current snapshot; vintages overwritten. |
| `hourly_load_metered.py` | hrl metered | Realized pool feature | |
| `hourly_load_prelim.py` | hrl preliminary | Faster realized load (preliminary) | |
| `five_min_instantaneous_load_v1_2025_OCT_15.py` | `inst_load` | RT 5-min realized load | |

### Outages
| Scrape | Feed | Role | Notes |
|---|---|---|---|
| `seven_day_outage_forecast.py` | outages forecast | Forward query feature | Already vintage-stamped via `forecast_execution_date`. |
| `long_term_outages.py` | long-term outages | Multi-month outlook | |
| `transmission_outages.py` | transmission outages | Transmission-side outage context | |

### Reserves and ancillary services
| Scrape | Feed | Role | Notes |
|---|---|---|---|
| `dispatched_reserves_v1_2025_08_13.py` | dispatched reserves DA | DA dispatch detail | |
| `real_time_dispatched_reserves_v1_2025_08_13.py` | dispatched reserves RT | RT dispatch detail | |
| `operational_reserves_v1_2025_08_13.py` | operational reserves | Reserve operation metrics | |
| `ancillary_services_v1_2026_mar_26.py` | AS | Ancillary services | |

### Demand bids and tie flows
| Scrape | Feed | Role | Notes |
|---|---|---|---|
| `hrl_dmd_bids.py` | hrl demand bids | Demand stack composition | |
| `five_min_tie_flows.py` | tie flows 5-min | Realized inter-region flow | |

## Added during this research cycle

Five scrapes written to support the AnEn vintage plan and supply-side modeling:

| Scrape | Feed | First Available | Role | Vintage? |
|---|---|---|---|---|
| `historical_load_forecasts.py` | `load_frcstd_hist` | 2011-01-01 | Vintage-stamped historical load forecasts. **Closes the load piece of the AnEn vintage gap immediately** — no need to wait for forward snapshotting. PK includes `evaluated_at_*` so multiple vintages per delivery hour are preserved. ~5-6 vintages/hour, irregular spacing 120-360 min. | Yes |
| `solar_generation_by_area.py` | `solar_gen` | 2019-04-02 | **Realized** solar generation per area (MIDATL, OTHER, RFC, RTO, SOUTH, WEST). Per-area granularity beyond `pjm_fuel_mix_hourly`. Use as observation side of AnEn pair, not as forecast. | No |
| `wind_generation_by_area.py` | `wind_gen` | 2011-01-01 | **Realized** wind generation per area. Same as solar but ~10 years longer history than fuel_mix. | No |
| `day_gen_capacity.py` | `day_gen_capacity` | 2012-01-01 | **Backward-only** supply-side capacity: `eco_max`, `emerg_max`, `total_committed`. Replaces the static 185 GW constant in `reserve_margin_pct`. `total_committed` is symmetric across pool/query (RPM-structural, flat intra-day); `eco_max` requires forward proxy via roll-forward. | No (backward only) |
| `reserve_market_results.py` | `reserve_market_results` | 2013-06-14 | **Backward-only** clearing-side supply tightness: `mcp` for SR / PR / 30MIN / REG, by locale (PJM_RTO, MAD). **Highest-impact regime feature** — reserve MCP spikes precede or coincide with energy LMP spikes. | No (backward only) |

## Confirmed available, pending implementation

These endpoints returned valid CSV from direct API probe but no scrape has been written yet.

| Endpoint | Schema highlights | Why | Priority |
|---|---|---|---|
| `reg_market_results` | `requirement`, `regd_ssmw`, `rega_ssmw`, `regd_procure`, `rega_procure`, `total_mw`, `deficiency`, `rto_perfscore`, `rega_mileage`, `regd_mileage` | Regulation market — `deficiency > 0` rows mark hours where the system couldn't fully procure regulation, a strong stress signal. Lower direct impact on DA energy LMP than reserves. | Medium |
| `area_control_error` | `area`, `ace_mw` | System ACE — frequency control deviations. RT signal, more relevant for RT models than DA. Daily aggregate `ace_abs_daily_avg` is a system-wide regime indicator. | Low |
| `inst_load` | `area`, `instantaneous_load` | Likely duplicates the existing `five_min_instantaneous_load_v1_2025_OCT_15.py`. **Verify before scraping** to avoid duplicate ingestion. | Verify |

## Suspected available, endpoint names unknown

Probed exhaustive name variants without hits, but PJM publishes these data products. Likely buried under non-obvious internal names. Confirming requires either screen-scraping the JS-rendered Data Miner 2 list page, or domain knowledge of the exact internal name. **High value once named.**

| Concept | Why it matters for the model |
|---|---|
| **Marginal fuel postings** | Tells you what fuel set the marginal price each hour. Direct enabler of proper spark/dark spread features. Currently we have to *infer* marginal fuel from the gas price + fuel mix — error-prone for shoulder hours where coal is on margin. |
| **DA cleared INC / DEC / UTC virtuals** | INCs and DECs at PJM hubs (Western Hub especially) are major DA-vs-RT spread drivers. Cleared volumes indicate market depth and convergence behavior. Without this, the model can't see virtual-trading regime shifts. |
| **Transmission constraints / shadow prices** | Direct evidence of which constraints were binding and at what shadow price. Drives the congestion component of LMP at Western Hub. The current model has no way to match on "congestion regime." |
| **Zonal-level LMPs and load** | Currently hub-level only. Zonal granularity lets you detect basis risk between Western Hub and surrounding zones. Useful for matching on "hub-vs-zone basis" regime. |
| **Marginal emissions** | Emissions intensity of the marginal unit. Useful for emissions-aware features and as a secondary regime indicator (low-emissions hours = renewables on margin = different price dynamics). |
| **Demand response cleared MW** | DR cleared MW is a load-side scarcity response. Tight days have more DR called. Indirectly captured today via reserve MCP, but DR cleared volumes would be a cleaner direct signal. |
| **Net interchange / scheduled imports-exports forecast** | Imports add supply (suppress LMP); exports remove it. Forward forecast of net interchange would be a query feature; realized would be a pool feature. Currently we only have `five_min_tie_flows` (realized 5-min). |

**Action**: ask the user to share the visible feed list from `dataminer2.pjm.com/list` (paste, screenshot, or HTML dump after JS render). Then re-probe each candidate. This is a one-time unlock — once we have the names, all of the above can be scraped following the existing pattern.

## Adjacent vendor sources (already in cache)

Not PJM Data Miner but already populated in `modelling/data/cache/`. Documented here so the inventory is complete.

| Cache file | Source | Role |
|---|---|---|
| `ice_python_next_day_gas_hourly.parquet` | ICE | Day-ahead natural gas prices at four hubs (M3, TCO, TZ6, DOM SOUTH). Direct query feature for `gas_level` group. |
| `meteologica_pjm_*_forecast_hourly_da_cutoff.parquet` | Meteologica | Alternate D-1 forecast for load / solar / wind / net-load. **2 days of vintage history only** today — needs daily snapshotting per `historical_forecasts.md` to become a pool feature. Useful today via the dual-forecaster overlays (disagreement flag, ensemble query). |
| `wsi_pjm_hourly_observed_temp.parquet`, `wsi_pjm_hourly_forecast_temp_latest.parquet` | WSI | Weather observation and forecast. |

## Modeling features each new source unlocks

Concrete `FEATURE_GROUPS` additions made possible by the scrapes added in this cycle plus the pending endpoints:

```python
# from reserve_market_results
"reserve_pricing": [
    "sr_mcp_daily_avg",           # synchronized reserve MCP, mean
    "sr_mcp_onpeak_avg",          # SR MCP, on-peak only
    "sr_mcp_daily_peak",          # SR MCP, max hour
    "pr_mcp_daily_avg",           # primary reserve MCP, mean
    "reg_ccp_daily_avg",          # regulation capability clearing price
    "as_req_daily_avg",           # AS requirement, mean MW
],

# from day_gen_capacity
"capacity_level": [
    "total_committed_daily_avg",  # symmetric pool/query (RPM-structural, near-flat day-to-day)
    "eco_max_daily_avg",          # pool-only; query proxied via yesterday's roll-forward
    "supply_slack_avg",           # eco_max - tgt_outage - load_avg
],

# from solar_gen / wind_gen (per-area enrichment)
"renewable_regional": [
    "solar_midatl_daily_avg",
    "solar_west_daily_avg",
    "wind_west_daily_avg",
    # ... etc per area
],

# from load_frcstd_hist (vintage-matched, replaces realized in pool)
# No new feature names; replaces values in existing load_level group
# under Option B in historical_forecasts.md (parallel _forecast/_realized columns)
```

The reserve-pricing group is the highest-leverage addition — it's the supply-side regime signal the model has been missing.

## Recommended ingestion order

1. **Run `reserve_market_results.py` backfill** — 2013-06-14 to today, ~190-200 rows/day, 180-day chunks. Single highest-impact addition.
2. **Run `day_gen_capacity.py` backfill** — 2012-01-01 to today, ~24 rows/day, 180-day chunks. Capacity headroom signal.
3. **Run `historical_load_forecasts.py` backfill** — 2011-01-01 to today, 14-day chunks. Vintage-archived load forecasts unlock Option B from `historical_forecasts.md` for the load group immediately.
4. **Run `solar_generation_by_area.py` and `wind_generation_by_area.py` backfills** — 90-day chunks. Per-area realized renewables enrich the pool side.
5. **Resolve the unknown-name gap list** before continuing. Confirm the feed catalog and probe candidates for marginal fuel, virtuals, constraints, zonal LMPs.
6. **Begin daily snapshotting per `historical_forecasts.md`** for the rolling forecast feeds (PJM and Meteologica). Independent of the above, but on the critical path for the AnEn vintage plan beyond load.

## Cross-references

- `historical_forecasts.md` — explains why vintage matters and the snapshotting plan for forecast feeds that don't have a `*_hist` archive.
- `hourly_vs_daily_features.md` — explains why all the per-area / per-locale data above is collapsed to daily aggregates for the matching distance.
- `backward_vs_forward_looking.md` — explains why backward-only feeds (capacity, reserve MCP) are still valuable as pool-side regime features even though they re-introduce a mild asymmetry.
- `pjm-like-day-research.md` §4 — the literature feature-priority tiers, which this inventory is now closer to satisfying (Tier 1 fully covered; Tier 3 reserve-margin signal materially improved by `reserve_market_results` + `day_gen_capacity`).
