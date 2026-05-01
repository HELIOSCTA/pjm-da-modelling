# PJM Data Miner Sources for Operator Effect Research

Base URL: `https://dataminer2.pjm.com/feed/{feed_name}/definition`

## Load Forecast vs Actual

| Feed | Description | Use |
|------|-------------|-----|
| `hrl_load_metered` | Hourly metered load by zone | Ground truth — actual load by zone by hour. Compare to forecast at zone level to isolate where the miss is (West vs Dom vs MidAtl) |
| `load_frcstd_7_day` | 7-day ahead hourly load forecast by zone | DA-relevant forecast vintage. Compare the forecast issued before DA clear vs what materialized |
| `hrl_da_demand_bids` | Hourly DA demand bids | What load was actually bid into DA. If cleared demand > actual load = the over-commitment gap driving DARTs |

## Congestion / Constraint Pricing

| Feed | Description | Use |
|------|-------------|-----|
| `da_hrl_lmps` | DA hourly LMPs by node/hub | DA congestion component by hour — the "hard wall" price |
| `rt_hrl_lmps` | RT hourly LMPs by node/hub | RT congestion component — what printed after operator intervention |
| `rt_fivemin_hrl_lmps` | 5-min RT LMPs | Granular congestion evolution within an hour — see operators managing flows in real-time (e.g., HE1 -$129 → HE7 -$24 in 5-min increments) |
| **`da_marginal_value`** | **DA binding constraint shadow prices** | **Most directly relevant.** Shows which constraints bound in DA and their shadow prices |
| **`rt_marginal_value`** | **RT binding constraint shadow prices** | RT counterpart — which constraints actually bound after operator actions. **Compare to DA to quantify the operator effect per constraint.** |

## Operator Actions (Direct Evidence)

| Feed | Description | Use |
|------|-------------|-----|
| `opr_post_cont_violations` | Post-contingency violations | Direct evidence of operators accepting post-contingency overloads (loosening constraints in RT) |
| `opr_reserve_actions` | Operator reserve actions | Emergency energy purchases, reserve deployments |
| `tlr_log` | TLR (Transmission Loading Relief) events | Direct evidence of operator-initiated redispatch to manage flows |
| `emergency_procedures` | Emergency procedure activations | When PJM went to emergency actions to manage the grid |

## Transmission Outages

| Feed | Description | Use |
|------|-------------|-----|
| `trans_outage_sum` | Transmission outage summary | Aggregate outage MW |
| `trans_outage_detail` | Detailed transmission outages | Facility-level — cross-reference with our 06c/d/e SQL queries |

## Priority Analysis

**Highest-value comparison:** `da_marginal_value` vs `rt_marginal_value` on the same constraints. If Bedington or Graceton constraint has shadow price $50/MW in DA but $15/MW in RT, that's direct quantification of the operator effect. Pair with `tlr_log` and `opr_post_cont_violations` on the same hours to show *how* operators managed it down.

**Second priority:** `hrl_da_demand_bids` vs `hrl_load_metered` — quantifies the DA demand over-commitment that feeds into the load forecast bias TODO.
