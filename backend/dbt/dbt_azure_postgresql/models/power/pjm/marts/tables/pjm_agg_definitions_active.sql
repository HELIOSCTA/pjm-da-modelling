{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['agg_pnode_name'], 'type': 'btree'},
      {'columns': ['bus_pnode_id'], 'type': 'btree'},
      {'columns': ['agg_pnode_type'], 'type': 'btree'}
    ]
  )
}}

---------------------------
-- PJM Aggregate-PNode → Bus-PNode Definitions (currently-active subset)
-- Grain: 1 row per (agg_pnode_id, bus_pnode_id, effective_date_ept)
--
-- This mart is the bridge from market geography (hubs, zones) to
-- constituent bus pnodes. Use it for:
--   - LMP queries scoped to a hub (join to da_hrl_lmps on bus_pnode_id)
--   - brief subagent zone tagging on outage rows
--   - hub composition snapshots for backtests (the upstream feed is
--     full-history; this mart filters to currently-effective only)
--
-- Validation invariant: SUM(bus_pnode_factor) per agg_pnode_id ≈ 1.0
--   (proportional weights; some hubs are unequal-weighted).
--
-- Indexes are scoped to the lookup patterns expected by the brief
-- subagents and the MCP view layer:
--   - agg_pnode_name : forward lookup ("buses in WESTERN HUB")
--   - bus_pnode_id   : reverse lookup ("which hubs is this bus in")
--   - agg_pnode_type : filter to HUB-only or ZONE-only views
---------------------------

WITH ACTIVE AS (
    SELECT *
    FROM {{ ref('staging_v1_pjm_agg_definitions') }}
    WHERE is_active
)

SELECT
    agg_pnode_id
    ,agg_pnode_name
    ,agg_pnode_type
    ,bus_pnode_id
    ,bus_pnode_name
    ,bus_pnode_factor
    ,effective_date_ept
    ,terminate_date_ept

FROM ACTIVE
ORDER BY agg_pnode_name, bus_pnode_factor DESC
