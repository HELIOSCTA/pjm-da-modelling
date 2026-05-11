{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Aggregate-PNode → Bus-PNode Definitions (typed pass-through)
-- Grain: 1 row per (agg_pnode_id, bus_pnode_id, effective_date_ept)
-- Source: PJM Data Miner 2 agg_definitions feed
-- terminate_date_ept = '9999-12-31' is the SCD2 sentinel for active rows.
---------------------------

WITH RAW AS (
    SELECT
        effective_date_ept::TIMESTAMP                               AS effective_date_ept
        ,terminate_date_ept::TIMESTAMP                              AS terminate_date_ept
        ,agg_pnode_id::BIGINT                                       AS agg_pnode_id
        ,agg_pnode_name
        ,bus_pnode_id::BIGINT                                       AS bus_pnode_id
        ,bus_pnode_name
        ,bus_pnode_factor::DOUBLE PRECISION                         AS bus_pnode_factor

    FROM {{ source('pjm_v1', 'agg_definitions') }}
)

SELECT * FROM RAW
