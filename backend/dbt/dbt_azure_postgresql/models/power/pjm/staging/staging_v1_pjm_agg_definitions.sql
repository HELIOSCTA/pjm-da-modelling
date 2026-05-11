{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- PJM Aggregate-PNode → Bus-PNode Definitions (normalized)
-- Grain: 1 row per (agg_pnode_id, bus_pnode_id, effective_date_ept)
--
-- Adds:
--   - is_active        : derived from terminate_date_ept >= '9999-01-01'
--                        (the SCD2 sentinel set by the upstream scraper)
--   - agg_pnode_type   : heuristic class derived from the name pattern.
--                        PJM does not expose a type field on this feed —
--                        we derive HUB / ZONE / RESID_AGG_FTR / EHV /
--                        INTERFACE / OTHER from the suffix tokens.
--                        ORDER MATTERS: more-specific patterns first
--                        (RESID_AGG_FTR before ZONE; HUB always wins).
---------------------------

WITH RAW AS (
    SELECT *
    FROM {{ ref('source_v1_pjm_agg_definitions') }}
),

TYPED AS (
    SELECT
        effective_date_ept
        ,terminate_date_ept
        ,(terminate_date_ept >= '9999-01-01'::TIMESTAMP)            AS is_active

        ,agg_pnode_id
        ,agg_pnode_name
        ,CASE
            WHEN agg_pnode_name ILIKE '%HUB%'           THEN 'HUB'
            WHEN agg_pnode_name ILIKE '%RESID_AGG_FTR%' THEN 'RESID_AGG_FTR'
            WHEN agg_pnode_name ILIKE '%ZONE%'          THEN 'ZONE'
            WHEN agg_pnode_name ILIKE '%EHV%'           THEN 'EHV'
            WHEN agg_pnode_name ILIKE '%INTERFACE%'     THEN 'INTERFACE'
            ELSE 'OTHER'
         END                                                        AS agg_pnode_type

        ,bus_pnode_id
        ,bus_pnode_name
        ,bus_pnode_factor

    FROM RAW
)

SELECT * FROM TYPED
