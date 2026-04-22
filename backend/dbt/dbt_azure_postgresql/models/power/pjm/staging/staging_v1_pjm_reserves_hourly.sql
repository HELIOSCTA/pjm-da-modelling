{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Unified Reserves (from dispatched_reserves)
-- Grain: 1 row per date x hour x area x reserve_type
-- Richest reserve feed: has clearing price, shortage indicator,
-- requirement vs. quantity
---------------------------

SELECT
    datetime_beginning_utc
    ,datetime_ending_utc
    ,timezone
    ,datetime_beginning_local
    ,datetime_ending_local
    ,date
    ,hour_ending
    ,area
    ,reserve_type
    ,reserve_quantity_mw
    ,reserve_requirement_mw
    ,reliability_requirement_mw
    ,extended_requirement_mw
    ,mw_adjustment
    ,market_clearing_price
    ,shortage_indicator

    -- Derived: reserve margin (quantity above requirement)
    ,(reserve_quantity_mw - reserve_requirement_mw) AS reserve_margin_mw

FROM {{ ref('source_v1_pjm_dispatched_reserves') }}
ORDER BY datetime_ending_local DESC, area, reserve_type
