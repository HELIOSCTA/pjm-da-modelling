{{
  config(
    materialized='ephemeral'
  )
}}

----------------------------------------------------
-- ICE CONTRACT DATES — daily snapshot per symbol
-- Grain: 1 row per trade_date × symbol
-- Source for strip / start_date / end_date metadata.
----------------------------------------------------

SELECT
    trade_date
    ,symbol
    ,strip
    ,start_date
    ,end_date
FROM {{ source('ice_python_v1', 'contract_dates') }}
