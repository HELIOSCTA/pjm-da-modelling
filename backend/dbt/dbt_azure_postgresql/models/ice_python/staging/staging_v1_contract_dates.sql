{{
  config(
    materialized='ephemeral'
  )
}}

----------------------------------------------------
-- ICE CONTRACT DATES — staged
-- Grain: 1 row per trade_date × symbol
----------------------------------------------------

SELECT * FROM {{ ref('source_v1_contract_dates') }}
