{{
  config(
    materialized='view'
  )
}}

----------------------------------------------------
-- ICE TICKER DATA — tick-level trade feed
-- One row per trade execution with symbol metadata + delivery window.
-- Trade-only columns: price, quantity, trade_direction.
-- Quote/book fields (bid, ask, conditions) are intentionally omitted.
-- trade_type is omitted because it is always 'TRADE' once filtered.
--
-- FILTER: price IS NOT NULL selects trade ticks only (drops pure
-- quote updates, which share the pivot grain but carry no price).
----------------------------------------------------

SELECT
    exec_time_local
    ,trade_date
    ,symbol
    ,description
    ,product_type
    ,contract_type
    ,strip
    ,start_date
    ,end_date
    ,price
    ,quantity
    ,trade_direction
FROM {{ ref('staging_v1_ticker_data') }}
WHERE price IS NOT NULL
