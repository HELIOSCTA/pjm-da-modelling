{{
  config(
    materialized='ephemeral'
  )
}}

----------------------------------------------------
-- ICE TICKER DATA — long → wide pivot
-- Grain: 1 row per exec_time_local × symbol
-- Pivots ICE fields into typed columns.
--   Numeric fields (Price, Size, Bid, Ask) cast to DOUBLE PRECISION.
--   String fields (Type, Conditions) stay as text.
-- Our column names use domain semantics:
--   Size → quantity, Type → trade_type.
-- exec_time_local is publisher-local (Mountain) — ICE XL returns trade
-- timestamps in its own timezone, not UTC.
--
-- trade_direction is a derived label from Conditions:
--   SetByBid → 'Lift' (aggressive BUY — buyer took the offer)
--   SetByAsk → 'Hit'  (aggressive SELL — seller took the bid)
--   Leg      → 'Leg'  (spread leg — not directional)
--   else     → NULL
-- See marts/views/ice_python_ticker_data_eod.sql for the convention note.
----------------------------------------------------

WITH PIVOTED AS (
    SELECT
        exec_time_local
        ,trade_date
        ,symbol
        ,(MAX(value) FILTER (WHERE field = 'Price'))::DOUBLE PRECISION AS price
        ,(MAX(value) FILTER (WHERE field = 'Size'))::DOUBLE PRECISION  AS quantity
        ,MAX(value) FILTER (WHERE field = 'Type')                      AS trade_type
        ,MAX(value) FILTER (WHERE field = 'Conditions')                AS conditions
        ,(MAX(value) FILTER (WHERE field = 'Bid'))::DOUBLE PRECISION   AS bid
        ,(MAX(value) FILTER (WHERE field = 'Ask'))::DOUBLE PRECISION   AS ask
    FROM {{ source('ice_python_v1', 'ticker_data') }}
    GROUP BY exec_time_local, trade_date, symbol
)

SELECT
    exec_time_local
    ,trade_date
    ,symbol
    ,price
    ,quantity
    ,trade_type
    ,conditions
    ,CASE
        WHEN conditions ILIKE '%SetByBid%' THEN 'Lift'
        WHEN conditions ILIKE '%SetByAsk%' THEN 'Hit'
        WHEN conditions ILIKE '%Leg%'      THEN 'Leg'
        ELSE NULL
     END AS trade_direction
    ,bid
    ,ask
FROM PIVOTED
