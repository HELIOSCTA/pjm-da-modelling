{{
  config(
    materialized='view'
  )
}}

----------------------------------------------------
-- ICE TICKER DATA — end-of-day summary per product
-- Grain: 1 row per trade_date × symbol
--
-- Aggregates tick-level trades into:
--   OHLC (open / high / low / close)
--   volume, VWAP
--   trade counts — total, Lift (aggressive buy), Hit (aggressive sell), Leg (spread)
--   block-trade flagging (conditions ILIKE '%block%')
--
-- Hit/Lift convention (verified empirically via MCP against actual ICE data):
--   ICE's "Set By X" = "the X side of the book REMAINED as the new reference
--   AFTER the trade" — NOT "X side was the aggressor". Counterintuitive but
--   confirmed: SetByBid trades execute at the ask and tick price UP (buyer
--   lifted offer), SetByAsk trades execute at the bid and tick price DOWN
--   (seller hit bid).
--
--   Conditions ILIKE '%SetByBid%' → Lift (buyer lifted the offer, aggressive BUY)
--   Conditions ILIKE '%SetByAsk%' → Hit  (seller hit the bid, aggressive SELL)
--   Conditions ILIKE '%Leg%'      → spread leg (not directional — excluded from buy/sell)
-- ICE may wrap multiple flags in one comma-separated string, so ILIKE matching
-- is used throughout.
--
-- Carries symbol metadata + delivery window (strip / start_date / end_date)
-- through from staging_v1_ticker_data.
--
-- Note: `trade_date` is publisher-local (Mountain) calendar day — derived
-- from exec_time_local during the scrape. Convert to EPT at the call site
-- if you need to align with PJM market hours.
----------------------------------------------------

WITH ticks AS (
    SELECT *
    FROM {{ ref('staging_v1_ticker_data') }}
    WHERE price IS NOT NULL
      AND quantity IS NOT NULL
),

opens AS (
    SELECT DISTINCT ON (trade_date, symbol)
        trade_date
        ,symbol
        ,price AS open_price
    FROM ticks
    ORDER BY trade_date, symbol, exec_time_local ASC
),

closes AS (
    SELECT DISTINCT ON (trade_date, symbol)
        trade_date
        ,symbol
        ,price AS close_price
    FROM ticks
    ORDER BY trade_date, symbol, exec_time_local DESC
),

aggs AS (
    SELECT
        trade_date
        ,symbol
        ,MAX(description)    AS description
        ,MAX(product_type)   AS product_type
        ,MAX(contract_type)  AS contract_type
        ,MAX(strip)          AS strip
        ,MAX(start_date)     AS start_date
        ,MAX(end_date)       AS end_date

        ,MAX(price)          AS high
        ,MIN(price)          AS low

        ,SUM(quantity)                                       AS volume
        ,SUM(price * quantity) / NULLIF(SUM(quantity), 0)    AS vwap

        ,COUNT(*)                                              AS trade_count
        ,COUNT(*)      FILTER (WHERE trade_direction = 'Lift')  AS lift_count
        ,COUNT(*)      FILTER (WHERE trade_direction = 'Hit')   AS hit_count
        ,COUNT(*)      FILTER (WHERE trade_direction = 'Leg')   AS leg_count
        ,SUM(quantity) FILTER (WHERE trade_direction = 'Lift')  AS buy_volume
        ,SUM(quantity) FILTER (WHERE trade_direction = 'Hit')   AS sell_volume
        ,SUM(quantity) FILTER (WHERE trade_direction = 'Leg')   AS leg_volume

        ,COUNT(*)      FILTER (WHERE conditions ILIKE '%block%')        AS block_trade_count
        ,SUM(quantity) FILTER (WHERE conditions ILIKE '%block%')        AS block_volume
    FROM ticks
    GROUP BY trade_date, symbol
)

SELECT
    a.trade_date
    ,a.symbol
    ,a.description
    ,a.product_type
    ,a.contract_type
    ,a.strip
    ,a.start_date
    ,a.end_date

    -- ────── OHLC ──────
    ,o.open_price  AS open
    ,a.high
    ,a.low
    ,c.close_price AS close

    -- ────── Volume & VWAP ──────
    ,a.volume
    ,a.vwap

    -- ────── Trade flow ──────
    ,a.trade_count
    ,a.lift_count
    ,a.hit_count
    ,a.leg_count
    ,a.buy_volume
    ,a.sell_volume
    ,a.leg_volume

    -- ────── Block trades ──────
    ,a.block_trade_count
    ,a.block_volume

FROM aggs a
LEFT JOIN opens  o ON a.trade_date = o.trade_date AND a.symbol = o.symbol
LEFT JOIN closes c ON a.trade_date = c.trade_date AND a.symbol = c.symbol
ORDER BY a.trade_date DESC, a.symbol
