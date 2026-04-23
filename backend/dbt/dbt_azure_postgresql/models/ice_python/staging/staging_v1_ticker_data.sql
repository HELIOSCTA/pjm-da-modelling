{{
  config(
    materialized='ephemeral'
  )
}}

----------------------------------------------------
-- ICE TICKER DATA — joined with symbol registry + contract dates
-- Grain: 1 row per exec_time_local × symbol
-- Enriches each tick with description/product_type/contract_type and
-- strip/start_date/end_date (delivery window).
-- Symbol registry mirrors backend/scrapes/ice_python/symbols/pjm_short_term_symbols.py
----------------------------------------------------

{% set symbols = [
    {'symbol': 'PDP D0-IUS', 'description': 'PJM Balance of Day',     'product_type': 'power', 'contract_type': 'daily'},
    {'symbol': 'PDP D1-IUS', 'description': 'PJM RT Next Day',        'product_type': 'power', 'contract_type': 'daily'},
    {'symbol': 'PDA D1-IUS', 'description': 'PJM DA Next Day',        'product_type': 'power', 'contract_type': 'daily'},
    {'symbol': 'PJL D1-IUS', 'description': 'PJM Off-Peak Next Day',  'product_type': 'power', 'contract_type': 'daily'},
    {'symbol': 'PDP W0-IUS', 'description': 'PJM Balance of Week',    'product_type': 'power', 'contract_type': 'weekly'},
    {'symbol': 'PDP W1-IUS', 'description': 'PJM Week 1',             'product_type': 'power', 'contract_type': 'weekly'},
    {'symbol': 'PDP W2-IUS', 'description': 'PJM Week 2',             'product_type': 'power', 'contract_type': 'weekly'},
    {'symbol': 'PDP W3-IUS', 'description': 'PJM Week 3',             'product_type': 'power', 'contract_type': 'weekly'},
    {'symbol': 'PDP W4-IUS', 'description': 'PJM Week 4',             'product_type': 'power', 'contract_type': 'weekly'},
] %}

WITH SYMBOL_REGISTRY AS (
    {% for s in symbols %}
        SELECT
            '{{ s.symbol }}'::VARCHAR AS symbol
            ,'{{ s.description }}'::VARCHAR AS description
            ,'{{ s.product_type }}'::VARCHAR AS product_type
            ,'{{ s.contract_type }}'::VARCHAR AS contract_type
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

-- ICE publishes Bid and Ask on separate ticks, so after the pivot each
-- row usually carries only one side of the book (or neither). To give
-- every tick the best-known quote, we forward-fill bid and ask per
-- symbol ordered by exec_time_local using the "count + max" idiom:
--   bid_grp = running count of non-null bids seen so far → rows sharing
--   a bid_grp are the current row and all subsequent rows before the
--   next non-null bid; MAX(bid) over that group returns the most recent
--   non-null bid. Same pattern for ask. Fill carries across trade_dates
--   so the first tick of a new session inherits the previous close.
TICKS_RAW AS (
    SELECT * FROM {{ ref('source_v1_ticker_data') }}
),

TICKS_FLAGGED AS (
    SELECT
        *
        ,COUNT(bid) OVER (PARTITION BY symbol ORDER BY exec_time_local
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS bid_grp
        ,COUNT(ask) OVER (PARTITION BY symbol ORDER BY exec_time_local
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ask_grp
    FROM TICKS_RAW
),

TICKS AS (
    SELECT
        exec_time_local
        ,trade_date
        ,symbol
        ,price
        ,quantity
        ,trade_type
        ,conditions
        ,trade_direction
        ,MAX(bid) OVER (PARTITION BY symbol, bid_grp) AS bid
        ,MAX(ask) OVER (PARTITION BY symbol, ask_grp) AS ask
    FROM TICKS_FLAGGED
),

DATES AS (
    SELECT * FROM {{ ref('staging_v1_contract_dates') }}
),

JOINED AS (
    SELECT
        t.exec_time_local
        ,t.trade_date
        ,t.symbol
        ,r.description
        ,r.product_type
        ,r.contract_type
        ,d.strip
        ,d.start_date
        ,d.end_date
        ,t.price
        ,t.quantity
        ,t.trade_type
        ,t.conditions
        ,t.trade_direction
        ,t.bid
        ,t.ask
    FROM TICKS t
    LEFT JOIN SYMBOL_REGISTRY r ON t.symbol = r.symbol
    LEFT JOIN DATES d
        ON t.symbol = d.symbol
        AND t.trade_date = d.trade_date
)

SELECT * FROM JOINED
ORDER BY exec_time_local DESC, symbol
