{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ICE NEXT-DAY GAS — gas_day-keyed source (10 PJM-relevant hubs)
-- Pivots the raw ICE feed by trade_date into one daily AVG per hub,
-- forward-fills sparse trade_dates at trade-date grain (small dataset
-- of ~1.5k weekday sessions), then joins the gas-day spine so every
-- (gas_day, hour_ending) row carries the price from the session that
-- priced it. Output is clipped to gas_days whose trade_date is on or
-- before the latest trade_date with real ICE data.
---------------------------

{% set columns = [
    {'symbol': 'XZR D1-IPG',  'col': 'tetco_m3_cash'},
    {'symbol': 'XIZ D1-IPG',  'col': 'columbia_tco_cash'},
    {'symbol': 'XWK D1-IPG',  'col': 'transco_z6_ny_cash'},
    {'symbol': 'XJL D1-IPG',  'col': 'dominion_south_cash'},
    {'symbol': 'XTG D1-IPG',  'col': 'nng_ventura_cash'},
    {'symbol': 'YAG D1-IPG',  'col': 'tetco_m2_cash'},
    {'symbol': 'Z2Y D1-IPG',  'col': 'transco_z5_north_cash'},
    {'symbol': 'Z1Q D1-IPG',  'col': 'tenn_z4_marcellus_cash'},
    {'symbol': 'YQE D1-IPG',  'col': 'transco_leidy_cash'},
    {'symbol': 'YHF D1-IPG',  'col': 'chicago_cg_cash'},
] %}

WITH SPINE AS (
    SELECT
        datetime_beginning_utc,
        datetime_ending_utc,
        timezone,
        datetime_beginning_local,
        datetime_ending_local,
        gas_day,
        hour_ending,
        trade_date
    FROM {{ ref('utils_v1_ice_gas_day_spine') }}
),

TRADE_DATES AS (
    SELECT DISTINCT trade_date FROM SPINE
),

ICEXL AS (
    SELECT
        trade_date::DATE AS trade_date

        {% for c in columns %}
            ,AVG(CASE WHEN symbol = '{{ c.symbol }}' THEN value END) AS {{ c.col }}
        {% endfor %}

    FROM {{ source('ice_python_v1', 'next_day_gas_v1_2025_dec_16') }}
    GROUP BY trade_date
),

ALIGNED AS (
    SELECT
        td.trade_date

        {% for c in columns %}
            ,icexl.{{ c.col }}
        {% endfor %}

    FROM TRADE_DATES td
    LEFT JOIN ICEXL icexl ON td.trade_date = icexl.trade_date
),

GROUPED AS (
    SELECT
        trade_date

        {% for c in columns %}
            ,{{ c.col }}
            ,SUM(CASE WHEN {{ c.col }} IS NOT NULL THEN 1 ELSE 0 END) OVER (ORDER BY trade_date) AS grp_{{ c.col }}
        {% endfor %}

    FROM ALIGNED
),

FILLED AS (
    SELECT
        trade_date

        {% for c in columns %}
            ,FIRST_VALUE({{ c.col }}) OVER (PARTITION BY grp_{{ c.col }} ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{ c.col }}
        {% endfor %}

    FROM GROUPED
)

SELECT
    s.datetime_beginning_utc,
    s.datetime_ending_utc,
    s.timezone,
    s.datetime_beginning_local,
    s.datetime_ending_local,
    s.gas_day,
    s.hour_ending,
    s.trade_date

    {% for c in columns %}
        ,f.{{ c.col }}
    {% endfor %}

FROM SPINE s
LEFT JOIN FILLED f ON s.trade_date = f.trade_date
WHERE s.trade_date <= (SELECT MAX(trade_date) FROM ICEXL)
