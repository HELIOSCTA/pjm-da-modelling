{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- ICE NEXT-DAY GAS SYMBOL MAPPING
-- Ordered: Louisiana > Southeast > East Texas > Northeast > Southwest > Rockies > Midwest
---------------------------

{% set columns = [
    {'symbol': 'XGF D1-IPG',  'col': 'hh_cash'},
    {'symbol': 'XVA D1-IPG',  'col': 'transco_st85_cash'},
    {'symbol': 'XLM D1-IPG',  'col': 'tgp_500l_cash'},
    {'symbol': 'YHV D1-IPG',  'col': 'fgt_z3_cash'},
    {'symbol': 'XLA D1-IPG',  'col': 'columbia_gulf_cash'},
    {'symbol': 'XTA D1-IPG',  'col': 'anr_se_cash'},
    {'symbol': 'YV7 D1-IPG',  'col': 'pine_prarie_cash'},
    {'symbol': 'XVM D1-IPG',  'col': 'tetco_wla_cash'},
    {'symbol': 'XT6 D1-IPG',  'col': 'waha_cash'},
    {'symbol': 'XYZ D1-IPG',  'col': 'houston_ship_channel_cash'},
    {'symbol': 'XIT D1-IPG',  'col': 'ngpl_txok_cash'},
    {'symbol': 'X7F D1-IPG',  'col': 'agt_cash'},
    {'symbol': 'XZR D1-IPG',  'col': 'tetco_m3_cash'},
    {'symbol': 'XIZ D1-IPG',  'col': 'columbia_tco_cash'},
    {'symbol': 'XWK D1-IPG',  'col': 'transco_z6_ny_cash'},
    {'symbol': 'XJL D1-IPG',  'col': 'dominion_south_cash'},
    {'symbol': 'YFF D1-IPG',  'col': 'transco_zone_5_south_cash'},
    {'symbol': 'Z2Y D1-IPG',  'col': 'transco_z5_north_cash'},
    {'symbol': 'YAG D1-IPG',  'col': 'tetco_m2_cash'},
    {'symbol': 'Z1Q D1-IPG',  'col': 'tenn_z4_marcellus_cash'},
    {'symbol': 'YQE D1-IPG',  'col': 'transco_leidy_cash'},
    {'symbol': 'YP8 D1-IPG',  'col': 'iroquois_z2_cash'},
    {'symbol': 'XKF D1-IPG',  'col': 'socal_cg_cash'},
    {'symbol': 'XGV D1-IPG',  'col': 'pge_cg_cash'},
    {'symbol': 'YKL D1-IPG',  'col': 'cig_cash'},
    {'symbol': 'XJR D1-IPG',  'col': 'ngpl_midcon_cash'},
    {'symbol': 'XJZ D1-IPG',  'col': 'michcon_cash'},
    {'symbol': 'XTG D1-IPG',  'col': 'nng_ventura_cash'},
    {'symbol': 'YHF D1-IPG',  'col': 'chicago_cg_cash'},
] %}

---------------------------
-- HOURLY GAS DAY DATE SPINE
---------------------------

WITH DATES AS (
    SELECT
        datetime,
        date,
        hour_ending,
        gas_day,
        trade_date
    FROM {{ ref('utils_v1_ice_gas_day_dates_hourly') }}
),

---------------------------
-- PIVOT ICE NEXT-DAY GAS BY SYMBOL
---------------------------

ICEXL AS (
    SELECT
        trade_date::DATE AS trade_date

        {% for c in columns %}
            ,AVG(CASE WHEN symbol = '{{ c.symbol }}' THEN value END) AS {{ c.col }}
        {% endfor %}

    FROM {{ source('ice_python_v1', 'next_day_gas_v1_2025_dec_16') }}
    GROUP BY trade_date
),

---------------------------
-- JOIN HOURLY DATES WITH ICE DATA
---------------------------

FINAL AS (
    SELECT
        dates.datetime,
        dates.date,
        dates.hour_ending,
        dates.gas_day,
        dates.trade_date

        {% for c in columns %}
            ,{{ c.col }}
        {% endfor %}

    FROM DATES dates
    LEFT JOIN ICEXL icexl ON dates.trade_date = icexl.trade_date
)

SELECT * FROM FINAL
ORDER BY datetime DESC
