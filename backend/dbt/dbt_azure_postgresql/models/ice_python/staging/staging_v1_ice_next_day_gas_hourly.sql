{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- FORWARD-FILL HOURLY NEXT-DAY GAS
---------------------------

{% set columns = [
    {'col': 'hh_cash',                    'grp': 'grp_hh'},
    {'col': 'transco_st85_cash',          'grp': 'grp_transco'},
    {'col': 'tgp_500l_cash',              'grp': 'grp_tgp_500l'},
    {'col': 'fgt_z3_cash',               'grp': 'grp_fgt_z3'},
    {'col': 'columbia_gulf_cash',         'grp': 'grp_columbia_gulf'},
    {'col': 'anr_se_cash',               'grp': 'grp_anr_se'},
    {'col': 'pine_prarie_cash',           'grp': 'grp_pine'},
    {'col': 'tetco_wla_cash',            'grp': 'grp_tetco_wla'},
    {'col': 'waha_cash',                  'grp': 'grp_waha'},
    {'col': 'houston_ship_channel_cash',  'grp': 'grp_houston_ship_channel'},
    {'col': 'ngpl_txok_cash',             'grp': 'grp_ngpl_txok'},
    {'col': 'agt_cash',                   'grp': 'grp_agt'},
    {'col': 'tetco_m3_cash',              'grp': 'grp_tetco'},
    {'col': 'columbia_tco_cash',          'grp': 'grp_columbia_tco'},
    {'col': 'transco_z6_ny_cash',         'grp': 'grp_transco_z6_ny'},
    {'col': 'dominion_south_cash',        'grp': 'grp_dominion_south'},
    {'col': 'transco_zone_5_south_cash',  'grp': 'grp_transco_zone'},
    {'col': 'transco_z5_north_cash',      'grp': 'grp_transco_z5_north'},
    {'col': 'tetco_m2_cash',              'grp': 'grp_tetco_m2'},
    {'col': 'tenn_z4_marcellus_cash',     'grp': 'grp_tenn_z4'},
    {'col': 'transco_leidy_cash',         'grp': 'grp_transco_leidy'},
    {'col': 'iroquois_z2_cash',           'grp': 'grp_iroquois'},
    {'col': 'socal_cg_cash',              'grp': 'grp_socal'},
    {'col': 'pge_cg_cash',               'grp': 'grp_pge'},
    {'col': 'cig_cash',                   'grp': 'grp_cig'},
    {'col': 'ngpl_midcon_cash',           'grp': 'grp_ngpl_midcon'},
    {'col': 'michcon_cash',               'grp': 'grp_michcon'},
    {'col': 'nng_ventura_cash',           'grp': 'grp_nng_ventura'},
    {'col': 'chicago_cg_cash',            'grp': 'grp_chicago_cg'},
] %}

---------------------------
-- SOURCE DATA
---------------------------

WITH HOURLY AS (
    SELECT
        datetime,
        date,
        hour_ending,
        gas_day,
        trade_date

        {% for c in columns %}
            ,{{ c.col }}
        {% endfor %}

    FROM {{ ref('source_v1_ice_next_day_gas_hourly') }}
),

---------------------------
-- NULL GROUP BOUNDARIES
---------------------------

GROUPED_DATA AS (
    SELECT
        datetime,
        date,
        hour_ending,
        gas_day,
        trade_date

        {% for c in columns %}
            ,{{ c.col }}
            ,SUM(CASE WHEN {{ c.col }} IS NOT NULL THEN 1 ELSE 0 END) OVER (ORDER BY datetime) AS {{ c.grp }}
        {% endfor %}

    FROM HOURLY
),

---------------------------
-- FORWARD FILL
---------------------------

FILLED_DATA AS (
    SELECT
        datetime,
        date,
        hour_ending,
        gas_day,
        trade_date

        {% for c in columns %}
            ,FIRST_VALUE({{ c.col }}) OVER (PARTITION BY {{ c.grp }} ORDER BY datetime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{ c.col }}
        {% endfor %}

    FROM GROUPED_DATA
),

---------------------------
-- FILTER TO VALID RANGE
---------------------------

FINAL AS (
    SELECT
        datetime,
        date,
        hour_ending,
        gas_day,
        trade_date

        {% for c in columns %}
            ,{{ c.col }}
        {% endfor %}

    FROM FILLED_DATA
    WHERE datetime <= (SELECT MAX(datetime) FROM {{ ref('source_v1_ice_next_day_gas_hourly') }} WHERE hh_cash IS NOT NULL)
)

SELECT * FROM FINAL
ORDER BY datetime DESC
