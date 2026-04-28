{{
  config(
    materialized='incremental',
    unique_key=['datetime_beginning_utc', 'region'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['datetime_beginning_utc', 'region'], 'type': 'btree'},
      {'columns': ['datetime_beginning_utc'], 'type': 'btree'}
    ]
  )
}}

---------------------------
-- RT Hourly Net Load Mart
-- Grain: 1 row per datetime_beginning_utc x region
-- Definition: net_load_mw = rt_load_mw - solar_gen_mw - wind_gen_mw
-- Driven by pjm_load_rt_hourly. Hours without load are dropped.
-- net_load_mw is NULL whenever solar_gen_mw or wind_gen_mw is missing for the
-- (hour, region) — by design, so consumers never see a partial-coverage figure.
---------------------------

{% set lookback_filter %}
    {% if is_incremental() %}
    AND datetime_beginning_utc >= (SELECT MAX(datetime_beginning_utc) - INTERVAL '10 days' FROM {{ this }})
    {% endif %}
{% endset %}

WITH LOAD AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,region
        ,rt_source
        ,rt_load_mw
    FROM {{ ref('pjm_load_rt_hourly') }}
    WHERE rt_load_mw IS NOT NULL
    {{ lookback_filter }}
),

SOLAR AS (
    SELECT
        datetime_beginning_utc
        ,region
        ,solar_gen_mw
    FROM {{ ref('pjm_solar_gen_rt_hourly') }}
    WHERE 1 = 1
    {{ lookback_filter }}
),

WIND AS (
    SELECT
        datetime_beginning_utc
        ,region
        ,wind_gen_mw
    FROM {{ ref('pjm_wind_gen_rt_hourly') }}
    WHERE 1 = 1
    {{ lookback_filter }}
),

FINAL AS (
    SELECT
        L.datetime_beginning_utc
        ,L.datetime_ending_utc
        ,L.timezone
        ,L.datetime_beginning_local
        ,L.datetime_ending_local
        ,L.date
        ,L.hour_ending
        ,L.region
        ,L.rt_source
        ,L.rt_load_mw
        ,S.solar_gen_mw
        ,W.wind_gen_mw
        ,(
            L.rt_load_mw
            - S.solar_gen_mw
            - W.wind_gen_mw
        ) AS net_load_mw
    FROM LOAD L
    LEFT JOIN SOLAR S
        ON L.datetime_beginning_utc = S.datetime_beginning_utc
        AND L.region = S.region
    LEFT JOIN WIND W
        ON L.datetime_beginning_utc = W.datetime_beginning_utc
        AND L.region = W.region
)

SELECT * FROM FINAL
