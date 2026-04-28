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
-- RT Hourly Solar Generation Mart
-- Grain: 1 row per datetime_beginning_utc x region
-- solar_gen_mw is preserved nullable so consumers can distinguish missing
-- reported generation from reported zero generation.
---------------------------

{% set lookback_filter %}
    {% if is_incremental() %}
    AND datetime_beginning_utc >= (SELECT MAX(datetime_beginning_utc) - INTERVAL '10 days' FROM {{ this }})
    {% endif %}
{% endset %}

SELECT
    datetime_beginning_utc
    ,datetime_ending_utc
    ,timezone
    ,datetime_beginning_local
    ,datetime_ending_local
    ,date
    ,hour_ending
    ,region
    ,solar_gen_mw
FROM {{ ref('source_v1_pjm_solar_gen_by_area') }}
WHERE 1 = 1
{{ lookback_filter }}
