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
-- RT Hourly Wind Generation Mart
-- Grain: 1 row per datetime_beginning_utc x region
-- Priority: HOURLY (real published value) > INSTANTANEOUS (5-min averaged).
-- The hourly source is already filtered to the publication watermark, so
-- 0-padded rows past the watermark are absent and the 5-min averaged value
-- fills the gap automatically.
---------------------------

{% set lookback_filter %}
    {% if is_incremental() %}
    AND datetime_beginning_utc >= (SELECT MAX(datetime_beginning_utc) - INTERVAL '10 days' FROM {{ this }})
    {% endif %}
{% endset %}

WITH HOURLY AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,region
        ,'HOURLY' AS source_table
        ,wind_gen_mw
    FROM {{ ref('source_v1_pjm_wind_gen_by_area') }}
    WHERE 1 = 1
    {{ lookback_filter }}
),

-- Lookback applied on raw 5-min grain BEFORE the hourly average so the
-- window prunes source rows instead of aggregated rows.
INSTANTANEOUS_5_MIN AS (
    SELECT
        datetime_beginning_utc
        ,timezone
        ,datetime_beginning_local
        ,date
        ,hour_ending
        ,region
        ,wind_gen_mw
    FROM {{ ref('source_v1_pjm_instantaneous_wind_gen') }}
    WHERE 1 = 1
    {{ lookback_filter }}
),

INSTANTANEOUS AS (
    SELECT
        DATE_TRUNC('hour', datetime_beginning_utc) AS datetime_beginning_utc
        ,DATE_TRUNC('hour', datetime_beginning_utc) + INTERVAL '1 hour' AS datetime_ending_utc
        ,timezone
        ,DATE_TRUNC('hour', datetime_beginning_local) AS datetime_beginning_local
        ,DATE_TRUNC('hour', datetime_beginning_local) + INTERVAL '1 hour' AS datetime_ending_local
        ,date
        ,hour_ending
        ,region
        ,'INSTANTANEOUS' AS source_table
        ,AVG(wind_gen_mw) AS wind_gen_mw
    FROM INSTANTANEOUS_5_MIN
    GROUP BY 1, 2, 3, 4, 5, date, hour_ending, region
),

COMBINED AS (
    SELECT * FROM HOURLY
    UNION ALL
    SELECT * FROM INSTANTANEOUS
),

RANKED AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (
            PARTITION BY datetime_beginning_utc, region
            ORDER BY
                CASE source_table
                    WHEN 'HOURLY' THEN 0
                    WHEN 'INSTANTANEOUS' THEN 1
                    ELSE 999
                END
        ) AS priority_rank
    FROM COMBINED
),

FINAL AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,region
        ,source_table AS gen_source
        ,wind_gen_mw
    FROM RANKED
    WHERE priority_rank = 1
)

SELECT * FROM FINAL
