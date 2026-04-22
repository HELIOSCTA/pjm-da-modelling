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
-- RT Hourly Load (normalized)
-- Grain: 1 row per date × hour × region
-- Mirrors the logic in staging_v1_pjm_load_rt; inlined here so the
-- is_incremental() lookback filter pushes into the metered/prelim/instantaneous
-- source scans BEFORE the priority_rank window function.
-- Priority: metered > prelim > instantaneous (5-min averaged to hourly).
---------------------------

{% set lookback_filter %}
    {% if is_incremental() %}
    AND datetime_beginning_utc >= (SELECT MAX(datetime_beginning_utc) - INTERVAL '10 days' FROM {{ this }})
    {% endif %}
{% endset %}

WITH METERED AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,load_area
        ,'METERED' AS source_table
        ,load_mw
    FROM {{ ref('source_v1_pjm_hrl_load_metered') }}
    WHERE load_area IN ('RTO', 'WEST', 'MIDATL', 'SOUTH')
    {{ lookback_filter }}
),

PRELIM AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,load_area
        ,'PRELIM' AS source_table
        ,load_mw
    FROM {{ ref('source_v1_pjm_hrl_load_prelim') }}
    WHERE load_area IN ('RTO', 'WEST', 'MIDATL', 'SOUTH')
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
        ,load_area
        ,load_mw
    FROM {{ ref('source_v1_pjm_five_min_load') }}
    WHERE load_area IN ('RTO', 'WEST', 'MIDATL', 'SOUTH')
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
        ,load_area
        ,'INSTANTANEOUS' AS source_table
        ,AVG(load_mw) AS load_mw
    FROM INSTANTANEOUS_5_MIN
    GROUP BY 1, 2, 3, 4, 5, date, hour_ending, load_area
),

COMBINED AS (
    SELECT * FROM METERED
    UNION ALL
    SELECT * FROM PRELIM
    UNION ALL
    SELECT * FROM INSTANTANEOUS
),

RANKED AS (
    SELECT
        *
        ,ROW_NUMBER() OVER (
            PARTITION BY datetime_beginning_utc, load_area
            ORDER BY
                CASE source_table
                    WHEN 'METERED' THEN 0
                    WHEN 'PRELIM' THEN 1
                    WHEN 'INSTANTANEOUS' THEN 2
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
        ,load_area AS region
        ,source_table AS rt_source
        ,load_mw AS rt_load_mw
    FROM RANKED
    WHERE priority_rank = 1
)

SELECT * FROM FINAL
