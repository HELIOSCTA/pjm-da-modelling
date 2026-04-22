{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Hourly Load (normalized)
-- Combines metered + prelim + instantaneous, preferring metered
-- Grain: 1 row per date × hour × region
---------------------------

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
    FROM {{ ref('source_v1_pjm_five_min_load') }}
    WHERE load_area IN ('RTO', 'WEST', 'MIDATL', 'SOUTH')
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
ORDER BY date DESC, hour_ending DESC, region
