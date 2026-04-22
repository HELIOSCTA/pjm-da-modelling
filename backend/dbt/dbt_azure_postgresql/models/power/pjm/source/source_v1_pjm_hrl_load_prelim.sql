{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- RT Prelim Hourly Load (normalized)
-- Grain: 1 row per date × hour × mkt_region × load_area
---------------------------

WITH MKT_REGION_LOOKUP AS (
    SELECT nerc_region, mkt_region, load_area
    FROM (VALUES
        ('MIDATL', 'MIDATL', 'MIDATL')
        ,('SERC', 'SOUTH', 'DOM')
        ,('RFC',  'WEST', 'AP')
        ,('RFC',  'WEST', 'DAY')
        ,('RFC',  'WEST', 'DEOK')
        ,('RFC',  'WEST', 'DUQ')
        ,('SERC', 'WEST', 'EKPC')
        ,('RFC',  'WEST', 'AEP')
        ,('RFC',  'WEST', 'ATSI')
        ,('RFC',  'WEST', 'NI')
    ) AS lookup_data(nerc_region, mkt_region, load_area)
),

PRELIM AS (
    SELECT
        prelim.datetime_beginning_utc
        ,prelim.datetime_beginning_utc + INTERVAL '1 hour' AS datetime_ending_utc
        ,'US/Eastern' AS timezone
        ,prelim.datetime_beginning_ept AS datetime_beginning_local
        ,prelim.datetime_beginning_ept + INTERVAL '1 hour' AS datetime_ending_local
        ,prelim.datetime_beginning_ept::DATE AS date
        ,(EXTRACT(HOUR FROM prelim.datetime_beginning_ept) + 1)::INT AS hour_ending

        ,lookup.mkt_region
        ,prelim.load_area

        ,prelim_load_avg_hourly AS load_mw

    FROM {{ source('pjm_v1', 'hourly_load_prelim') }} prelim
    LEFT JOIN MKT_REGION_LOOKUP lookup ON prelim.load_area = lookup.load_area
    WHERE
        prelim.datetime_beginning_ept::DATE >= current_date - 14
),

--------------------------------
-- Regional aggregation
--------------------------------

RTO AS (
    SELECT
        MAX(datetime_beginning_utc) AS datetime_beginning_utc
        ,MAX(datetime_ending_utc) AS datetime_ending_utc
        ,MAX(timezone) AS timezone
        ,MAX(datetime_beginning_local) AS datetime_beginning_local
        ,MAX(datetime_ending_local) AS datetime_ending_local
        ,date, hour_ending
        ,'RTO' AS mkt_region
        ,'RTO' AS load_area
        ,SUM(load_mw) AS load_mw
    FROM PRELIM
    WHERE mkt_region IN ('MIDATL', 'WEST', 'SOUTH')
    GROUP BY date, hour_ending
),

WEST AS (
    SELECT
        MAX(datetime_beginning_utc) AS datetime_beginning_utc
        ,MAX(datetime_ending_utc) AS datetime_ending_utc
        ,MAX(timezone) AS timezone
        ,MAX(datetime_beginning_local) AS datetime_beginning_local
        ,MAX(datetime_ending_local) AS datetime_ending_local
        ,date, hour_ending, mkt_region
        ,'WEST' AS load_area
        ,SUM(load_mw) AS load_mw
    FROM PRELIM
    WHERE mkt_region = 'WEST'
    GROUP BY date, hour_ending, mkt_region
),

SOUTH AS (
    SELECT
        MAX(datetime_beginning_utc) AS datetime_beginning_utc
        ,MAX(datetime_ending_utc) AS datetime_ending_utc
        ,MAX(timezone) AS timezone
        ,MAX(datetime_beginning_local) AS datetime_beginning_local
        ,MAX(datetime_ending_local) AS datetime_ending_local
        ,date, hour_ending, mkt_region
        ,'SOUTH' AS load_area
        ,SUM(load_mw) AS load_mw
    FROM PRELIM
    WHERE mkt_region = 'SOUTH'
    GROUP BY date, hour_ending, mkt_region
),

FINAL AS (
    SELECT datetime_beginning_utc, datetime_ending_utc, timezone, datetime_beginning_local, datetime_ending_local, date, hour_ending, mkt_region, load_area, load_mw FROM PRELIM
    UNION ALL
    SELECT * FROM RTO
    UNION ALL
    SELECT * FROM WEST
    UNION ALL
    SELECT * FROM SOUTH
)

SELECT * FROM FINAL
ORDER BY datetime_ending_local DESC, mkt_region, load_area
