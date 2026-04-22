{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- DA Hourly Load (normalized)
-- Grain: 1 row per date × hour × region
---------------------------

WITH DA_BIDS AS (
    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,mkt_region
        ,load_area
        ,da_load_mw
    FROM {{ ref('source_v1_pjm_hrl_dmd_bids') }}
),

--------------------------------
-- SOUTH = RTO - MIDATL - WEST
--------------------------------

SOUTH AS (
    SELECT
        MAX(datetime_beginning_utc) AS datetime_beginning_utc
        ,MAX(datetime_ending_utc) AS datetime_ending_utc
        ,MAX(timezone) AS timezone
        ,MAX(datetime_beginning_local) AS datetime_beginning_local
        ,MAX(datetime_ending_local) AS datetime_ending_local
        ,date
        ,hour_ending
        ,'SOUTH' AS mkt_region
        ,'SOUTH' AS load_area
        ,(
            AVG(CASE WHEN load_area = 'RTO' THEN da_load_mw END)
            - AVG(CASE WHEN load_area = 'MIDATL' THEN da_load_mw END)
            - AVG(CASE WHEN load_area = 'WEST' THEN da_load_mw END)
        ) AS da_load_mw
    FROM DA_BIDS
    GROUP BY date, hour_ending
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
        ,da_load_mw
    FROM DA_BIDS

    UNION ALL

    SELECT
        datetime_beginning_utc
        ,datetime_ending_utc
        ,timezone
        ,datetime_beginning_local
        ,datetime_ending_local
        ,date
        ,hour_ending
        ,load_area AS region
        ,da_load_mw
    FROM SOUTH
)

SELECT * FROM FINAL
ORDER BY datetime_ending_local DESC, region
