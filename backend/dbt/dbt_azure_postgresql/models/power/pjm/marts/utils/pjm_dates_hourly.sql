{{
  config(
    materialized='view'
  )
}}

WITH UTILS AS (
    SELECT * FROM {{ ref('utils_v1_pjm_dates_hourly') }}
)

SELECT
    datetime AT TIME ZONE 'US/Eastern' AT TIME ZONE 'UTC' AS datetime_beginning_utc
    ,(datetime + INTERVAL '1 hour') AT TIME ZONE 'US/Eastern' AT TIME ZONE 'UTC' AS datetime_ending_utc
    ,'US/Eastern' AS timezone
    ,datetime AS datetime_beginning_local
    ,datetime + INTERVAL '1 hour' AS datetime_ending_local
    ,date
    ,hour_ending
    ,year
    ,year_month
    ,summer_winter
    ,summer_winter_yyyy
    ,month
    ,mm_dd
    ,mm_dd_cy
    ,eia_storage_week
    ,eia_storage_week_number
    ,period
    ,is_offpeak_with_weekends_holidays
    ,day_of_week
    ,day_of_week_number
    ,is_weekend
    ,is_nerc_holiday
FROM UTILS
ORDER BY datetime_ending_local DESC
