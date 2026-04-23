{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Meteologica PJM Demand Forecast (Hourly)
-- UNIONs 4 source tables (RTO + 3 macro regions), produces UTC/timezone/local
-- triplets for issue time, ranks by issue time (most recent first).
-- Grain: 1 row per forecast_execution_datetime x forecast_date x hour_ending x region
---------------------------

WITH UNIONED AS (

    SELECT 'RTO'    AS region, update_id, issue_date, forecast_period_start, forecast_mw FROM {{ ref('src_meteo_pjm_rto_load') }}
    UNION ALL
    SELECT 'MIDATL' AS region, update_id, issue_date, forecast_period_start, forecast_mw FROM {{ ref('src_meteo_pjm_midatl_load') }}
    UNION ALL
    SELECT 'SOUTH'  AS region, update_id, issue_date, forecast_period_start, forecast_mw FROM {{ ref('src_meteo_pjm_south_load') }}
    UNION ALL
    SELECT 'WEST'   AS region, update_id, issue_date, forecast_period_start, forecast_mw FROM {{ ref('src_meteo_pjm_west_load') }}

),

---------------------------
-- NORMALIZE TIMESTAMPS (UTC + timezone + local triplets)
-- issue_date is published in UTC; convert to America/New_York for EPT local.
-- forecast_period_start is an hour-beginning local timestamp (no tz).
-- hour_ending = EXTRACT(HOUR FROM forecast_period_start) + 1 (1..24).
---------------------------

NORMALIZED AS (
    SELECT
        region

        ,issue_date::TIMESTAMP AS forecast_execution_datetime_utc
        ,'US/Eastern' AS timezone
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') AS forecast_execution_datetime_local
        ,(issue_date::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')::DATE AS forecast_execution_date

        ,forecast_period_start::DATE AS forecast_date
        ,EXTRACT(HOUR FROM forecast_period_start)::INT + 1 AS hour_ending

        ,forecast_mw::NUMERIC AS forecast_load_mw
    FROM UNIONED
),

--------------------------------
-- Rank forecasts per (forecast_date, region) by issue time (most recent first)
-- Partitioned by region because each Meteologica region is a separate endpoint
-- with potentially different issue_dates.
--------------------------------

FORECAST_RANK AS (
    SELECT
        forecast_date
        ,region
        ,forecast_execution_datetime_local

        ,DENSE_RANK() OVER (
            PARTITION BY forecast_date, region
            ORDER BY forecast_execution_datetime_local DESC
        ) AS forecast_rank

    FROM (
        SELECT DISTINCT forecast_execution_datetime_local, forecast_date, region
        FROM NORMALIZED
    ) sub
),

--------------------------------
-- FINAL
--------------------------------

FINAL AS (
    SELECT
        n.forecast_execution_datetime_utc
        ,n.timezone
        ,n.forecast_execution_datetime_local
        ,r.forecast_rank
        ,n.forecast_execution_date

        ,(n.forecast_date + INTERVAL '1 hour' * (n.hour_ending - 1)) AS forecast_datetime
        ,n.forecast_date
        ,n.hour_ending

        ,n.region
        ,n.forecast_load_mw

    FROM NORMALIZED n
    JOIN FORECAST_RANK r
        ON n.forecast_execution_datetime_local = r.forecast_execution_datetime_local
        AND n.forecast_date = r.forecast_date
        AND n.region = r.region
)

SELECT * FROM FINAL
ORDER BY forecast_date DESC, forecast_execution_datetime_local DESC, hour_ending, region
