{{
  config(
    materialized='ephemeral'
  )
}}

----------------------------------------------------
----------------------------------------------------
WITH HOURS AS (
    SELECT generate_series(
        TIMESTAMP '2010-01-01 00:00:00',
        (date_trunc('year', current_date) + interval '1 year - 1 hour')::timestamp,
        INTERVAL '1 hour'
    )::TIMESTAMP AS datetime
),

DATETIMES AS (
    SELECT
        -- YEAR
        EXTRACT(YEAR FROM datetime) AS year
        ,CONCAT(EXTRACT(YEAR FROM datetime), '-', EXTRACT(MONTH FROM datetime)) as year_month

        -- SUMMER / WINTER
        ,CASE
            WHEN EXTRACT(month from datetime) in (11, 12, 1, 2, 3) then 'WINTER'
            WHEN EXTRACT(month from datetime) in (4, 5, 6, 7, 8, 9, 10) then 'SUMMER'
            ELSE NULL
        END AS summer_winter
        ,CASE
            WHEN EXTRACT(month from datetime) in (1, 2, 3) then 'XH-' || right(EXTRACT(year from datetime)::text, 2)
            WHEN EXTRACT(month from datetime) in (11, 12) then 'XH-' || right((EXTRACT(year from datetime)+1)::text, 2)
            WHEN EXTRACT(month from datetime) in (4, 5, 6, 7, 8, 9, 10) then 'JV-' || right(EXTRACT(year from datetime)::text, 2)
            ELSE NULL
        END AS summer_winter_yyyy

        -- MONTH
        ,EXTRACT(MONTH FROM datetime) AS month
        ,TO_CHAR(datetime, 'MM-DD') AS mm_dd
        ,(EXTRACT(YEAR FROM current_date)::VARCHAR || '-' || TO_CHAR(datetime::date, 'MM-DD'))::DATE as mm_dd_cy

        -- EIA WEEKS
        ,(datetime::date +
            CASE
                WHEN (EXTRACT(DOW FROM datetime::date) - 5) >= 0
                THEN -(EXTRACT(DOW FROM datetime::date) - 5 - 7)
                ELSE -(EXTRACT(DOW FROM datetime::date) - 5)
            END * INTERVAL '1 day')::date as eia_storage_week
        ,EXTRACT(WEEK FROM (datetime::date +
            CASE
                WHEN (EXTRACT(DOW FROM datetime::date) - 5) >= 0
                THEN -(EXTRACT(DOW FROM datetime::date) - 5 - 7)
                ELSE -(EXTRACT(DOW FROM datetime::date) - 5)
            END * INTERVAL '1 day')::date) as eia_storage_week_number

        -- DAILY
        ,datetime::date as date

        -- HOURLY
        ,datetime
        ,EXTRACT(HOUR FROM datetime) + 1 AS hour_ending

        -- PEAK / OFF-PEAK (HE08-HE23 weekdays, excluding NERC holidays)
        ,CASE
            WHEN
                EXTRACT(DOW FROM datetime::date) BETWEEN 1 AND 5
                AND EXTRACT(HOUR FROM datetime) + 1 BETWEEN 8 AND 23
                AND datetime::date NOT IN (SELECT nerc_holiday FROM {{ref('utils_v1_nerc_holidays')}} )
            THEN 'OnPeak'
            ELSE 'OffPeak'
        END AS period
        ,CASE
            WHEN EXTRACT(HOUR FROM datetime) + 1 NOT BETWEEN 8 AND 23 THEN 1
            ELSE 0
        END AS is_offpeak_with_weekends_holidays

        -- WEEKENDS/HOLIDAYS
        ,TRIM(TO_CHAR(datetime::date, 'Day')) AS day_of_week
        ,EXTRACT(DOW FROM datetime::date) AS day_of_week_number
        ,CASE
            WHEN EXTRACT(DOW FROM datetime::date) IN (0, 6) THEN 1
            ELSE 0
        END AS is_weekend
        ,CASE
            WHEN datetime::date IN (SELECT nerc_holiday FROM {{ref('utils_v1_nerc_holidays')}} ) THEN 1
            ELSE 0
        END AS is_nerc_holiday

    FROM HOURS
    WHERE TO_CHAR(datetime, 'MM-DD') != '02-29'
)

SELECT * FROM DATETIMES
ORDER BY datetime DESC
