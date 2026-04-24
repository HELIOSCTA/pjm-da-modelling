{{
  config(
    materialized='ephemeral'
  )
}}

----------------------------------------------------
----------------------------------------------------

WITH DAYS AS (
    SELECT generate_series(
        DATE '2010-01-01',
        (date_trunc('year', current_date) + interval '1 year - 1 day')::date,
        INTERVAL '1 day'
    )::DATE AS date
),

NERC_HOLIDAYS AS (
    SELECT holiday_date::date AS holiday_date FROM {{ ref('pjm_holidays') }} WHERE holiday_type = 'nerc'
),
FEDERAL_HOLIDAYS AS (
    SELECT holiday_date::date AS holiday_date FROM {{ ref('pjm_holidays') }} WHERE holiday_type = 'federal'
),
SOFT_HOLIDAYS AS (
    SELECT holiday_date::date AS holiday_date FROM {{ ref('pjm_holidays') }} WHERE holiday_type = 'soft'
),

DATES AS (
    SELECT

        -- YEAR
        -- EXTRACT(YEAR FROM date) AS year
        -- ,CONCAT(EXTRACT(YEAR FROM date), '-', EXTRACT(MONTH FROM date)) as year_month

        -- SUMMER / WINTER
        CASE
            WHEN EXTRACT(month from date) in (11, 12, 1, 2, 3) then 'WINTER'
            WHEN EXTRACT(month from date) in (4, 5, 6, 7, 8, 9, 10) then 'SUMMER'
            ELSE NULL
        END AS summer_winter
        ,CASE
            WHEN EXTRACT(month from date) in (1, 2, 3) then 'XH-' || right(EXTRACT(year from date)::text, 2)
            WHEN EXTRACT(month from date) in (11, 12) then 'XH-' || right((EXTRACT(year from date)+1)::text, 2)
            WHEN EXTRACT(month from date) in (4, 5, 6, 7, 8, 9, 10) then 'JV-' || right(EXTRACT(year from date)::text, 2)
            ELSE NULL
        END AS summer_winter_yyyy

        -- MONTH
        ,EXTRACT(MONTH FROM date) AS month
        -- ,TO_CHAR(date, 'MM-DD') AS mm_dd
        -- ,(EXTRACT(YEAR FROM current_date)::VARCHAR || '-' || TO_CHAR(date::date, 'MM-DD'))::DATE as mm_dd_cy

        -- EIA WEEKS
        -- ,(date::date +
        --     CASE
        --         WHEN (EXTRACT(DOW FROM date::date) - 5) >= 0
        --         THEN -(EXTRACT(DOW FROM date::date) - 5 - 7)
        --         ELSE -(EXTRACT(DOW FROM date::date) - 5)
        --     END * INTERVAL '1 day')::date as eia_storage_week
        -- ,EXTRACT(WEEK FROM (date::date +
        --     CASE
        --         WHEN (EXTRACT(DOW FROM date::date) - 5) >= 0
        --         THEN -(EXTRACT(DOW FROM date::date) - 5 - 7)
        --         ELSE -(EXTRACT(DOW FROM date::date) - 5)
        --     END * INTERVAL '1 day')::date) as eia_storage_week_number

        -- DAILY
        ,date::date as date

        -- OnPeak (HE08-HE23 weekdays, excluding NERC holidays)
        ,CASE
            WHEN
                EXTRACT(DOW FROM date::date) BETWEEN 1 AND 5
                AND date::date NOT IN (SELECT holiday_date FROM NERC_HOLIDAYS)
            THEN 1
            ELSE 0
        END AS is_onpeak_with_weekends_holidays

        -- WEEKENDS/HOLIDAYS
        -- ,TRIM(TO_CHAR(date::date, 'Day')) AS day_of_week
        ,EXTRACT(DOW FROM date::date) AS day_of_week_number

        ,CASE
            WHEN EXTRACT(DOW FROM date::date) IN (0, 6) THEN 1
            ELSE 0
        END AS is_weekend
        ,CASE
            WHEN date::date IN (SELECT holiday_date FROM NERC_HOLIDAYS) THEN 1
            ELSE 0
        END AS is_nerc_holiday
        ,CASE
            WHEN date::date IN (SELECT holiday_date FROM FEDERAL_HOLIDAYS) THEN 1
            ELSE 0
        END AS is_federal_holiday
        ,CASE
            WHEN date::date IN (SELECT holiday_date FROM SOFT_HOLIDAYS) THEN 1
            ELSE 0
        END AS is_soft_holiday
        -- Bridge day: weekday adjacent to a NERC holiday but not itself any holiday
        ,CASE
            WHEN EXTRACT(DOW FROM date::date) BETWEEN 1 AND 5
                AND date::date NOT IN (SELECT holiday_date::date FROM {{ ref('pjm_holidays') }})
                AND (
                    (date::date + INTERVAL '1 day')::date IN (SELECT holiday_date FROM NERC_HOLIDAYS)
                    OR (date::date - INTERVAL '1 day')::date IN (SELECT holiday_date FROM NERC_HOLIDAYS)
                )
            THEN 1
            ELSE 0
        END AS is_bridge_day

    FROM DAYS
    WHERE TO_CHAR(date, 'MM-DD') != '02-29'
)

SELECT * FROM DATES
ORDER BY date DESC
