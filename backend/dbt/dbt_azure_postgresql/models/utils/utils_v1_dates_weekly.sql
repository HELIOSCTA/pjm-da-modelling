{{
  config(
    materialized='view'
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

DATES AS (
    SELECT

        -- EIA WEEKS
        (date::date + 
            CASE 
                WHEN (EXTRACT(DOW FROM date::date) - 5) >= 0 
                THEN -(EXTRACT(DOW FROM date::date) - 5 - 7)
                ELSE -(EXTRACT(DOW FROM date::date) - 5)
            END * INTERVAL '1 day')::date as eia_storage_week
        ,EXTRACT(WEEK FROM (date::date + 
            CASE 
                WHEN (EXTRACT(DOW FROM date::date) - 5) >= 0 
                THEN -(EXTRACT(DOW FROM date::date) - 5 - 7)
                ELSE -(EXTRACT(DOW FROM date::date) - 5)
            END * INTERVAL '1 day')::date) as eia_storage_week_number

        -- DAILY
        ,date::date as date
        
        -- HOLIDAYS
        ,CASE 
            WHEN date::date IN (SELECT nerc_holiday FROM {{ref('utils_v1_nerc_holidays')}} ) THEN 1  --'NERC Holiday'
            ELSE 0  --'No Holiday'
        END AS is_nerc_holiday

    FROM DAYS
),

-- SELECT * FROM DATES
-- ORDER BY date DESC

----------------------------------------------------
----------------------------------------------------

WEEKS AS (
    SELECT 

        eia_storage_week
        ,eia_storage_week_number

        ,SUM(is_nerc_holiday) as is_nerc_holiday

    FROM DATES
    GROUP BY 
        eia_storage_week
        ,eia_storage_week_number
),

-- SELECT * FROM WEEKS
-- ORDER BY eia_storage_week DESC

----------------------------------------------------
----------------------------------------------------

FINAL AS (
    SELECT
        
        -- YEAR
        EXTRACT(YEAR FROM eia_storage_week) AS year
        ,MAKE_DATE(EXTRACT(YEAR FROM eia_storage_week)::int, EXTRACT(MONTH FROM eia_storage_week)::int, 1) AS year_month

        -- SUMMER / WINTER
        ,CASE
            WHEN EXTRACT(month from eia_storage_week) in (11, 12, 1, 2, 3) then 'WINTER'
            WHEN EXTRACT(month from eia_storage_week) in (4, 5, 6, 7, 8, 9, 10) then 'SUMMER'
            ELSE NULL
        END AS summer_winter
        ,CASE
            WHEN EXTRACT(month from eia_storage_week) in (1, 2, 3) then 'XH-' || right(EXTRACT(year from eia_storage_week)::text, 2)
            WHEN EXTRACT(month from eia_storage_week) in (11, 12) then 'XH-' || right((EXTRACT(year from eia_storage_week)+1)::text, 2)
            WHEN EXTRACT(month from eia_storage_week) in (4, 5, 6, 7, 8, 9, 10) then 'JV-' || right(EXTRACT(year from eia_storage_week)::text, 2)
            ELSE NULL
        END AS summer_winter_yyyy

        -- MONTH
        ,EXTRACT(MONTH FROM eia_storage_week) AS month

        -- EIA WEEKS
        ,eia_storage_week
        ,eia_storage_week_number

        -- HOLIDAYS
        ,is_nerc_holiday

    FROM WEEKS
)

SELECT * FROM FINAL
ORDER BY eia_storage_week DESC