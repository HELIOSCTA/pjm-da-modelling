{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- HOURLY GAS DAY DATE SPINE
---------------------------

WITH DATE_SPINE AS (
    SELECT generate_series(
        DATE '2020-01-01',
        (CURRENT_DATE + INTERVAL '2 years')::DATE,
        INTERVAL '1 day'
    )::DATE AS date
),

HOURS AS (
    SELECT generate_series(1, 24) AS hour_ending
),

---------------------------
-- HOURLY SPINE WITH GAS DAY
---------------------------

HOURLY_SPINE AS (
    SELECT
        (d.date + (h.hour_ending || ' hours')::INTERVAL)::TIMESTAMP AS datetime,
        d.date::DATE AS date,
        h.hour_ending::INTEGER AS hour_ending,
        CASE
            WHEN h.hour_ending < 10 THEN d.date
            ELSE (d.date + INTERVAL '1 day')::DATE
        END AS gas_day,
        d.date::DATE AS trade_date
    FROM DATE_SPINE d
    CROSS JOIN HOURS h
),

FINAL AS (
    SELECT * FROM HOURLY_SPINE
)

SELECT * FROM FINAL
ORDER BY datetime DESC
