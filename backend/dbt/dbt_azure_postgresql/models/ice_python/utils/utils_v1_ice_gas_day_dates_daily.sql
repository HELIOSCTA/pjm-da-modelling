{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- DAILY GAS DAY DATE SPINE
---------------------------

WITH DATE_SPINE AS (
    SELECT generate_series(
        DATE '2020-01-01',
        (CURRENT_DATE + INTERVAL '2 years')::DATE,
        INTERVAL '1 day'
    )::DATE AS date
),

---------------------------
-- MAP TRADE DATE TO GAS DAY
---------------------------

FINAL AS (
    SELECT
        date::DATE AS date,
        (date + INTERVAL '1 day')::DATE AS gas_day,
        date::DATE AS trade_date
    FROM DATE_SPINE
)

SELECT * FROM FINAL
ORDER BY date DESC
