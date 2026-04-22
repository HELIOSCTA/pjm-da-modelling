{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Ancillary Services Prices (pivoted wide)
-- Grain: 1 row per date x hour
-- Key signal: sr_price > 0 means reserve scarcity adder is active
---------------------------

WITH LONG AS (
    SELECT * FROM {{ ref('source_v1_pjm_ancillary_services') }}
),

PIVOTED AS (
    SELECT
        MAX(datetime_beginning_utc) AS datetime_beginning_utc
        ,MAX(datetime_ending_utc) AS datetime_ending_utc
        ,MAX(timezone) AS timezone
        ,MAX(datetime_beginning_local) AS datetime_beginning_local
        ,MAX(datetime_ending_local) AS datetime_ending_local
        ,date
        ,hour_ending

        -- Reserve prices ($/MWh)
        ,MAX(CASE WHEN ancillary_service = 'MAD Synchronized Reserve' THEN value END) AS sr_price
        ,MAX(CASE WHEN ancillary_service = 'MAD Non-Synchronized Reserve' THEN value END) AS non_sr_price
        ,MAX(CASE WHEN ancillary_service = 'MAD Secondary Reserve' THEN value END) AS secondary_reserve_price

        -- Regulation prices ($/MWh)
        ,MAX(CASE WHEN ancillary_service = 'RTO Regulation Capability' THEN value END) AS regulation_price

        -- Mileage ratio (dimensionless)
        ,MAX(CASE WHEN ancillary_service = 'RTO Mileage Ratio' THEN value END) AS mileage_ratio

        -- Scarcity flag: SR price > 0 means a reserve scarcity adder is active
        ,CASE
            WHEN MAX(CASE WHEN ancillary_service = 'MAD Synchronized Reserve' THEN value END) > 0
            THEN TRUE ELSE FALSE
        END AS scarcity_adder_active

    FROM LONG
    GROUP BY date, hour_ending
)

SELECT * FROM PIVOTED
ORDER BY datetime_ending_local DESC
