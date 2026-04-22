{{
  config(
    materialized='ephemeral'
  )
}}

----------------------------------
-- Daily LMPs (normalized)
-- Grain: 1 row per date Ã— hub Ã— period Ã— market
----------------------------------

{% set onpeak_start = 8 %}
{% set onpeak_end = 23 %}

WITH HOURLY AS (
    SELECT * FROM {{ ref('staging_v1_pjm_lmps_hourly') }}
    WHERE market = 'da' OR date < (CURRENT_TIMESTAMP AT TIME ZONE 'MST')::DATE
),

FLAT AS (
    SELECT
        date
        ,hub
        ,'flat' AS period
        ,market
        ,AVG(lmp_total) AS lmp_total
        ,AVG(lmp_system_energy_price) AS lmp_system_energy_price
        ,AVG(lmp_congestion_price) AS lmp_congestion_price
        ,AVG(lmp_marginal_loss_price) AS lmp_marginal_loss_price
    FROM HOURLY
    GROUP BY date, hub, market
),

ONPEAK AS (
    SELECT
        date
        ,hub
        ,'onpeak' AS period
        ,market
        ,AVG(lmp_total) AS lmp_total
        ,AVG(lmp_system_energy_price) AS lmp_system_energy_price
        ,AVG(lmp_congestion_price) AS lmp_congestion_price
        ,AVG(lmp_marginal_loss_price) AS lmp_marginal_loss_price
    FROM HOURLY
    WHERE hour_ending BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date, hub, market
),

OFFPEAK AS (
    SELECT
        date
        ,hub
        ,'offpeak' AS period
        ,market
        ,AVG(lmp_total) AS lmp_total
        ,AVG(lmp_system_energy_price) AS lmp_system_energy_price
        ,AVG(lmp_congestion_price) AS lmp_congestion_price
        ,AVG(lmp_marginal_loss_price) AS lmp_marginal_loss_price
    FROM HOURLY
    WHERE hour_ending NOT BETWEEN {{ onpeak_start }} AND {{ onpeak_end }}
    GROUP BY date, hub, market
),

DAILY AS (
    SELECT * FROM FLAT
    UNION ALL
    SELECT * FROM ONPEAK
    UNION ALL
    SELECT * FROM OFFPEAK
)

SELECT * FROM DAILY
ORDER BY date DESC, hub, period, market
