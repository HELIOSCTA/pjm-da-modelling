{{
  config(
    materialized='view'
  )
}}

---------------------------
-- Transmission Outages — last 24h delta (simple variant)
-- Uses source-table timestamps directly (no snapshot needed).
--
-- NEW      — row first appeared in DB in last 24h (created_at)
-- REVISED  — PJM revised an existing row in last 24h (last_revised)
--
-- Trade-off vs the *_snapshot variant: no prev_* diff columns and no CLEARED
-- detection. Returns useful data on day 1 (no history baseline required).
--
-- Inlined here (not in a staging model) because the staging-name length plus
-- dbt's `__dbt__cte__` prefix would exceed Postgres's 63-char identifier limit.
---------------------------

WITH RECENT AS (
    SELECT *
    FROM {{ ref('source_v1_pjm_transmission_outages') }}
    WHERE
        created_at  >= NOW() - INTERVAL '24 hours'
        OR last_revised >= NOW() - INTERVAL '24 hours'
),

CLASSIFIED AS (
    SELECT
        ticket_id
        ,CASE
            WHEN created_at >= NOW() - INTERVAL '24 hours' THEN 'NEW'
            ELSE 'REVISED'
         END                                                          AS change_type
        ,GREATEST(created_at, last_revised)                            AS captured_at
        ,zone
        ,facility_name
        ,equipment_type
        ,station
        ,voltage_kv
        ,start_datetime
        ,end_datetime
        ,status
        ,outage_state
        ,risk
        ,cause
        ,last_revised
        ,equipment_count
        ,created_at

    FROM RECENT
)

SELECT * FROM CLASSIFIED
ORDER BY change_type, voltage_kv DESC, captured_at DESC
