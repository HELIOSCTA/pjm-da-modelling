{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Transmission Outages — 7-day forward outlook
-- Filter: outage_state in ('Active','Approved','Received'), LINE/XFMR/PS, voltage_kv >= 230
-- Window: ticket overlaps [now(), now() + 7 days]
-- Grain: 1 row per ticket_id, with state_class label for "locked vs planned"
---------------------------

WITH WINDOW_7D AS (
    SELECT
        *
        ,CASE
            WHEN outage_state IN ('Active', 'Approved') THEN 'locked'
            WHEN outage_state = 'Received' THEN 'planned'
            ELSE 'other'
        END AS state_class

    FROM {{ ref('source_v1_pjm_transmission_outages') }}
    WHERE
        outage_state IN ('Active', 'Approved', 'Received')
        AND equipment_type IN ('LINE', 'XFMR', 'PS')
        AND voltage_kv >= 230
        AND start_datetime < (NOW() + INTERVAL '7 days')
        AND (end_datetime IS NULL OR end_datetime >= NOW())
)

SELECT * FROM WINDOW_7D
ORDER BY state_class, voltage_kv DESC, start_datetime
