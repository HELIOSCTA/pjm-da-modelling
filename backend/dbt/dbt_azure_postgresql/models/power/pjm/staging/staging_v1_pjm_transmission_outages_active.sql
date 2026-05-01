{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Transmission Outages — currently active or scheduled-and-locked-in
-- Filter: outage_state in ('Active','Approved'), LINE/XFMR/PS, voltage_kv >= 230
-- Grain: 1 row per ticket_id
---------------------------

WITH ACTIVE AS (
    SELECT *
    FROM {{ ref('source_v1_pjm_transmission_outages') }}
    WHERE
        outage_state IN ('Active', 'Approved')
        AND equipment_type IN ('LINE', 'XFMR', 'PS')
        AND voltage_kv >= 230
)

SELECT * FROM ACTIVE
ORDER BY voltage_kv DESC, last_revised DESC
