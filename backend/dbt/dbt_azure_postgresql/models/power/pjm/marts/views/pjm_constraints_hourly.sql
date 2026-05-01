{{
  config(
    materialized='view'
  )
}}

---------------------------
-- Long-form view of PJM binding-constraint shadow prices across DA / RT / DART.
-- One row per (date, hour_ending, market, monitored_facility, contingency_facility).
-- Filter `market IN ('DA','RT','DART')` to slice; default contains all three.
---------------------------

SELECT * FROM {{ ref('staging_v1_pjm_constraints_hourly') }}
