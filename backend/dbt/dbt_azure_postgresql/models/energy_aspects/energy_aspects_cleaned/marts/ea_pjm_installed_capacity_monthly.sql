{{
  config(
    materialized='view'
  )
}}

---------------------------
-- EA PJM Installed Capacity (monthly view)
-- Forward-projected through 2030; one row per month.
-- Materialized as a view so downstream Prefect flows can `SELECT *` from it.
---------------------------

SELECT *
FROM {{ ref('staging_v1_ea_pjm_installed_capacity_monthly') }}
ORDER BY date DESC
