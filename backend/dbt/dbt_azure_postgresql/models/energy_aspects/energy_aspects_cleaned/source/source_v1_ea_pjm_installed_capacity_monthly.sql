{{
  config(
    materialized='ephemeral'
  )
}}

---------------------------
-- Energy Aspects PJM installed capacity monthly source
-- Grain: 1 row per month
---------------------------

SELECT
     date::DATE AS date
    ,fcst_ng_installed_capacity_in_pjm_in_mw::NUMERIC AS natural_gas_mw
    ,fcst_coal_installed_capacity_in_pjm_in_mw::NUMERIC AS coal_mw
    ,fcst_nuclear_installed_capacity_in_pjm_in_mw::NUMERIC AS nuclear_mw
    ,fcst_oil_products_installed_capacity_in_pjm_in_mw::NUMERIC AS oil_products_mw
    ,fcst_solar_installed_capacity_in_pjm_in_mw::NUMERIC AS solar_mw
    ,fcst_onshore_wind_installed_capacity_in_pjm_in_mw::NUMERIC AS onshore_wind_mw
    ,fcst_hydro_installed_capacity_in_pjm_in_mw::NUMERIC AS hydro_mw
    ,fcst_offshore_wind_installed_capacity_in_pjm_in_mw::NUMERIC AS offshore_wind_mw
    ,fcst_battery_installed_capacity_in_pjm_in_mw::NUMERIC AS battery_mw

FROM {{ source('energy_aspects_v1', 'us_installed_capacity_by_iso_and_fuel_type') }}
