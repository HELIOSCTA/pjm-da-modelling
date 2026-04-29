{{
  config(
    materialized='ephemeral'
  )
}}

SELECT
     date
    ,natural_gas_mw
    ,coal_mw
    ,nuclear_mw
    ,oil_products_mw
    ,solar_mw
    ,onshore_wind_mw
    ,hydro_mw
    ,offshore_wind_mw
    ,battery_mw
    ,COALESCE(natural_gas_mw, 0)
        + COALESCE(coal_mw, 0)
        + COALESCE(nuclear_mw, 0)
        + COALESCE(oil_products_mw, 0)
        + COALESCE(solar_mw, 0)
        + COALESCE(onshore_wind_mw, 0)
        + COALESCE(hydro_mw, 0)
        + COALESCE(offshore_wind_mw, 0)
        + COALESCE(battery_mw, 0) AS total_installed_capacity_mw

FROM {{ ref('source_v1_ea_pjm_installed_capacity_monthly') }}
