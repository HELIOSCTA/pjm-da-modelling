select 

    datetime
    ,date
    ,hour_ending
    -- ,pjm_timezone
    -- ,pjm_datetime_beginning_local
    -- ,pjm_datetime_ending_local
    -- ,gas_timezone
    -- ,gas_datetime_beginning_local
    -- ,gas_datetime_ending_local
    -- ,gas_day
    -- ,trade_date

    ,tetco_m3_cash
    ,columbia_tco_cash
    ,transco_z6_ny_cash
    ,dominion_south_cash
    ,nng_ventura_cash
    ,tetco_m2_cash
    ,transco_z5_north_cash
    ,tenn_z4_marcellus_cash
    ,transco_leidy_cash
    ,chicago_cg_cash

from pjm_da_modelling_cleaned.ice_python_next_day_gas_hourly_pjm